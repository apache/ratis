/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.raft.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.util.Daemon;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.protocol.Message;
import org.apache.raft.server.protocol.ServerProtoUtils;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.RaftLog;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.statemachine.SnapshotInfo;
import org.apache.raft.util.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * This class tracks the log entries that have been committed in a quorum and
 * applies them to the state machine. We let a separate thread do this work
 * asynchronously so that this will not block normal raft protocol.
 *
 * If the auto log compaction is enabled, the state machine updater thread will
 * trigger a snapshot of the state machine by calling
 * {@link StateMachine#takeSnapshot} when the log size exceeds a limit.
 */
class StateMachineUpdater implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(StateMachineUpdater.class);

  enum State {
    RUNNING, STOP, RELOAD
  }

  private final RaftProperties properties;
  private final StateMachine stateMachine;
  private final RaftServer server;
  private final RaftLog raftLog;

  private volatile long lastAppliedIndex;

  private final boolean autoSnapshotEnabled;
  private final long snapshotThreshold;
  private long lastSnapshotIndex;

  private final Thread updater;
  private volatile State state = State.RUNNING;

  StateMachineUpdater(StateMachine stateMachine, RaftServer server,
      RaftLog raftLog, long lastAppliedIndex, RaftProperties properties) {
    this.properties = properties;
    this.stateMachine = stateMachine;
    this.server = server;
    this.raftLog = raftLog;

    this.lastAppliedIndex = lastAppliedIndex;
    lastSnapshotIndex = lastAppliedIndex;

    autoSnapshotEnabled = properties.getBoolean(
        RaftServerConfigKeys.RAFT_SERVER_AUTO_SNAPSHOT_ENABLED_KEY,
        RaftServerConfigKeys.RAFT_SERVER_AUTO_SNAPSHOT_ENABLED_DEFAULT);
    snapshotThreshold = properties.getLong(
        RaftServerConfigKeys.RAFT_SERVER_SNAPSHOT_TRIGGER_THRESHOLD_KEY,
        RaftServerConfigKeys.RAFT_SERVER_SNAPSHOT_TRIGGER_THRESHOLD_DEFAULT);
    updater = new Daemon(this);
  }

  void start() {
    updater.start();
  }

  void stop() {
    state = State.STOP;
    updater.interrupt();
    try {
      stateMachine.close();
    } catch (IOException ignored) {
    }
  }

  void reloadStateMachine() {
    state = State.RELOAD;
    notifyUpdater();
  }

  synchronized void notifyUpdater() {
    notifyAll();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "-" + raftLog.getSelfId();
  }

  @Override
  public void run() {
    final RaftStorage storage = server.getState().getStorage();
    while (isRunning()) {
      try {
        synchronized (this) {
          // when the peers just start, the committedIndex is initialized as 0
          // and will be updated only after the leader contacts other peers.
          // Thus initially lastAppliedIndex can be greater than lastCommitted.
          while (lastAppliedIndex >= raftLog.getLastCommittedIndex()) {
            wait();
          }
        }

        final long committedIndex = raftLog.getLastCommittedIndex();
        Preconditions.checkState(lastAppliedIndex < committedIndex);

        if (state == State.RELOAD) {
          Preconditions.checkState(stateMachine.getState() == StateMachine.State.PAUSED);

          stateMachine.reinitialize(properties, storage);

          SnapshotInfo snapshot = stateMachine.getLatestSnapshot();
          Preconditions.checkState(snapshot != null && snapshot.getIndex() > lastAppliedIndex,
              "Snapshot: %s, lastAppliedIndex: %s", snapshot, lastAppliedIndex);

          lastAppliedIndex = snapshot.getIndex();
          lastSnapshotIndex = snapshot.getIndex();
          state = State.RUNNING;
        }

        while (lastAppliedIndex < committedIndex) {
          final LogEntryProto next = raftLog.get(lastAppliedIndex + 1);
          if (next != null) {
            if (next.hasConfigurationEntry()) {
              // the reply should have already been set. only need to record
              // the new conf in the state machine.
              stateMachine.setRaftConfiguration(
                  ServerProtoUtils.toRaftConfiguration(next.getIndex(),
                      next.getConfigurationEntry()));
            } else if (next.hasClientOperation()) {
              CompletableFuture<Message> messageFuture =
                  stateMachine.applyLogEntry(next);
              server.replyPendingRequest(next.getIndex(), messageFuture);
            }
            lastAppliedIndex++;
          } else {
            LOG.debug("{}: logEntry {} is null. There may be snapshot to load. state:{}",
                this, lastAppliedIndex + 1, state);
            break;
          }
        }

        // check if need to trigger a snapshot
        if (shouldTakeSnapshot(lastAppliedIndex)) {
          stateMachine.takeSnapshot();
          // TODO purge logs, including log cache. but should keep log for leader's RPCSenders
          lastSnapshotIndex = lastAppliedIndex;
        }
      } catch (InterruptedException e) {
        if (!isRunning()) {
          LOG.info("{}: the StateMachineUpdater is interrupted and will exit.",
              this);
        } else {
          RaftUtils.terminate(e,
              this + ": the StateMachineUpdater is wrongly interrupted", LOG);
        }
      } catch (Throwable t) {
        LOG.warn("the StateMachineUpdater hits Throwable", t);
        RaftUtils.terminate(t,
            this + ": the StateMachineUpdater hits Throwable", LOG);
      }
    }
  }

  private boolean isRunning() {
    return state != State.STOP;
  }

  private boolean shouldTakeSnapshot(long currentAppliedIndex) {
    return autoSnapshotEnabled && (state != State.RELOAD) &&
        (currentAppliedIndex - lastSnapshotIndex >= snapshotThreshold);
  }

  long getLastAppliedIndex() {
    return lastAppliedIndex;
  }
}
