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
import org.apache.raft.server.storage.RaftLog;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.server.storage.RaftStorageDirectory.SnapshotPathAndTermIndex;
import org.apache.raft.util.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

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

  private final StateMachine stateMachine;
  private final RaftStorage storage;
  private final RaftLog raftLog;

  private long lastAppliedIndex;

  private final boolean autoSnapshotEnabled;
  private final long snapshotThreshold;
  private long lastSnapshotIndex;

  private final Thread updater;
  private volatile State state = State.RUNNING;
  private volatile SnapshotPathAndTermIndex toLoadSnapshot;

  StateMachineUpdater(StateMachine stateMachine, RaftLog raftLog,
      RaftStorage storage, long lastAppliedIndex, RaftProperties properties) {
    this.stateMachine = stateMachine;
    this.storage = storage;
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

  void reloadStateMachine(SnapshotPathAndTermIndex snapshot) {
    state = State.RELOAD;
    this.toLoadSnapshot = snapshot;
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
    final StateMachine sm = this.stateMachine;
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
          Preconditions.checkState(toLoadSnapshot != null &&
              toLoadSnapshot.endIndex > lastAppliedIndex,
              "toLoadSnapshot: %s, lastAppliedIndex: %s", toLoadSnapshot,
              lastAppliedIndex);

          stateMachine.reloadSnapshot(toLoadSnapshot.path.toFile());
          lastAppliedIndex = toLoadSnapshot.endIndex;
          lastSnapshotIndex = toLoadSnapshot.endIndex;
          state = State.RUNNING;
        }

        while (lastAppliedIndex < committedIndex) {
          final LogEntryProto next = raftLog.get(lastAppliedIndex + 1);
          if (next != null) {
            sm.applyLogEntry(next);
            lastAppliedIndex++;
          } else {
            LOG.debug("{}: logEntry {} is null. There may be snapshot to load."
                    + " state:{}, toLoadSnapshot:{}",
                this.toString(), lastAppliedIndex + 1, state, toLoadSnapshot);
            break;
          }
        }

        // check if need to trigger a snapshot
        if (shouldTakeSnapshot(lastAppliedIndex)) {
          File snapshotFile = storage.getStorageDir().getSnapshotFile(
              raftLog.get(lastAppliedIndex).getTerm(), lastAppliedIndex);
          sm.takeSnapshot(snapshotFile, storage);
          // TODO purge logs, including log cache. but should keep log for leader's RPCSenders
          lastSnapshotIndex = lastAppliedIndex;
        }
      } catch (InterruptedException e) {
        if (!isRunning()) {
          LOG.info("{}: the StateMachineUpdater is interrupted and will exit.",
              this.toString());
        } else {
          RaftUtils.terminate(e,
              this + ": the StateMachineUpdater is wrongly interrupted", LOG);
        }
      } catch (Throwable t) {
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

  @VisibleForTesting
  StateMachine getStateMachine() {
    return stateMachine;
  }
}
