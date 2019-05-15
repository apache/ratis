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
package org.apache.ratis.server.impl;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
  private volatile Long stopIndex = null;

  enum State {
    RUNNING, STOP, RELOAD
  }

  private final RaftProperties properties;
  private final StateMachine stateMachine;
  private final RaftServerImpl server;
  private final RaftLog raftLog;

  private volatile long lastAppliedIndex;

  private final boolean autoSnapshotEnabled;
  private final long autoSnapshotThreshold;
  private long lastSnapshotIndex;

  private final Thread updater;
  private volatile State state = State.RUNNING;

  StateMachineUpdater(StateMachine stateMachine, RaftServerImpl server,
      RaftLog raftLog, long lastAppliedIndex, RaftProperties properties) {
    this.properties = properties;
    this.stateMachine = stateMachine;
    this.server = server;
    this.raftLog = raftLog;

    this.lastAppliedIndex = lastAppliedIndex;
    lastSnapshotIndex = lastAppliedIndex;

    autoSnapshotEnabled = RaftServerConfigKeys.Snapshot.autoTriggerEnabled(properties);
    autoSnapshotThreshold = RaftServerConfigKeys.Snapshot.autoTriggerThreshold(properties);
    updater = new Daemon(this);
  }

  void start() {
    updater.start();
  }

  private void stop() {
    state = State.STOP;
    try {
      stateMachine.close();
    } catch (IOException ignored) {
      LOG.warn(server.getId() + ": Failed to close "
          + stateMachine.getClass().getSimpleName()
          + " " + stateMachine, ignored);
    }
  }

  /**
   * Stop the updater thread after all the committed transactions
   * have been applied to the state machine.
   *
   * @throws InterruptedException
   */
  void stopAndJoin() throws InterruptedException {
    if (stopIndex == null) {
      synchronized (this) {
        this.stopIndex = raftLog.getLastCommittedIndex();
        notifyUpdater();
      }
      LOG.info("{}: set stopIndex = {}", this, stopIndex);
    }
    updater.join();
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
    return this.getClass().getSimpleName() + "-" + raftLog.getSelfId() + "-" + server.getGroupId();
  }

  @Override
  public void run() {
    while (isRunning()) {
      try {
        synchronized (this) {
          // when the peers just start, the committedIndex is initialized as 0
          // and will be updated only after the leader contacts other peers.
          // Thus initially lastAppliedIndex can be greater than lastCommitted.
          while (lastAppliedIndex >= raftLog.getLastCommittedIndex()
              && !shouldStop()) {
            wait();
          }
        }

        final long committedIndex = raftLog.getLastCommittedIndex();
        Preconditions.assertTrue(lastAppliedIndex <= committedIndex);

        if (state == State.RELOAD) {
          Preconditions.assertTrue(stateMachine.getLifeCycleState() == LifeCycle.State.PAUSED);

          stateMachine.reinitialize();

          SnapshotInfo snapshot = stateMachine.getLatestSnapshot();
          Preconditions.assertTrue(snapshot != null && snapshot.getIndex() > lastAppliedIndex,
              "Snapshot: %s, lastAppliedIndex: %s", snapshot, lastAppliedIndex);

          lastAppliedIndex = snapshot.getIndex();
          lastSnapshotIndex = snapshot.getIndex();
          state = State.RUNNING;
        }

        final MemoizedSupplier<List<CompletableFuture<Message>>> futures
            = MemoizedSupplier.valueOf(() -> new ArrayList<>());
        while (lastAppliedIndex < committedIndex) {
          final long nextIndex = lastAppliedIndex + 1;
          final LogEntryProto next = raftLog.get(nextIndex);
          if (next != null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("{}: applying nextIndex={}, nextLog={}",
                  this, nextIndex, ServerProtoUtils.toString(next));
            }
            final CompletableFuture<Message> f = server.applyLogToStateMachine(next);
            if (f != null) {
              futures.get().add(f);
            }
            lastAppliedIndex = nextIndex;
          } else {
            LOG.debug("{}: logEntry {} is null. There may be snapshot to load. state:{}",
                this, nextIndex, state);
            break;
          }
        }

        // check if need to trigger a snapshot
        if (shouldTakeSnapshot()) {
          if (futures.isInitialized()) {
            JavaUtils.allOf(futures.get()).get();
          }
          lastSnapshotIndex = stateMachine.takeSnapshot();
          raftLog.purge(lastSnapshotIndex);
        }

        if (shouldStop()) {
          stop();
        }
      } catch (InterruptedException e) {
        if (!isRunning()) {
          LOG.info("{}: the StateMachineUpdater is interrupted and will exit.", this);
        } else {
          final String s = this + ": the StateMachineUpdater is wrongly interrupted";
          ExitUtils.terminate(1, s, e, LOG);
        }
      } catch (Throwable t) {
        final String s = this + ": the StateMachineUpdater hits Throwable";
        ExitUtils.terminate(2, s, t, LOG);
      }
    }
  }

  private boolean isRunning() {
    return state != State.STOP;
  }

  private boolean shouldStop() {
    return stopIndex != null && getLastAppliedIndex() >= stopIndex;
  }

  private boolean shouldTakeSnapshot() {
    return autoSnapshotEnabled &&
        ( ((state != State.RELOAD)
            && (getLastAppliedIndex() - lastSnapshotIndex
              >= autoSnapshotThreshold))
          || shouldStop()
        );
  }

  long getLastAppliedIndex() {
    return lastAppliedIndex;
  }
}
