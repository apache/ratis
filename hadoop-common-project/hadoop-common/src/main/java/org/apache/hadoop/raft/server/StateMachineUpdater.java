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
package org.apache.hadoop.raft.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.server.storage.RaftLog;
import org.apache.hadoop.raft.server.storage.RaftStorage;
import org.apache.hadoop.raft.util.RaftUtils;
import org.apache.hadoop.util.Daemon;
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

  private final StateMachine stateMachine;
  private final RaftStorage storage;
  private final RaftLog raftLog;

  private long lastApplied;

  private final boolean autoSnapshotEnabled;
  private final long snapshotThreshold;
  private long lastSnapshotIndex;

  private final Thread updater;
  private volatile boolean running = true;

  StateMachineUpdater(StateMachine stateMachine, RaftLog raftLog,
      RaftStorage storage, long lastApplied, RaftProperties properties) {
    this.stateMachine = stateMachine;
    this.storage = storage;
    this.raftLog = raftLog;

    this.lastApplied = lastApplied;
    lastSnapshotIndex = lastApplied;

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
    running = false;
    updater.interrupt();
    try {
      stateMachine.close();
    } catch (IOException ignored) {
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void run() {
    while (running) {
      try {
        synchronized (this) {
          // when the peers just start, the committedIndex is initialized as 0
          // and will be updated only after the leader contacts other peers.
          // Thus initially lastApplied can be greater than lastCommitted.
          while (lastApplied >= raftLog.getLastCommittedIndex()) {
            wait();
          }
        }

        final long committedIndex = raftLog.getLastCommittedIndex();
        Preconditions.checkState(lastApplied < committedIndex);

        while (lastApplied < committedIndex) {
          final LogEntryProto next = raftLog.get(lastApplied + 1);
          stateMachine.applyLogEntry(next);
          lastApplied++;
        }

        // check if need to trigger a snapshot
        if (shouldTakeSnapshot(lastApplied)) {
          File snapshotFile = storage.getStorageDir().getSnapshotFile(
              lastApplied);
          stateMachine.takeSnapshot(snapshotFile, storage);
          lastSnapshotIndex = lastApplied;
        }
      } catch (InterruptedException e) {
        if (!running) {
          LOG.info("The StateMachineUpdater is interrupted and will exit.");
        } else {
          RaftUtils.terminate(e,
              "The StateMachineUpdater is wrongly interrupted", LOG);
        }
      } catch (Throwable t) {
        RaftUtils.terminate(t,
            "The StateMachineUpdater hits Throwable", LOG);
      }
    }
  }

  private boolean shouldTakeSnapshot(long currentAppliedIndex) {
    return autoSnapshotEnabled &&
        (currentAppliedIndex - lastSnapshotIndex >= snapshotThreshold);
  }

  @VisibleForTesting
  StateMachine getStateMachine() {
    return stateMachine;
  }
}
