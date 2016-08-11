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

import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.proto.RaftProtos;
import org.apache.hadoop.raft.server.storage.RaftStorage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface StateMachine extends Closeable {
  /**
   * Pass in the RaftProperties and RaftStorage for later usage.
   */
  void initialize(RaftProperties properties, RaftStorage storage);

  /**
   * Apply a committed log entry to the state machine
   * @param entry the log entry that has been committed to a quorum of the raft
   *              peers
   */
  void applyLogEntry(RaftProtos.LogEntryProto entry);

  /**
   * Dump the in-memory state into a snapshot file in the RaftStorage. The
   * StateMachine implementation can decide 1) its own snapshot format, 2) when
   * a snapshot is taken, and 3) how the snapshot is taken (e.g., whether the
   * snapshot blocks the state machine, and whether to purge log entries after
   * a snapshot is done).
   *
   * In the meanwhile, when the size of raft log outside of the latest snapshot
   * exceeds certain threshold, the RaftServer may choose to trigger a snapshot
   * if {@link RaftServerConfigKeys#RAFT_SERVER_AUTO_SNAPSHOT_ENABLED_KEY} is
   * enabled.
   *
   * @param snapshotFile the file where the snapshot is written
   * @param storage the RaftStorage where the snapshot file is stored
   * @return the largest index of the log entry that has been applied to the
   *         state machine and also included in the snapshot. Note the log purge
   *         should be handled separately.
   */
  long takeSnapshot(File snapshotFile, RaftStorage storage);

  /**
   * Load states from the given snapshot file.
   * @param snapshotFile the snapshot file
   * @return the largest log entry index that is applied to the state machine
   *         while loading the snapshot.
   */
  long loadSnapshot(File snapshotFile) throws IOException;

  /**
   * Reset the whole state machine, and then load states from the snapshot file.
   */
  long reloadSnapshot(File snapshotFile) throws IOException;

  class DummyStateMachine implements StateMachine {
    @Override
    public void initialize(RaftProperties properties, RaftStorage storage) {
      // do nothing
    }

    @Override
    public void applyLogEntry(RaftProtos.LogEntryProto entry) {
      // do nothing
    }

    @Override
    public long takeSnapshot(File snapshotFile, RaftStorage storage) {
      return RaftConstants.INVALID_LOG_INDEX;
    }

    @Override
    public long loadSnapshot(File snapshotFile) throws IOException {
      return RaftConstants.INVALID_LOG_INDEX;
    }

    @Override
    public long reloadSnapshot(File snapshotFile) throws IOException {
      return RaftConstants.INVALID_LOG_INDEX;
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }
  }

}
