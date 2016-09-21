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

import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos;
import org.apache.raft.protocol.Message;
import org.apache.raft.server.storage.RaftStorage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface StateMachine extends Closeable {
  /**
   * Pass in the RaftProperties and RaftStorage for later usage.
   */
  void initialize(RaftProperties properties, RaftStorage storage);

  /**
   * Apply a committed log entry to the state machine.
   * @param entry the log entry that has been committed to a quorum of the raft
   *              peers
   * @throws Exception exception when apply the log entry. This may happen if
   *                   the op in the log entry is invalid to the state machine.
   */
  Message applyLogEntry(RaftProtos.LogEntryProto entry) throws Exception;

  Message query(Message query) throws Exception;

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
   * The snapshot should include the latest raft configuration.
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

  /**
   * Record the RaftConfiguration in the state machine. The RaftConfiguration
   * should also be stored in the snapshot.
   */
  void setRaftConfiguration(RaftConfiguration conf);

  /**
   * @return the latest raft configuration recorded in the state machine.
   */
  RaftConfiguration getRaftConfiguration();

  class DummyStateMachine implements StateMachine {
    @Override
    public void initialize(RaftProperties properties, RaftStorage storage) {
      // do nothing
    }

    @Override
    public Message applyLogEntry(RaftProtos.LogEntryProto entry) {
      // do nothing
      return null;
    }

    @Override
    public Message query(Message query) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public long takeSnapshot(File snapshotFile, RaftStorage storage) {
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    @Override
    public long loadSnapshot(File snapshotFile) throws IOException {
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    @Override
    public long reloadSnapshot(File snapshotFile) throws IOException {
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    @Override
    public void setRaftConfiguration(RaftConfiguration conf) {
      // do nothing
    }

    @Override
    public RaftConfiguration getRaftConfiguration() {
      return null;
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }
  }

}
