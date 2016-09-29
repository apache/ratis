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
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.protocol.Message;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.statemachine.SnapshotInfo;
import org.apache.raft.statemachine.StateMachineStorage;
import org.apache.raft.util.ProtoUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface StateMachine extends Closeable {

  /**
   * The Lifecycle state for the StateMachine. Upon construction, State machine is in NEW state.
   * initialize() will put the SM in RUNNING state if no exceptions. Paused state is for stopping
   * internal compaction or other operations in the SM so that a new snapshot can be installed.
   *
   * TODO: list and verify all possible state transitions
   * NEW --> STARTING -> RUNNING
   *     |      |___________|
   *     |      |
   *     |      V
   *     |-> CLOSING -> CLOSED
   */
  enum State {
    NEW,
    STARTING,
    RUNNING,
    PAUSING,
    PAUSED,
    CLOSING,
    CLOSED
  }

  /**
   * Initializes the State Machine with the given properties and storage. The state machine is
   * responsible reading the latest snapshot from the file system (if any) and initialize itself
   * with the latest term and index there including all the edits.
   */
  void initialize(RaftProperties properties, RaftStorage storage) throws IOException;

  /**
   * Returns the lifecycle state for this StateMachine.
   * @return the lifecycle state.
   */
  State getState();

  /**
   * Pauses the state machine. On return, the state machine should have closed all open files so
   * that a new snapshot can be installed.
   */
  void pause();

  /**
   * Re-initializes the State Machine in PAUSED state with the given properties and storage. The
   * state machine is responsible reading the latest snapshot from the file system (if any) and
   * initialize itself with the latest term and index there including all the edits.
   */
  void reinitialize(RaftProperties properties, RaftStorage storage) throws IOException;

  /**
   * Apply a committed log entry to the state machine.
   * @param entry the log entry that has been committed to a quorum of the raft
   *              peers
   */
  CompletableFuture<Message> applyLogEntry(LogEntryProto entry);

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
   * @return the largest index of the log entry that has been applied to the
   *         state machine and also included in the snapshot. Note the log purge
   *         should be handled separately.
   */
  // TODO: refactor this
  long takeSnapshot() throws IOException;

  /**
   * Record the RaftConfiguration in the state machine. The RaftConfiguration
   * should also be stored in the snapshot.
   */
  void setRaftConfiguration(RaftConfiguration conf);

  /**
   * @return the latest raft configuration recorded in the state machine.
   */
  RaftConfiguration getRaftConfiguration();

  /**
   * @return StateMachineStorage to interact with the durability guarantees provided by the
   * state machine.
   */
  StateMachineStorage getStateMachineStorage();

  /**
   * Returns the information for the latest durable snapshot.
   */
  SnapshotInfo getLatestSnapshot();

  /**
   * Query the state machine. The request must be read-only.
   * TODO: extend RaftClientRequest to have a read-only request subclass.
   */
  CompletableFuture<RaftClientReply> queryStateMachine(RaftClientRequest request);

  /**
   * Validate/pre-process the incoming update request in the state machine.
   * @return the content to be written to the log entry. Null means the request
   * should be rejected.
   * @throws IOException thrown by the state machine while validation
   */
  ClientOperationEntry validateUpdate(RaftClientRequest request)
      throws IOException;

  /**
   * Notify the state machine that the raft peer is no longer leader.
   */
  void notifyNotLeader(Collection<ClientOperationEntry> pendingEntries);

  /**
   * The operation entry to be written into the raft log.
   */
  interface ClientOperationEntry {
    RaftProtos.ClientOperationProto getLogEntryContent();
  }

  class DummyStateMachine implements StateMachine {
    @Override
    public void initialize(RaftProperties properties, RaftStorage storage) {
      // do nothing
    }

    @Override
    public State getState() {
      return null;
    }

    public void pause() {
    }

    @Override
    public void reinitialize(RaftProperties properties, RaftStorage storage) throws IOException {
    }

    @Override
    public CompletableFuture<Message> applyLogEntry(LogEntryProto entry) {
      // return the same message contained in the entry
      Message msg = () -> entry.getClientOperation().getOp().toByteArray();
      return CompletableFuture.completedFuture(msg);
    }

    @Override
    public long takeSnapshot() {
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
    public StateMachineStorage getStateMachineStorage() {
      return new StateMachineStorage() {
        @Override
        public void init(RaftStorage raftStorage) throws IOException {
        }

        @Override
        public SnapshotInfo getLatestSnapshot() {
          return null;
        }

        @Override
        public void format() throws IOException {
        }
      };
    }

    @Override
    public SnapshotInfo getLatestSnapshot() {
      return null;
    }

    @Override
    public CompletableFuture<RaftClientReply> queryStateMachine(
        RaftClientRequest request) {
      return null;
    }

    @Override
    public ClientOperationEntry validateUpdate(RaftClientRequest request)
        throws IOException {
      return () -> RaftProtos.ClientOperationProto.newBuilder()
          .setOp(ProtoUtils.toByteString(request.getMessage().getContent()))
          .build();
    }

    @Override
    public void notifyNotLeader(Collection<ClientOperationEntry> pendingEntries) {

    }

    @Override
    public void close() throws IOException {
      // do nothing
    }

  }

}
