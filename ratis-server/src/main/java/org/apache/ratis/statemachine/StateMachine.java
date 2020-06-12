/*
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
package org.apache.ratis.statemachine;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * StateMachine is the entry point for the custom implementation of replicated state as defined in
 * the "State Machine Approach" in the literature
 * (see https://en.wikipedia.org/wiki/State_machine_replication).
 */
public interface StateMachine extends Closeable {
  Logger LOG = LoggerFactory.getLogger(StateMachine.class);

  /** A registry to support different state machines in multi-raft environment. */
  interface Registry extends Function<RaftGroupId, StateMachine> {
  }

  interface DataApi {
    /** A noop implementation of {@link DataApi}. */
    DataApi DEFAULT = new DataApi() {};

    /**
     * Read asynchronously the state machine data from this state machine.
     *
     * @return a future for the read task.
     */
    default CompletableFuture<ByteString> read(LogEntryProto entry) {
      return CompletableFuture.completedFuture(null);
    }

    /**
     * Write asynchronously the state machine data in the given log entry to this state machine.
     *
     * @return a future for the write task
     */
    default CompletableFuture<?> write(LogEntryProto entry) {
      return CompletableFuture.completedFuture(null);
    }

    /**
     * Create asynchronously a {@link DataStream} to stream state machine data.
     * The state machine may use the first message (i.e. request.getMessage()) as the header to create the stream.
     *
     * @return a future of the stream.
     */
    default CompletableFuture<DataStream> stream(RaftClientRequest request) {
      return CompletableFuture.completedFuture(null);
    }

    /**
     * Link asynchronously the given stream with the given log entry.
     *
     * @return a future for the link task.
     */
    default CompletableFuture<?> link(DataStream stream, LogEntryProto entry) {
      return CompletableFuture.completedFuture(null);
    }

    /**
     * Flush the state machine data till the given log index.
     *
     * @param logIndex The log index to flush.
     * @return a future for the flush task.
     */
    default CompletableFuture<Void> flush(long logIndex) {
      return CompletableFuture.completedFuture(null);
    }

    /**
     * Truncates asynchronously the state machine data to the given log index.
     * It is a noop if the corresponding log entry does not have state machine data.
     *
     * @param logIndex The last log index after truncation.
     * @return a future for truncate task.
     */
    default CompletableFuture<Void> truncate(long logIndex) {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * For streaming state machine data.
   */
  interface DataStream {
    /** @return a channel for streaming state machine data. */
    WritableByteChannel getWritableByteChannel();

    /**
     * Clean up asynchronously this stream.
     *
     * When there is an error, this method is invoked to clean up the associated resources.
     * If this stream is not yet linked (see {@link DataApi#link(DataStream, LogEntryProto)}),
     * the state machine may choose to remove the data or to keep the data internally for future recovery.
     * If this stream is already linked, the data must not be removed.
     *
     * @return a future for the cleanup task.
     */
    CompletableFuture<?> cleanUp();
  }

  /**
   * Get the optional {@link DataApi} object.
   *
   * If this {@link StateMachine} chooses to support {@link DataApi},
   * it may either implement {@link DataApi} directly or override this method to return a {@link DataApi} object.
   *
   * Otherwise, this {@link StateMachine} does not support {@link DataApi}.
   * Then, this method returns the default noop {@link DataApi} object.
   *
   * @return The optional {@link DataApi} object.
   */
  default DataApi data() {
    return this instanceof DataApi? (DataApi)this : DataApi.DEFAULT;
  }

  /**
   * Initializes the State Machine with the given server, group and storage. The state machine is
   * responsible reading the latest snapshot from the file system (if any) and initialize itself
   * with the latest term and index there including all the edits.
   */
  void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException;

  /**
   * Returns the lifecycle state for this StateMachine.
   * @return the lifecycle state.
   */
  LifeCycle.State getLifeCycleState();

  /**
   * Pauses the state machine. On return, the state machine should have closed all open files so
   * that a new snapshot can be installed.
   */
  void pause();

  /**
   * Re-initializes the State Machine in PAUSED state. The
   * state machine is responsible reading the latest snapshot from the file system (if any) and
   * initialize itself with the latest term and index there including all the edits.
   */
  void reinitialize() throws IOException;

  /**
   * Dump the in-memory state into a snapshot file in the RaftStorage. The
   * StateMachine implementation can decide 1) its own snapshot format, 2) when
   * a snapshot is taken, and 3) how the snapshot is taken (e.g., whether the
   * snapshot blocks the state machine, and whether to purge log entries after
   * a snapshot is done).
   *
   * In the meanwhile, when the size of raft log outside of the latest snapshot
   * exceeds certain threshold, the RaftServer may choose to trigger a snapshot
   * if {@link org.apache.ratis.server.RaftServerConfigKeys.Snapshot#AUTO_TRIGGER_ENABLED_KEY} is enabled.
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
   */
  CompletableFuture<Message> query(Message request);

  /**
   * Query the state machine, provided minIndex <= commit index.
   * The request must be read-only.
   * Since the commit index of this server may lag behind the Raft service,
   * the returned result may possibly be stale.
   *
   * When minIndex > {@link #getLastAppliedTermIndex()},
   * the state machine may choose to either
   * (1) return exceptionally, or
   * (2) wait until minIndex <= {@link #getLastAppliedTermIndex()} before running the query.
   */
  CompletableFuture<Message> queryStale(Message request, long minIndex);

  /**
   * Validate/pre-process the incoming update request in the state machine.
   * @return the content to be written to the log entry. Null means the request
   * should be rejected.
   * @throws IOException thrown by the state machine while validation
   */
  TransactionContext startTransaction(RaftClientRequest request) throws IOException;

  /**
   * This is called before the transaction passed from the StateMachine is appended to the raft log.
   * This method will be called from log append and having the same strict serial order that the
   * transactions will have in the RAFT log. Since this is called serially in the critical path of
   * log append, it is important to do only required operations here.
   * @return The Transaction context.
   */
  TransactionContext preAppendTransaction(TransactionContext trx) throws IOException;

  /**
   * Called to notify the state machine that the Transaction passed cannot be appended (or synced).
   * The exception field will indicate whether there was an exception or not.
   * @param trx the transaction to cancel
   * @return cancelled transaction
   */
  TransactionContext cancelTransaction(TransactionContext trx) throws IOException;

  /**
   * Called for transactions that have been committed to the RAFT log. This step is called
   * sequentially in strict serial order that the transactions have been committed in the log.
   * The SM is expected to do only necessary work, and leave the actual apply operation to the
   * applyTransaction calls that can happen concurrently.
   * @param trx the transaction state including the log entry that has been committed to a quorum
   *            of the raft peers
   * @return The Transaction context.
   */
  TransactionContext applyTransactionSerial(TransactionContext trx);

  /**
   * Called to notify state machine about indexes which are processed
   * internally by Raft Server, this currently happens when conf entries are
   * processed in raft Server. This keep state machine to keep a track of index
   * updates.
   * @param term term of the current log entry
   * @param index index which is being updated
   */
  default void notifyIndexUpdate(long term, long index) {

  }

  /**
   * Apply a committed log entry to the state machine. This method can be called concurrently with
   * the other calls, and there is no guarantee that the calls will be ordered according to the
   * log commit order.
   * @param trx the transaction state including the log entry that has been committed to a quorum
   *            of the raft peers
   */
  // TODO: We do not need to return CompletableFuture
  CompletableFuture<Message> applyTransaction(TransactionContext trx);

  TermIndex getLastAppliedTermIndex();

  /**
   * Notify the state machine that the raft peer is no longer leader.
   */
  void notifyNotLeader(Collection<TransactionContext> pendingEntries) throws IOException;

  /**
   * Notify the Leader's state machine that one of the followers is slow
   * this notification is based on "raft.server.rpc.slowness.timeout"
   *
   * @param roleInfoProto information about the current node role and rpc delay information
   */
  default void notifySlowness(RoleInfoProto roleInfoProto) {

  }

  /**
   * Notify the state machine that the pipeline has failed.
   * This notification is triggered when a log operation throws an Exception.
   * @param t Exception which was caught, indicates possible cause.
   * @param failedEntry if append failed for a specific entry, null otherwise.
   */
  default void notifyLogFailed(Throwable t, LogEntryProto failedEntry) {

  }

  /**
   * Notify the Leader's state machine that a leader has not been elected for a long time
   * this notification is based on "raft.server.leader.election.timeout"
   *
   * @param roleInfoProto information about the current node role and rpc delay information
   */
  default void notifyExtendedNoLeader(RoleInfoProto roleInfoProto) {

  }

  /**
   * Notify the Follower's state machine that the leader has purged entries
   * from its log and hence to catch up, the Follower state machine would have
   * to install the latest snapshot.
   * @param firstTermIndexInLog TermIndex of the first append entry available
   *                           in the Leader's log.
   * @param roleInfoProto information about the current node role and
   *                            rpc delay information
   * @return After the snapshot installation is complete, return the last
   * included term index in the snapshot.
   */
  default CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Notify the state machine that a RaftPeer has been elected as leader.
   */
  default void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId raftPeerId) {
  }

  /**
   * Notify about group removal in the state machine. This function is called
   * during group removal after all the pending transactions have been applied
   * by the state machine.
   */
  default void notifyGroupRemove() {
  }

  /**
   * Converts the proto object into a useful log string to add information about state machine data.
   * @param proto state machine proto
   * @return the string representation of the proto.
   */
  default String toStateMachineLogEntryString(RaftProtos.StateMachineLogEntryProto proto) {
    return ServerProtoUtils.toStateMachineLogEntryString(proto, null);
  }
}
