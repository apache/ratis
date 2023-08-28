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

import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.ReferenceCountedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * StateMachine is the entry point for the custom implementation of replicated state as defined in
 * the "State Machine Approach" in the literature
 * (see https://en.wikipedia.org/wiki/State_machine_replication).
 *
 *  A {@link StateMachine} implementation must be threadsafe.
 *  For example, the {@link #applyTransaction(TransactionContext)} method and the {@link #query(Message)} method
 *  can be invoked in parallel.
 */
public interface StateMachine extends Closeable {
  Logger LOG = LoggerFactory.getLogger(StateMachine.class);

  /** A registry to support different state machines in multi-raft environment. */
  interface Registry extends Function<RaftGroupId, StateMachine> {
  }

  /**
   * An optional API for managing data outside the raft log.
   * For data intensive applications, it can be more efficient to implement this API
   * in order to support zero buffer coping and a light-weighted raft log.
   */
  interface DataApi {
    /** A noop implementation of {@link DataApi}. */
    DataApi DEFAULT = new DataApi() {};

    /**
     * Read asynchronously the state machine data from this state machine.
     *
     * @return a future for the read task.
     */
    default CompletableFuture<ByteString> read(LogEntryProto entry) {
      throw new UnsupportedOperationException("This method is NOT supported.");
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
     * The given stream can be null if it is unavailable due to errors.
     * In such case, the state machine may either recover the data by itself
     * or complete the returned future exceptionally.
     *
     * @param stream the stream, which can possibly be null, to be linked.
     * @param entry the log entry to be linked.
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
   * An optional API for event notifications.
   */
  interface EventApi {
    /** A noop implementation of {@link EventApi}. */
    EventApi DEFAULT = new EventApi() {};

    /**
     * Notify the {@link StateMachine} that a new leader has been elected.
     * Note that the new leader can possibly be this server.
     *
     * @param groupMemberId The id of this server.
     * @param newLeaderId The id of the new leader.
     */
    default void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {}

    /**
     * Notify the {@link StateMachine} a term-index update event.
     * This method will be invoked when a {@link MetadataProto}
     * or {@link RaftConfigurationProto} is processed.
     * For {@link StateMachineLogEntryProto}, this method will not be invoked.
     *
     * @param term The term of the log entry
     * @param index The index of the log entry
     */
    default void notifyTermIndexUpdated(long term, long index) {}

    /**
     * Notify the {@link StateMachine} a configuration change.
     * This method will be invoked when a {@link RaftConfigurationProto} is processed.
     *
     * @param term term of the current log entry
     * @param index index which is being updated
     * @param newRaftConfiguration new configuration
     */
    default void notifyConfigurationChanged(long term, long index, RaftConfigurationProto newRaftConfiguration) {}

    /**
     * Notify the {@link StateMachine} a group removal event.
     * This method is invoked after all the pending transactions have been applied by the {@link StateMachine}.
     */
    default void notifyGroupRemove() {}

    /**
     * Notify the {@link StateMachine} that a log operation failed.
     *
     * @param cause The cause of the failure.
     * @param failedEntry The failed log entry, if there is any.
     */
    default void notifyLogFailed(Throwable cause, LogEntryProto failedEntry) {}

    /**
     * Notify the {@link StateMachine} that the progress of install snapshot is
     * completely done. Could trigger the cleanup of snapshots.
     *
     * @param result {@link InstallSnapshotResult}
     * @param snapshotIndex the index of installed snapshot
     * @param peer the peer who installed the snapshot
     */
    default void notifySnapshotInstalled(InstallSnapshotResult result, long snapshotIndex,  RaftPeer peer) {}

    /**
     * Notify the {@link StateMachine} that a raft server has step down.
     */
    default void notifyServerShutdown(RoleInfoProto roleInfo) {}
  }

  /**
   * An optional API for leader-only event notifications.
   * The method in this interface will be invoked only when the server is the leader.
   */
  interface LeaderEventApi {
    /** A noop implementation of {@link LeaderEventApi}. */
    LeaderEventApi DEFAULT = new LeaderEventApi() {};

    /**
     * Notify the {@link StateMachine} that the given follower is slow.
     * This notification is based on "raft.server.rpc.slowness.timeout".
     *
     * @param leaderInfo information about the current node role and rpc delay information
     * @param slowFollower The follower being slow.
     *
     * @see org.apache.ratis.server.RaftServerConfigKeys.Rpc#SLOWNESS_TIMEOUT_KEY
     */
    default void notifyFollowerSlowness(RoleInfoProto leaderInfo, RaftPeer slowFollower) {}

    /** @deprecated Use {@link #notifyFollowerSlowness(RoleInfoProto, RaftPeer)}. */
    @Deprecated
    default void notifyFollowerSlowness(RoleInfoProto leaderInfo) {}

    /**
     * Notify {@link StateMachine} that this server is no longer the leader.
     */
    default void notifyNotLeader(Collection<TransactionContext> pendingEntries) throws IOException {}

    /**
     * Notify the {@link StateMachine} that this server becomes ready after changed to leader.
     */
    default void notifyLeaderReady() {}
  }

  /**
   * An optional API for follower-only event notifications.
   * The method in this interface will be invoked only when the server is a follower.
   */
  interface FollowerEventApi {
    /** A noop implementation of {@link FollowerEventApi}. */
    FollowerEventApi DEFAULT = new FollowerEventApi() {};

    /**
     * Notify the {@link StateMachine} that there is no leader in the group for an extended period of time.
     * This notification is based on "raft.server.notification.no-leader.timeout".
     *
     * @param roleInfoProto information about the current node role and rpc delay information
     *
     * @see org.apache.ratis.server.RaftServerConfigKeys.Notification#NO_LEADER_TIMEOUT_KEY
     */
    default void notifyExtendedNoLeader(RoleInfoProto roleInfoProto) {}

    /**
     * Notify the {@link StateMachine} that the leader has purged entries from its log.
     * In order to catch up, the {@link StateMachine} has to install the latest snapshot asynchronously.
     *
     * @param roleInfoProto information about the current node role and rpc delay information.
     * @param firstTermIndexInLog The term-index of the first append entry available in the leader's log.
     * @return return the last term-index in the snapshot after the snapshot installation.
     */
    default CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
        RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * For write state machine data.
   */
  interface DataChannel extends WritableByteChannel {
    /**
     * This method is the same as {@link WritableByteChannel#write(ByteBuffer)}.
     *
     * If the implementation has overridden {@link #write(ReferenceCountedObject)},
     * then it does not have to override this method.
     */
    @Override
    default int write(ByteBuffer buffer) throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * Similar to {@link #write(ByteBuffer)}
     * except that the parameter is a {@link ReferenceCountedObject}.
     *
     * This is an optional method.
     * The default implementation is the same as write(referenceCountedBuffer.get()).
     *
     * The implementation may choose to override this method in order to retain the buffer for later use.
     *
     * - If the buffer is retained, it must be released afterward.
     *   Otherwise, the buffer will not be returned, and it will cause a memory leak.
     *
     * - If the buffer is retained multiple times, it must be released the same number of time.
     *
     * - It is safe to access the buffer before this method returns with or without retaining it.
     *
     * - If the buffer is not retained but is accessed after this method returns,
     *   the content of the buffer could possibly be changed unexpectedly, and it will cause data corruption.
     */
    default int write(ReferenceCountedObject<ByteBuffer> referenceCountedBuffer) throws IOException {
      return write(referenceCountedBuffer.get());
    }

    /**
     * Similar to {@link java.nio.channels.FileChannel#force(boolean)},
     * the underlying implementation should force writing the data and/or metadata to the underlying storage.
     *
     * @param metadata Should the metadata be forced?
     * @throws IOException If there are IO errors.
     */
    void force(boolean metadata) throws IOException;
  }

  /**
   * For streaming state machine data.
   */
  interface DataStream {
    /** @return a channel for streaming state machine data. */
    DataChannel getDataChannel();

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

    /**
     * @return an {@link Executor} for executing the streaming tasks of this stream.
     *         If the returned value is null, the default {@link Executor} will be used.
     */
    default Executor getExecutor() {
      return null;
    }
  }

  /**
   * Get the {@link DataApi} object.
   *
   * If this {@link StateMachine} chooses to support the optional {@link DataApi},
   * it may either implement {@link DataApi} directly or override this method to return a {@link DataApi} object.
   * Otherwise, this {@link StateMachine} does not support {@link DataApi}.
   * Then, this method returns the default noop {@link DataApi} object.
   *
   * @return The {@link DataApi} object.
   */
  default DataApi data() {
    return this instanceof DataApi? (DataApi)this : DataApi.DEFAULT;
  }

  /**
   * Get the {@link EventApi} object.
   *
   * If this {@link StateMachine} chooses to support the optional {@link EventApi},
   * it may either implement {@link EventApi} directly or override this method to return an {@link EventApi} object.
   * Otherwise, this {@link StateMachine} does not support {@link EventApi}.
   * Then, this method returns the default noop {@link EventApi} object.
   *
   * @return The {@link EventApi} object.
   */
  default EventApi event() {
    return this instanceof EventApi ? (EventApi)this : EventApi.DEFAULT;
  }

  /**
   * Get the {@link LeaderEventApi} object.
   *
   * If this {@link StateMachine} chooses to support the optional {@link LeaderEventApi},
   * it may either implement {@link LeaderEventApi} directly
   * or override this method to return an {@link LeaderEventApi} object.
   * Otherwise, this {@link StateMachine} does not support {@link LeaderEventApi}.
   * Then, this method returns the default noop {@link LeaderEventApi} object.
   *
   * @return The {@link LeaderEventApi} object.
   */
  default LeaderEventApi leaderEvent() {
    return this instanceof LeaderEventApi? (LeaderEventApi)this : LeaderEventApi.DEFAULT;
  }

  /**
   * Get the {@link FollowerEventApi} object.
   *
   * If this {@link StateMachine} chooses to support the optional {@link FollowerEventApi},
   * it may either implement {@link FollowerEventApi} directly
   * or override this method to return an {@link FollowerEventApi} object.
   * Otherwise, this {@link StateMachine} does not support {@link FollowerEventApi}.
   * Then, this method returns the default noop {@link FollowerEventApi} object.
   *
   * @return The {@link LeaderEventApi} object.
   */
  default FollowerEventApi followerEvent() {
    return this instanceof FollowerEventApi? (FollowerEventApi)this : FollowerEventApi.DEFAULT;
  }

  /**
   * Initializes the State Machine with the given parameter.
   * The state machine must, if there is any, read the latest snapshot.
   */
  void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException;

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
   * Start a transaction for the given request.
   * This method can be invoked in parallel when there are multiple requests.
   * The implementation should validate the request,
   * prepare a {@link StateMachineLogEntryProto},
   * and then build a {@link TransactionContext}.
   * The implementation should also be light-weighted.
   *
   * @return null if the request should be rejected.
   *         Otherwise, return a transaction with the content to be written to the log.
   * @throws IOException thrown by the state machine while validation
   *
   * @see TransactionContext.Builder
   */
  TransactionContext startTransaction(RaftClientRequest request) throws IOException;

  /**
   * This is called before the transaction passed from the StateMachine is appended to the raft log.
   * This method is called with the same strict serial order as the transaction order in the raft log.
   * Since this is called serially in the critical path of log append,
   * it is important to do only required operations here.
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
  TransactionContext applyTransactionSerial(TransactionContext trx) throws InvalidProtocolBufferException;

  /**
   * Apply a committed log entry to the state machine. This method is called sequentially in
   * strict serial order that the transactions have been committed in the log. Note that this
   * method, which returns a future, is asynchronous. The state machine implementation may
   * choose to apply the log entries in parallel. In that case, the order of applying entries to
   * state machine could possibly be different from the log commit order.
   *
   * The implementation must be deterministic so that the raft log can be replayed in any raft peers.
   * Note that, if there are three or more servers,
   * the Raft algorithm makes sure the that log remains consistent
   * even if there are hardware errors in one machine (or less than the majority number of machines).
   *
   * Any exceptions thrown in this method are treated as unrecoverable errors (such as hardware errors).
   * The server will be shut down when it occurs.
   * Administrators should manually fix the underlying problem and then restart the machine.
   *
   * @param trx the transaction state including the log entry that has been replicated to a majority of the raft peers.
   *
   * @return a future containing the result message of the transaction,
   *         where the result message will be replied to the client.
   *         When there is an application level exception (e.g. access denied),
   *         the state machine may complete the returned future exceptionally.
   *         The exception will be wrapped in an {@link org.apache.ratis.protocol.exceptions.StateMachineException}
   *         and then replied to the client.
   */
  CompletableFuture<Message> applyTransaction(TransactionContext trx);

  /** @return the last term-index applied by this {@link StateMachine}. */
  TermIndex getLastAppliedTermIndex();

  /**
   * Converts the given proto to a string.
   *
   * @param proto state machine proto
   * @return the string representation of the proto.
   */
  default String toStateMachineLogEntryString(StateMachineLogEntryProto proto) {
    return JavaUtils.getClassSimpleName(proto.getClass()) +  ":" + ClientInvocationId.valueOf(proto);
  }
}
