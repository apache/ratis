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
package org.apache.ratis.server;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.ReadRequestTypeProto;
import org.apache.ratis.protocol.AdminAsynchronousProtocol;
import org.apache.ratis.protocol.AdminProtocol;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientAsynchronousProtocol;
import org.apache.ratis.protocol.RaftClientProtocol;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.metrics.RaftServerMetrics;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.common.collect.Iterables;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Raft server interface */
public interface RaftServer extends Closeable, RpcType.Get,
    RaftServerProtocol, RaftServerAsynchronousProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol,
    AdminProtocol, AdminAsynchronousProtocol {
  Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  /** A division of a {@link RaftServer} for a particular {@link RaftGroup}. */
  interface Division extends Closeable {
    Logger LOG = LoggerFactory.getLogger(Division.class);

    enum LeadershipStatus {
      /** The division is a leader and it is ready. */
      LEADER_READY(null),
      /** The division is a leader, but it is NOT ready. */
      LEADER_NOT_READY(LeaderNotReadyException.class),
      /** The division is a leader, but it is stepping down. */
      LEADER_STEPPING_DOWN(LeaderSteppingDownException.class),
      /** The division is NOT a leader; */
      NOT_LEADER(NotLeaderException.class);

      private final Class<? extends RaftException> exceptionClass;

      LeadershipStatus(Class<? extends RaftException> exceptionClass) {
        this.exceptionClass = exceptionClass;
      }

      /**
       *  @return For {@link #LEADER_READY}, return null.
       *          Otherwise, return the corresponding exception class.
       */
      public Class<? extends RaftException> getExceptionClass() {
        return exceptionClass;
      }
    }

    /** @return the {@link DivisionProperties} for this division. */
    DivisionProperties properties();

    /** @return the {@link RaftGroupMemberId} for this division. */
    RaftGroupMemberId getMemberId();

    /** @return the {@link RaftPeerId} for this division. */
    default RaftPeerId getId() {
      return getMemberId().getPeerId();
    }

    /** @return the {@link RaftPeer} for this division. */
    default RaftPeer getPeer() {
      return Optional.ofNullable(getRaftConf().getPeer(getId(), RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER))
        .orElseGet(() -> getRaftServer().getPeer());
    }

    /** @return the information about this division. */
    DivisionInfo getInfo();

    /** @return the current leadership status of this division. */
    LeadershipStatus getCurrentLeadershipStatus();

    /**
     * Convert the given leadership status (not necessarily the current status) to an exception.
     *
     * @return For {@link LeadershipStatus#LEADER_READY}, return null.
     *         Otherwise, return an exception corresponding to {@link LeadershipStatus#getExceptionClass()}.
     */
    RaftException newLeadershipException(LeadershipStatus leadershipStatus);

    /** @return the {@link RaftGroup} for this division. */
    default RaftGroup getGroup() {
      final Collection<RaftPeer> allFollowerPeers = getRaftConf().getAllPeers(RaftPeerRole.FOLLOWER);
      final Collection<RaftPeer> allListenerPeers = getRaftConf().getAllPeers(RaftPeerRole.LISTENER);
      Iterable<RaftPeer> peers = Iterables.concat(allFollowerPeers, allListenerPeers);
      return RaftGroup.valueOf(getMemberId().getGroupId(), peers);
    }

    /** @return the current {@link RaftConfiguration} for this division. */
    RaftConfiguration getRaftConf();

    /** @return the {@link RaftServer} containing this division. */
    RaftServer getRaftServer();

    /** @return the {@link RaftServerMetrics} for this division. */
    RaftServerMetrics getRaftServerMetrics();

    /** @return the {@link StateMachine} for this division. */
    StateMachine getStateMachine();

    /**
     * Execute a local read-only query after applying the configured
     * {@link RaftServerConfigKeys.Read.Option} consistency checks.
     *
     * <p>This API is intended for embedded users that already have a local
     * server division.  Applications may use local {@link Message}
     * implementations so that their state machine can avoid materializing
     * application request bytes.  Remote clients should continue to use
     * {@link org.apache.ratis.client.api.AsyncApi#sendReadOnly(Message)}.
     *
     * <p>This is an experimental API, subject to incompatible changes and removal in future releases.
     */
    default CompletableFuture<Message> readOnlyAsync(Message queryMessage) throws IOException {
      return readOnlyAsync(ClientId.randomId(), ReadRequestTypeProto.getDefaultInstance(), queryMessage);
    }

    /**
     * Execute a local read-only query using the given executor after the configured consistency checks.
     * See {@link #readOnlyAsync(ClientId, ReadRequestTypeProto, Message, Executor)} for executor semantics.
     *
     * <p>This is an experimental API, subject to incompatible changes and removal in future releases.
     */
    default CompletableFuture<Message> readOnlyAsync(Message queryMessage, Executor executor) throws IOException {
      return readOnlyAsync(ClientId.randomId(), ReadRequestTypeProto.getDefaultInstance(), queryMessage, executor);
    }

    /**
     * Execute a local read-only query after applying the configured
     * {@link RaftServerConfigKeys.Read.Option} consistency checks.
     *
     * <p>The {@code clientId} and {@code readRequestType} identify the read
     * semantics.  In particular, callers requesting read-after-write
     * consistency must use the same client ID as their prior writes.
     * The query is submitted to the division's server executor.
     *
     * <p>This is an experimental API, subject to incompatible changes and removal in future releases.
     */
    default CompletableFuture<Message> readOnlyAsync(ClientId clientId, ReadRequestTypeProto readRequestType,
        Message queryMessage) throws IOException {
      return readOnlyAsync(clientId, readRequestType, queryMessage, null);
    }

    /**
     * Execute a local read-only query after applying the configured consistency checks,
     * submitting {@link StateMachine#query(Message)} to the given executor.
     *
     * <p>A null executor selects the division's existing server executor, which is shared with other server work.
     * A supplied executor remains owned by the caller. To keep queries off Ratis completion threads,
     * use an executor that dispatches to another thread rather than executing inline or using a caller-runs policy.
     * Applications are responsible for propagating any required thread-local context to the query thread.
     *
     * <p>This API does not coordinate query execution with snapshot installation.
     * The state machine remains responsible for safe access to its query resources.
     *
     * <p>This is an experimental API, subject to incompatible changes and removal in future releases.
     *
     * @param clientId the client requesting the read; use the same ID as prior writes for read-after-write consistency.
     * @param readRequestType the read consistency semantics.
     * @param queryMessage the local state machine query.
     * @param executor the query executor, or null to use the division's server executor.
     * @return the query result, or an exceptional future if the read, query, or executor submission fails.
     */
    default CompletableFuture<Message> readOnlyAsync(ClientId clientId, ReadRequestTypeProto readRequestType,
        Message queryMessage, Executor executor) throws IOException {
      throw new UnsupportedOperationException("readOnlyAsync is not supported");
    }

    /**
     * Establish a local read barrier using the default read request semantics.
     * Remote clients should use
     * {@link org.apache.ratis.client.api.AsyncApi#sendReadOnly(org.apache.ratis.protocol.Message)}.
     *
     * <p>This is an experimental API, subject to incompatible changes and removal in future releases.
     *
     * @see #readIndexAsync(ClientId, ReadRequestTypeProto)
     */
    default CompletableFuture<Long> readIndexAsync() throws IOException {
      return readIndexAsync(ClientId.randomId(), ReadRequestTypeProto.getDefaultInstance());
    }

    /**
     * Establish a read barrier without executing a state machine query.
     * The returned future completes with the confirmed ReadIndex only after that index has been applied locally.
     * This API uses the existing ReadIndex configuration regardless of the configured
     * {@link RaftServerConfigKeys.Read.Option}.
     *
     * <p>For read-after-write consistency, use the same client ID as the prior writes and set
     * {@code readAfterWriteConsistent} in {@code readRequestType}.
     * The existing ReadIndex and local apply-wait timeouts apply; this API does not introduce an overall timeout.
     * Cancelling or timing out the returned future does not cancel shared work or other readers.
     *
     * <p>Embedded callers can wait for this future and then execute their local query on the calling thread,
     * or use an asynchronous continuation with an application executor. Non-async continuations may execute on
     * Ratis completion threads, so attaching a query that way does not isolate query execution from the barrier.
     *
     * <p>This is an experimental API, subject to incompatible changes and removal in future releases.
     *
     * @param clientId the client requesting the read barrier.
     * @param readRequestType the read consistency semantics.
     * @return the confirmed ReadIndex, which may be less than the current local applied index.
     * @throws IllegalArgumentException if {@code preferNonLinearizable} is set.
     */
    default CompletableFuture<Long> readIndexAsync(ClientId clientId, ReadRequestTypeProto readRequestType)
        throws IOException {
      throw new UnsupportedOperationException("readIndexAsync is not supported");
    }

    /** @return the raft log of this division. */
    RaftLog getRaftLog();

    /** @return the storage of this division. */
    RaftStorage getRaftStorage();

    /** @return the commit information of this division. */
    Collection<CommitInfoProto> getCommitInfos();

    /** @return the retry cache of this division. */
    RetryCache getRetryCache();

    /** @return the data stream map of this division. */
    DataStreamMap getDataStreamMap();

    /** @return the {@link ThreadGroup} the threads of this Division belong to. */
    ThreadGroup getThreadGroup();

    @Override
    void close();
  }

  /** @return the server ID. */
  RaftPeerId getId();

  /**
   * @return the general {@link RaftPeer} for this server.
   *         To obtain a specific {@link RaftPeer} for a {@link RaftGroup}, use {@link Division#getPeer()}.
   */
  RaftPeer getPeer();

  /** @return the group IDs the server is part of. */
  Iterable<RaftGroupId> getGroupIds();

  /** @return the groups the server is part of. */
  Iterable<RaftGroup> getGroups() throws IOException;

  Division getDivision(RaftGroupId groupId) throws IOException;

  /** @return the server properties. */
  RaftProperties getProperties();

  /** @return the rpc service. */
  RaftServerRpc getServerRpc();

  /** @return the data stream rpc service. */
  DataStreamServerRpc getDataStreamServerRpc();

  /** @return the {@link RpcType}. */
  default RpcType getRpcType() {
    return getFactory().getRpcType();
  }

  /** @return the factory for creating server components. */
  ServerFactory getFactory();

  /** Start this server. */
  void start() throws IOException;

  LifeCycle.State getLifeCycleState();

  /** @return a {@link Builder}. */
  static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link RaftServer} objects. */
  class Builder {
    private static final Method NEW_RAFT_SERVER_METHOD = initNewRaftServerMethod();

    private static Method initNewRaftServerMethod() {
      final String className = RaftServer.class.getPackage().getName() + ".impl.ServerImplUtils";
      final Class<?>[] argClasses = {RaftPeerId.class, RaftGroup.class, RaftStorage.StartupOption.class,
          StateMachine.Registry.class, ThreadGroup.class, RaftProperties.class, Parameters.class};
      try {
        final Class<?> clazz = ReflectionUtils.getClassByName(className);
        return clazz.getMethod("newRaftServer", argClasses);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to initNewRaftServerMethod", e);
      }
    }

    private static RaftServer newRaftServer(RaftPeerId serverId, RaftGroup group, RaftStorage.StartupOption option,
        StateMachine.Registry stateMachineRegistry, ThreadGroup threadGroup, RaftProperties properties,
        Parameters parameters) throws IOException {
      try {
        return (RaftServer) NEW_RAFT_SERVER_METHOD.invoke(null,
            serverId, group, option, stateMachineRegistry, threadGroup, properties, parameters);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Failed to build " + serverId, e);
      } catch (InvocationTargetException e) {
        throw IOUtils.asIOException(e.getCause());
      }
    }

    private RaftPeerId serverId;
    private StateMachine.Registry stateMachineRegistry ;
    private RaftGroup group = null;
    private RaftStorage.StartupOption option = RaftStorage.StartupOption.FORMAT;
    private RaftProperties properties;
    private Parameters parameters;
    private ThreadGroup threadGroup;

    /** @return a {@link RaftServer} object. */
    public RaftServer build() throws IOException {
      return newRaftServer(
          serverId,
          group,
          option,
          Objects.requireNonNull(stateMachineRegistry , "Neither 'stateMachine' nor 'setStateMachineRegistry' " +
              "is initialized."),
          threadGroup,
          Objects.requireNonNull(properties, "The 'properties' field is not initialized."),
          parameters);
    }

    /** Set the server ID. */
    public Builder setServerId(RaftPeerId serverId) {
      this.serverId = serverId;
      return this;
    }

    /** Set the {@link StateMachine} of the server. */
    public Builder setStateMachine(StateMachine stateMachine) {
      return setStateMachineRegistry(gid -> stateMachine);
    }

    /** Set the {@link StateMachine.Registry} of the server. */
    public Builder setStateMachineRegistry(StateMachine.Registry stateMachineRegistry ) {
      this.stateMachineRegistry = stateMachineRegistry ;
      return this;
    }

    /** Set all the peers (including the server being built) in the Raft cluster. */
    public Builder setGroup(RaftGroup group) {
      this.group = group;
      return this;
    }

    /** Set the startup option for the group. */
    public Builder setOption(RaftStorage.StartupOption option) {
      this.option = option;
      return this;
    }

    /** Set {@link RaftProperties}. */
    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }

    /** Set {@link Parameters}. */
    public Builder setParameters(Parameters parameters) {
      this.parameters = parameters;
      return this;
    }

    /**
     * Set {@link ThreadGroup} so the application can control RaftServer threads consistently with the application.
     * For example, configure {@link ThreadGroup#uncaughtException(Thread, Throwable)} for the whole thread group.
     * If not set, the new thread will be put into the thread group of the caller thread.
     */
    public Builder setThreadGroup(ThreadGroup threadGroup) {
      this.threadGroup = threadGroup;
      return this;
    }
  }
}
