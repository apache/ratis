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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.metrics.RaftServerMetrics;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/** Raft server interface */
public interface RaftServer extends Closeable, RpcType.Get,
    RaftServerProtocol, RaftServerAsynchronousProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol,
    AdminProtocol, AdminAsynchronousProtocol {
  Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  /** A division of a {@link RaftServer} for a particular {@link RaftGroup}. */
  interface Division extends Closeable {
    Logger LOG = LoggerFactory.getLogger(Division.class);

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
      return Optional.ofNullable(getGroup())
          .map(g -> g.getPeer(getId()))
          .orElseGet(() -> getRaftServer().getPeer());
    }

    /** @return the information about this division. */
    DivisionInfo getInfo();

    /** @return the {@link RaftGroup} for this division. */
    default RaftGroup getGroup() {
      return RaftGroup.valueOf(getMemberId().getGroupId(), getRaftConf().getAllPeers());
    }

    /** @return the current {@link RaftConfiguration} for this division. */
    RaftConfiguration getRaftConf();

    /** @return the {@link RaftServer} containing this division. */
    RaftServer getRaftServer();

    /** @return the {@link RaftServerMetrics} for this division. */
    RaftServerMetrics getRaftServerMetrics();

    /** @return the {@link StateMachine} for this division. */
    StateMachine getStateMachine();

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

    /** @return the internal {@link RaftClient} of this division. */
    RaftClient getRaftClient();

    @Override
    void close();
  }

  /** @return the server ID. */
  RaftPeerId getId();

  /** @return the {@link RaftPeer} for this server. */
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
      final Class<?>[] argClasses = {RaftPeerId.class, RaftGroup.class, StateMachine.Registry.class,
          RaftProperties.class, Parameters.class};
      try {
        final Class<?> clazz = ReflectionUtils.getClassByName(className);
        return clazz.getMethod("newRaftServer", argClasses);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to initNewRaftServerMethod", e);
      }
    }

    private static RaftServer newRaftServer(RaftPeerId serverId, RaftGroup group,
        StateMachine.Registry stateMachineRegistry, RaftProperties properties, Parameters parameters)
        throws IOException {
      try {
        return (RaftServer) NEW_RAFT_SERVER_METHOD.invoke(null,
            serverId, group, stateMachineRegistry, properties, parameters);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Failed to build " + serverId, e);
      } catch (InvocationTargetException e) {
        throw IOUtils.asIOException(e.getCause());
      }
    }

    private RaftPeerId serverId;
    private StateMachine.Registry stateMachineRegistry ;
    private RaftGroup group = null;
    private RaftProperties properties;
    private Parameters parameters;

    /** @return a {@link RaftServer} object. */
    public RaftServer build() throws IOException {
      return newRaftServer(
          serverId,
          group,
          Objects.requireNonNull(stateMachineRegistry , "Neither 'stateMachine' nor 'setStateMachineRegistry' " +
              "is initialized."),
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
  }
}
