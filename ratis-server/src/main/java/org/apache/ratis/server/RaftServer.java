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
package org.apache.ratis.server;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.impl.ServerFactory;
import org.apache.ratis.server.impl.ServerImplUtils;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.statemachine.StateMachine;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/** Raft server interface */
public interface RaftServer extends Closeable, RpcType.Get,
    RaftServerProtocol, RaftServerAsynchronousProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol,
    AdminProtocol, AdminAsynchronousProtocol {

  /** @return the server ID. */
  RaftPeerId getId();

  /** @return the group IDs the server is part of. */
  Iterable<RaftGroupId> getGroupIds() throws IOException;

  /** @return the server properties. */
  RaftProperties getProperties();

  /** @return the factory for creating server components. */
  ServerFactory getFactory();

  /** Start this server. */
  void start();

  /** @return a {@link Builder}. */
  static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link RaftServer} objects. */
  class Builder {
    private RaftPeerId serverId;
    private StateMachine stateMachine;
    private RaftGroup group = RaftGroup.emptyGroup();
    private RaftProperties properties;
    private Parameters parameters;

    /** @return a {@link RaftServer} object. */
    public RaftServer build() throws IOException {
      return ServerImplUtils.newRaftServer(
          serverId,
          group,
          Objects.requireNonNull(stateMachine, "The 'stateMachine' is not initialized."),
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
      this.stateMachine = stateMachine;
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
