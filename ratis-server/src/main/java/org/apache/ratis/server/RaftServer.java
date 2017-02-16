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

import com.google.common.base.Preconditions;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientAsynchronousProtocol;
import org.apache.ratis.protocol.RaftClientProtocol;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.ServerImplUtils;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.statemachine.StateMachine;

import java.io.Closeable;
import java.io.IOException;

/** Raft server interface */
public interface RaftServer extends Closeable, RaftServerProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol {
  /** @return the server ID. */
  RaftPeerId getId();

  /** Set server RPC service. */
  void setServerRpc(RaftServerRpc serverRpc);

  /** Start this server. */
  void start();

  /**
   * Returns the StateMachine instance.
   * @return the StateMachine instance.
   */
  StateMachine getStateMachine();

  /** @return a {@link Builder}. */
  static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link RaftServer} objects. */
  class Builder {
    private RaftPeerId serverId;
    private StateMachine stateMachine;
    private Iterable<RaftPeer> peers;
    private RaftProperties properties;

    /** @return a {@link RaftServer} object. */
    public RaftServer build() throws IOException {
      Preconditions.checkNotNull(stateMachine);
      Preconditions.checkNotNull(peers);
      Preconditions.checkNotNull(properties);
      Preconditions.checkNotNull(serverId);

      return ServerImplUtils.newRaftServer(serverId, stateMachine, peers,
          properties);
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
    public Builder setPeers(Iterable<RaftPeer> peers) {
      this.peers = peers;
      return this;
    }

    /** Set {@link RaftProperties}. */
    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }
  }
}
