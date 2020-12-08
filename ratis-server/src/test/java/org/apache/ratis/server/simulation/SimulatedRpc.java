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
package org.apache.ratis.server.simulation;

import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.ServerFactory;
import org.apache.ratis.util.JavaUtils;

import java.util.Objects;

class SimulatedRpc implements RpcType {
  static final SimulatedRpc INSTANCE = new SimulatedRpc();

  @Override
  public String name() {
    return getClass().getName();
  }

  @Override
  public Factory newFactory(Parameters parameters) {
    return new Factory(parameters);
  }

  static class Factory implements ServerFactory, ClientFactory {
    static String SERVER_REQUEST_REPLY_KEY = "raft.simulated.serverRequestReply";
    static String CLIENT_TO_SERVER_REQUEST_REPLY_KEY = "raft.simulated.client2serverRequestReply";

    static Parameters newRaftParameters(
        SimulatedRequestReply<RaftServerRequest, RaftServerReply> server,
        SimulatedClientRpc client2server) {
      final Parameters p = new Parameters();
      p.put(SERVER_REQUEST_REPLY_KEY, server, SimulatedRequestReply.class);
      p.put(CLIENT_TO_SERVER_REQUEST_REPLY_KEY, client2server, SimulatedClientRpc.class);
      return p;
    }

    private final SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply;
    private final SimulatedClientRpc client2serverRequestReply;

    Factory(Parameters parameters) {
      serverRequestReply = JavaUtils.cast(parameters.getNonNull(SERVER_REQUEST_REPLY_KEY, SimulatedRequestReply.class));
      client2serverRequestReply = parameters.getNonNull(
          CLIENT_TO_SERVER_REQUEST_REPLY_KEY, SimulatedClientRpc.class);
    }

    @Override
    public SimulatedServerRpc newRaftServerRpc(RaftServer server) {
      return new SimulatedServerRpc(server,
          Objects.requireNonNull(serverRequestReply),
          Objects.requireNonNull(client2serverRequestReply));
    }

    @Override
    public SimulatedClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties) {
      return Objects.requireNonNull(client2serverRequestReply);
    }

    @Override
    public RpcType getRpcType() {
      return INSTANCE;
    }
  }
}
