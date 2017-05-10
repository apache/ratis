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
package org.apache.ratis.server.impl;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class RaftServerProxy implements RaftServer {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerProxy.class);

  private final RaftPeerId id;
  private final RaftServerImpl impl;
  private final StateMachine stateMachine;
  private final RaftProperties properties;

  private final RaftServerRpc serverRpc;
  private final ServerFactory factory;

  RaftServerProxy(RaftPeerId id, StateMachine stateMachine,
                  RaftConfiguration raftConf, RaftProperties properties, Parameters parameters)
      throws IOException {
    this.id = id;
    this.properties = properties;
    this.stateMachine = stateMachine;

    final RpcType rpcType = RaftConfigKeys.Rpc.type(properties);
    this.factory = ServerFactory.cast(rpcType.newFactory(properties, parameters));
    this.impl = new RaftServerImpl(id, this, raftConf, properties);
    this.serverRpc = initRaftServerRpc(factory, this, raftConf);
  }

  private static RaftServerRpc initRaftServerRpc(
      ServerFactory factory, RaftServer server, RaftConfiguration raftConf) {
    final RaftServerRpc rpc = factory.newRaftServerRpc(server);
    // add peers into rpc service
    if (raftConf != null) {
      rpc.addPeers(raftConf.getPeers());
    }
    return rpc;
  }

  @Override
  public RpcType getRpcType() {
    return getFactory().getRpcType();
  }

  @Override
  public ServerFactory getFactory() {
    return factory;
  }

  @Override
  public StateMachine getStateMachine() {
    return stateMachine;
  }

  @Override
  public RaftProperties getProperties() {
    return properties;
  }

  public RaftServerRpc getServerRpc() {
    return serverRpc;
  }

  public RaftServerImpl getImpl() {
    return impl;
  }

  @Override
  public void start() {
    getImpl().start();
    getServerRpc().start();
  }

  @Override
  public RaftPeerId getId() {
    return id;
  }

  @Override
  public void close() {
    getImpl().shutdown();
    try {
      getServerRpc().close();
    } catch (IOException ignored) {
      LOG.warn("Failed to close RPC server for " + getId(), ignored);
    }
  }

  @Override
  public CompletableFuture<RaftClientReply> submitClientRequestAsync(
      RaftClientRequest request) throws IOException {
    return getImpl().submitClientRequestAsync(request);
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return getImpl().submitClientRequest(request);
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    return getImpl().setConfiguration(request);
  }

  /**
   * Handle a raft configuration change request from client.
   */
  @Override
  public CompletableFuture<RaftClientReply> setConfigurationAsync(
      SetConfigurationRequest request) throws IOException {
    return getImpl().setConfigurationAsync(request);
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto r)
      throws IOException {
    return getImpl().requestVote(r);
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto r)
      throws IOException {
    return getImpl().appendEntries(r);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    return getImpl().installSnapshot(request);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":" + getId().toString();
  }
}
