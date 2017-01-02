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
package org.apache.raft.grpc;

import com.google.common.base.Preconditions;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.grpc.client.RaftClientSenderWithGrpc;
import org.apache.raft.grpc.server.PipelinedLogAppenderFactory;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.impl.BlockRequestHandlingInjection;
import org.apache.raft.server.impl.DelayLocalExecutionInjection;
import org.apache.raft.server.impl.LogAppenderFactory;
import org.apache.raft.server.impl.RaftServerImpl;
import org.apache.raft.util.NetUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY;

public class MiniRaftClusterWithGRpc extends MiniRaftCluster.RpcBase {
  public static final Factory<MiniRaftClusterWithGRpc> FACTORY
      = new Factory<MiniRaftClusterWithGRpc>() {
    @Override
    public MiniRaftClusterWithGRpc newCluster(
        String[] ids, RaftProperties prop, boolean formatted) throws IOException {
      return new MiniRaftClusterWithGRpc(ids, prop, formatted);
    }
  };

  public static final DelayLocalExecutionInjection sendServerRequestInjection =
      new DelayLocalExecutionInjection(RaftGRpcService.GRPC_SEND_SERVER_REQUEST);

  public MiniRaftClusterWithGRpc(int numServers, RaftProperties properties)
      throws IOException {
    this(generateIds(numServers, 0), properties, true);
  }

  public MiniRaftClusterWithGRpc(String[] ids, RaftProperties properties,
      boolean formatted) throws IOException {
    super(ids, getPropForGrpc(properties), formatted);
    init(initRpcServices(getServers(), properties));
  }

  private static RaftProperties getPropForGrpc(RaftProperties prop) {
    RaftProperties newProp = new RaftProperties(prop);
    newProp.setClass(RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY,
        PipelinedLogAppenderFactory.class, LogAppenderFactory.class);
    return newProp;
  }

  private static Map<RaftPeer, RaftGRpcService> initRpcServices(
      Collection<RaftServerImpl> servers, RaftProperties prop) throws IOException {
    final Map<RaftPeer, RaftGRpcService> peerRpcs = new HashMap<>();

    for (RaftServerImpl s : servers) {
      final RaftGRpcService rpc = new RaftGRpcService(s, prop);
      peerRpcs.put(new RaftPeer(s.getId(), rpc.getInetSocketAddress()), rpc);
    }
    return peerRpcs;
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return new RaftClientSenderWithGrpc(getPeers());
  }

  @Override
  protected Collection<RaftPeer> addNewPeers(Collection<RaftPeer> newPeers,
                                             Collection<RaftServerImpl> newServers, boolean startService)
      throws IOException {
    final Map<RaftPeer, RaftGRpcService> peers = initRpcServices(newServers, properties);
    for (Map.Entry<RaftPeer, RaftGRpcService> entry : peers.entrySet()) {
      RaftServerImpl server = servers.get(entry.getKey().getId());
      server.setServerRpc(entry.getValue());
      if (!startService) {
        BlockRequestHandlingInjection.getInstance().blockReplier(server.getId());
      } else {
        server.start();
      }
    }
    return new ArrayList<>(peers.keySet());
  }

  @Override
  protected RaftServerImpl setPeerRpc(RaftPeer peer) throws IOException {
    RaftServerImpl server = servers.get(peer.getId());
    int port = NetUtils.newInetSocketAddress(peer.getAddress()).getPort();
    int oldPort = properties.getInt(RaftGrpcConfigKeys.RAFT_GRPC_SERVER_PORT_KEY,
        RaftGrpcConfigKeys.RAFT_GRPC_SERVER_PORT_DEFAULT);
    properties.setInt(RaftGrpcConfigKeys.RAFT_GRPC_SERVER_PORT_KEY, port);
    final RaftGRpcService rpc = new RaftGRpcService(server, properties);
    Preconditions.checkState(
        rpc.getInetSocketAddress().toString().contains(peer.getAddress()),
        "address in the raft conf: %s, address in rpc server: %s",
        peer.getAddress(), rpc.getInetSocketAddress().toString());
    server.setServerRpc(rpc);
    properties.setInt(RaftGrpcConfigKeys.RAFT_GRPC_SERVER_PORT_KEY, oldPort);
    return server;
  }

  @Override
  public void startServer(String id) {
    super.startServer(id);
    BlockRequestHandlingInjection.getInstance().unblockReplier(id);
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequestInjection,
        leaderId, delayMs, getMaxTimeout());
  }
}
