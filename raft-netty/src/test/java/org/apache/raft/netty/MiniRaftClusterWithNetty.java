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
package org.apache.raft.netty;

import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.netty.client.NettyClientRequestSender;
import org.apache.raft.netty.server.NettyRpcService;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.impl.DelayLocalExecutionInjection;
import org.apache.raft.server.impl.RaftConfiguration;
import org.apache.raft.server.impl.RaftServerImpl;
import org.apache.raft.util.NetUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MiniRaftClusterWithNetty extends MiniRaftCluster.RpcBase {
  public static final Factory<MiniRaftClusterWithNetty> FACTORY
      = new Factory<MiniRaftClusterWithNetty>() {
    @Override
    public MiniRaftClusterWithNetty newCluster(
        String[] ids, RaftProperties prop, boolean formatted) {
      return new MiniRaftClusterWithNetty(ids, prop, formatted);
    }
  };

  public static final DelayLocalExecutionInjection sendServerRequest
      = new DelayLocalExecutionInjection(NettyRpcService.SEND_SERVER_REQUEST);

  public MiniRaftClusterWithNetty(int numServers, RaftProperties properties) {
    this(generateIds(numServers, 0), properties, true);
  }

  public MiniRaftClusterWithNetty(
      String[] ids, RaftProperties properties, boolean formatted) {
    super(ids, properties, formatted);
    init(initRpcServices(getServers(), getConf()));
  }

  private static String getAddress(String id, RaftConfiguration conf) {
    final RaftPeer peer = conf.getPeer(id);
    if (peer != null) {
      final String address = peer.getAddress();
      if (address != null) {
        return address;
      }
    }
    return "0.0.0.0:0";
  }

  private static NettyRpcService newNettyRpcService(
      RaftServerImpl s, RaftConfiguration conf) {
    final String address = getAddress(s.getId(), conf);
    final int port = NetUtils.newInetSocketAddress(address).getPort();
    return new NettyRpcService(port, s);
  }

  private static Map<RaftPeer, NettyRpcService> initRpcServices(
      Collection<RaftServerImpl> servers, RaftConfiguration conf) {
    final Map<RaftPeer, NettyRpcService> peerRpcs = new HashMap<>();

    for (RaftServerImpl s : servers) {
      final NettyRpcService rpc = newNettyRpcService(s, conf);
      peerRpcs.put(new RaftPeer(s.getId(), rpc.getInetSocketAddress()), rpc);
    }

    return peerRpcs;
  }

  @Override
  protected RaftServerImpl setPeerRpc(RaftPeer peer) throws IOException {
    final RaftServerImpl s = servers.get(peer.getId());
    final NettyRpcService rpc = newNettyRpcService(s, conf);
    s.setServerRpc(rpc);
    return s;
  }

  @Override
  protected Collection<RaftPeer> addNewPeers(
      Collection<RaftPeer> newPeers, Collection<RaftServerImpl> newServers,
      boolean startService) throws IOException {
    return addNewPeers(initRpcServices(newServers, conf),
        newServers, startService);
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return new NettyClientRequestSender(getPeers());
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequest,
        leaderId, delayMs, getMaxTimeout());
  }
}
