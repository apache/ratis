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
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.netty.server.NettyRpcService;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.RaftServer;
import org.apache.raft.util.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MiniRaftClusterWithNetty extends MiniRaftCluster {
    static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithNetty.class);

  public MiniRaftClusterWithNetty(int numServers, RaftProperties properties)
      throws IOException {
    this(generateIds(numServers, 0), properties, true);
  }

  public MiniRaftClusterWithNetty(
      String[] ids, RaftProperties properties, boolean formatted) {
    super(ids, properties, formatted);
    init(initRpcServices(getServers(), getConf()));
  }

  private static String getAddress(String id, RaftConfiguration conf) {
    final String address = conf.getPeer(id).getAddress();
    return address != null? address: "0.0.0.0:0";
  }

  private static Map<RaftPeer, NettyRpcService> initRpcServices(
      Collection<RaftServer> servers, RaftConfiguration conf) {
    final Map<RaftPeer, NettyRpcService> peerRpcs = new HashMap<>();

    for (RaftServer s : servers) {
      final String address = getAddress(s.getId(), conf);
      final int port = RaftUtils.newInetSocketAddress(address).getPort();
      final NettyRpcService rpc = new NettyRpcService(port, s);
      peerRpcs.put(new RaftPeer(s.getId(), rpc.getInetSocketAddress()), rpc);
    }

    return peerRpcs;
  }

  @Override
  protected Collection<RaftPeer> addNewPeers(
      Collection<RaftPeer> newPeers, Collection<RaftServer> newServers,
      boolean startService) throws IOException {
    return addNewPeers(initRpcServices(newServers, conf),
        newServers, startService);
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return null;
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {

  }

  @Override
  public void setBlockRequestsFrom(String src, boolean block) {

  }

  @Override
  public void delaySendingRequests(String senderId, int delayMs) {

  }
}
