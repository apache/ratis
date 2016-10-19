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

import org.apache.raft.MiniRaftCluster;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.grpc.server.RaftGRpcService;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MiniRaftClusterWithGRpc extends MiniRaftCluster {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithGRpc.class);

  public MiniRaftClusterWithGRpc(int numServers, RaftProperties properties)
      throws IOException {
    this(generateIds(numServers, 0), properties, true);
  }

  public MiniRaftClusterWithGRpc(String[] ids, RaftProperties properties,
      boolean formatted) throws IOException {
    super(ids, properties, formatted);
    Collection<RaftServer> s = getServers();
    Map<RaftPeer, RaftGRpcService> peers = initRpcServices(properties, s);

    conf = new RaftConfiguration(
        peers.keySet().toArray(new RaftPeer[peers.size()]),
        RaftServerConstants.INVALID_LOG_INDEX);
    for (Map.Entry<RaftPeer, RaftGRpcService> entry : peers.entrySet()) {
      RaftServer server = servers.get(entry.getKey().getId());
      server.setInitialConf(conf);
      server.setServerRpc(entry.getValue());
    }
  }

  private Map<RaftPeer, RaftGRpcService> initRpcServices(RaftProperties prop,
      Collection<RaftServer> servers) throws IOException {
    final Map<RaftPeer, RaftGRpcService> peerRpcs = new HashMap<>();

    for (RaftServer s : servers) {
      final RaftGRpcService rpc = new RaftGRpcService(s, prop);
      peerRpcs.put(new RaftPeer(s.getId(), rpc.getInetSocketAddress()), rpc);
    }

    LOG.info("peers = " + peerRpcs.keySet());
    return peerRpcs;
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return null;
  }

  @Override
  protected Collection<RaftPeer> addNewPeers(Collection<RaftPeer> newPeers,
      Collection<RaftServer> newServers) throws IOException {
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
