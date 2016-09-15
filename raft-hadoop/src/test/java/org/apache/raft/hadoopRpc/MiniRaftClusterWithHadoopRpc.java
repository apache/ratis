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
package org.apache.raft.hadoopRpc;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.hadoopRpc.client.HadoopClientRequestSender;
import org.apache.raft.hadoopRpc.server.HadoopRpcService;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.DelayLocalExecutionInjection;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.BlockRequestHandlingInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MiniRaftClusterWithHadoopRpc extends MiniRaftCluster {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithHadoopRpc.class);
  public static final DelayLocalExecutionInjection sendServerRequest =
      new DelayLocalExecutionInjection(HadoopRpcService.SEND_SERVER_REQUEST);

  private final Configuration hadoopConf;

  public MiniRaftClusterWithHadoopRpc(int numServers, RaftProperties properties,
      Configuration conf) throws IOException {
    this(generateIds(numServers, 0), properties, conf, true);
  }

  public MiniRaftClusterWithHadoopRpc(String[] ids, RaftProperties properties,
      Configuration hadoopConf, boolean formatted) throws IOException {
    super(ids, properties, formatted);
    this.hadoopConf = hadoopConf;

    Collection<RaftServer> s = getServers();
    Map<RaftPeer, HadoopRpcService> peers = initRpcServices(s);

    conf = new RaftConfiguration(
        peers.keySet().toArray(new RaftPeer[peers.size()]),
        RaftServerConstants.INVALID_LOG_INDEX);
    for (Map.Entry<RaftPeer, HadoopRpcService> entry : peers.entrySet()) {
      RaftServer server = servers.get(entry.getKey().getId());
      server.setInitialConf(conf);
      server.setServerRpc(entry.getValue());
    }
  }

  private Map<RaftPeer, HadoopRpcService> initRpcServices(
      Collection<RaftServer> servers) throws IOException {
    final Map<RaftPeer, HadoopRpcService> peerRpcs = new HashMap<>();

    for(RaftServer s : servers) {
      final HadoopRpcService rpc = new HadoopRpcService(s, hadoopConf);
      peerRpcs.put(new RaftPeer(s.getId(), rpc.getInetSocketAddress()), rpc);
    }

    LOG.info("peers = " + peerRpcs.keySet());
    return peerRpcs;
  }

  private void setPeerRpc(RaftPeer peer) throws IOException {
    Configuration hconf = new Configuration(hadoopConf);
    hconf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, peer.getAddress());

    RaftServer server = servers.get(peer.getId());
    final HadoopRpcService rpc = new HadoopRpcService(server, hconf);
    Preconditions.checkState(
        rpc.getInetSocketAddress().toString().contains(peer.getAddress()),
        "address in the raft conf: %s, address in rpc server: %s",
        peer.getAddress(), rpc.getInetSocketAddress().toString());
    server.setServerRpc(rpc);
  }

  @Override
  public void restart(boolean format) throws IOException {
    super.restart(format);

    for (RaftPeer peer : conf.getPeers()) {
      setPeerRpc(peer);
    }
    start();
  }

  @Override
  public void restartServer(String id, boolean format) throws IOException {
    super.restartServer(id, format);
    setPeerRpc(conf.getPeer(id));
    getServer(id).start();
  }

  @Override
  public Collection<RaftPeer> addNewPeers(Collection<RaftPeer> newPeers,
      Collection<RaftServer> newServers) throws IOException {
    Map<RaftPeer, HadoopRpcService> peers = initRpcServices(newServers);
    for (Map.Entry<RaftPeer, HadoopRpcService> entry : peers.entrySet()) {
      RaftServer server = servers.get(entry.getKey().getId());
      server.setServerRpc(entry.getValue());
    }
    return new ArrayList<>(peers.keySet());
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return new HadoopClientRequestSender(getPeers(), hadoopConf);
  }

  @Override
  public void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    // block reqeusts sent to leader if delayMs > 0
    final boolean block = delayMs > 0;
    LOG.debug("{} requests sent to leader {} and set {}ms delay for the others",
        block? "Block": "Unblock", leaderId, delayMs);
    if (block) {
      BlockRequestHandlingInjection.getInstance().blockReplier(leaderId);
    } else {
      BlockRequestHandlingInjection.getInstance().unblockReplier(leaderId);
    }

    // delay RaftServerRequest for other servers
    getServers().stream().filter(s -> !s.getId().equals(leaderId))
        .forEach(s -> {
          if (block) {
            sendServerRequest.setDelayMs(s.getId(), delayMs);
          } else {
            sendServerRequest.removeDelay(s.getId());
          }
        });

    final long sleepMs = 3 * getMaxTimeout();
    Thread.sleep(sleepMs);
  }

  @Override
  public void setBlockRequestsFrom(String src, boolean block) {
    if (block) {
      BlockRequestHandlingInjection.getInstance().blockRequestor(src);
    } else {
      BlockRequestHandlingInjection.getInstance().unblockRequestor(src);
    }
  }

  @Override
  public void delaySendingRequests(String senderId, int delayMs) {
    sendServerRequest.setDelayMs(senderId, delayMs);
  }
}
