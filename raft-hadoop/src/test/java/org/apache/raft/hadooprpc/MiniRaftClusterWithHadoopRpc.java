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
package org.apache.raft.hadooprpc;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.hadooprpc.client.HadoopClientRequestSender;
import org.apache.raft.hadooprpc.server.HadoopRpcService;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.impl.DelayLocalExecutionInjection;
import org.apache.raft.server.impl.RaftServerImpl;
import org.apache.raft.server.RaftServerConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MiniRaftClusterWithHadoopRpc extends MiniRaftCluster.RpcBase {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithHadoopRpc.class);

  public static final Factory<MiniRaftClusterWithHadoopRpc> FACTORY
      = new Factory<MiniRaftClusterWithHadoopRpc>() {
    @Override
    public MiniRaftClusterWithHadoopRpc newCluster(
        String[] ids, RaftProperties prop, boolean formatted) throws IOException {
      final Configuration conf = new Configuration();
      conf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, "0.0.0.0:0");
      return new MiniRaftClusterWithHadoopRpc(ids, prop, conf, formatted);
    }
  };

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

    init(initRpcServices(getServers(), hadoopConf));
  }

  private static Map<RaftPeer, HadoopRpcService> initRpcServices(
      Collection<RaftServerImpl> servers, Configuration hadoopConf) throws IOException {
    final Map<RaftPeer, HadoopRpcService> peerRpcs = new HashMap<>();

    for(RaftServerImpl s : servers) {
      final HadoopRpcService rpc = new HadoopRpcService(s, hadoopConf);
      peerRpcs.put(new RaftPeer(s.getId(), rpc.getInetSocketAddress()), rpc);
    }
    return peerRpcs;
  }

  @Override
  protected RaftServerImpl setPeerRpc(RaftPeer peer) throws IOException {
    Configuration hconf = new Configuration(hadoopConf);
    hconf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, peer.getAddress());

    RaftServerImpl server = servers.get(peer.getId());
    final HadoopRpcService rpc = new HadoopRpcService(server, hconf);
    Preconditions.checkState(
        rpc.getInetSocketAddress().toString().contains(peer.getAddress()),
        "address in the raft conf: %s, address in rpc server: %s",
        peer.getAddress(), rpc.getInetSocketAddress().toString());
    server.setServerRpc(rpc);
    return server;
  }

  @Override
  public Collection<RaftPeer> addNewPeers(Collection<RaftPeer> newPeers,
                                          Collection<RaftServerImpl> newServers, boolean startService)
      throws IOException {
    return addNewPeers(initRpcServices(newServers, hadoopConf),
        newServers, startService);
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return new HadoopClientRequestSender(getPeers(), hadoopConf);
  }

  @Override
  public void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequest,
        leaderId, delayMs, getMaxTimeout());
  }
}
