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
package org.apache.hadoop.raft.hadoopRpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.raft.MiniRaftCluster;
import org.apache.hadoop.raft.client.RaftClientRequestSender;
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MiniRaftClusterWithHadoopRpc extends MiniRaftCluster {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithHadoopRpc.class);

  private final Configuration conf;

  public MiniRaftClusterWithHadoopRpc(int numServers, RaftProperties properties,
                                      Configuration conf) throws IOException {
    super(numServers, properties);
    this.conf = conf;
    setServers(getServers());
  }

  private void setServers(Collection<RaftServer> servers) throws IOException {
    final List<RaftPeer> peers = new ArrayList<>();
    final List<HadoopRpcService> rpcServices = new ArrayList<>();

    for(RaftServer s : servers) {
      final HadoopRpcService rpc = new HadoopRpcService(s, conf);
      rpcServices.add(rpc);
      s.setServerRpc(rpc);
      peers.add(new RaftPeer(s.getId(), rpc.getIpcServerAddress()));
    }

    LOG.info("peers = " + peers);

    for(HadoopRpcService rpc : rpcServices) {
      rpc.addPeers(peers, conf);
    }
  }

  @Override
  public void addNewPeers(Collection<RaftPeer> newPeers,
                          Collection<RaftServer> newServers) throws IOException {
    setServers(newServers);
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    // TODO
    return null;
  }
}
