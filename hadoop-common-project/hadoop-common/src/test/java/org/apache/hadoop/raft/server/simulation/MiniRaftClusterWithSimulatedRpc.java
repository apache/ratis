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
package org.apache.hadoop.raft.server.simulation;

import org.apache.hadoop.raft.MiniRaftCluster;
import org.apache.hadoop.raft.client.RaftClientRequestSender;
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.protocol.RaftServerReply;
import org.apache.hadoop.raft.server.protocol.RaftServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MiniRaftClusterWithSimulatedRpc extends MiniRaftCluster {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithSimulatedRpc.class);

  private final SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply;
  private final SimulatedClientRequestReply client2serverRequestReply;

  public MiniRaftClusterWithSimulatedRpc(int numServers, RaftProperties properties) {
    super(numServers, properties);
    final Collection<RaftPeer> peers = getConf().getPeers();
    serverRequestReply = new SimulatedRequestReply<>(peers);
    client2serverRequestReply = new SimulatedClientRequestReply(peers);

    setRpcServers(getServers());
  }

  private void setRpcServers(Collection<RaftServer> newServers) {
    newServers.forEach(s -> s.setServerRpc(
        new SimulatedServerRpc(s, serverRequestReply, client2serverRequestReply)
    ));
  }

  public SimulatedRequestReply<RaftServerRequest, RaftServerReply>
      getServerRequestReply() {
    return serverRequestReply;
  }

  @Override
  public void addNewPeers(Collection<RaftPeer> newPeers,
                          Collection<RaftServer> newServers) {
    serverRequestReply.addPeers(newPeers);
    client2serverRequestReply.addPeers(newPeers);

    setRpcServers(newServers);
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return client2serverRequestReply;
  }

  @Override
  public void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    // block leader sendRequest if delayMs > 0
    final boolean block = delayMs > 0;
    LOG.debug("{} leader queue {} and set {}ms delay for the other queues",
        block? "Block": "Unblock", leaderId, delayMs);
    serverRequestReply.getQueue(leaderId).blockSendRequest.setBlocked(block);

    // set delay takeRequest for the other queues
    getServers().stream().filter(s -> !s.getId().equals(leaderId))
        .map(s -> serverRequestReply.getQueue(s.getId()))
        .forEach(q -> q.delayTakeRequest.setDelayMs(delayMs));

    final long sleepMs = 3 * RaftConstants.ELECTION_TIMEOUT_MAX_MS / 2;
    Thread.sleep(sleepMs);
  }
}
