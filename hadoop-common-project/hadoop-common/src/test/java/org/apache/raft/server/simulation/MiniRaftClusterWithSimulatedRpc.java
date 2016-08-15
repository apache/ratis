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
package org.apache.raft.server.simulation;

import org.apache.raft.MiniRaftCluster;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.protocol.RaftServerReply;
import org.apache.raft.server.protocol.RaftServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MiniRaftClusterWithSimulatedRpc extends MiniRaftCluster {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithSimulatedRpc.class);

  private final SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply;
  private final SimulatedClientRequestReply client2serverRequestReply;

  public MiniRaftClusterWithSimulatedRpc(int numServers,
      RaftProperties properties) {
    this(numServers, properties, true);
  }

  public MiniRaftClusterWithSimulatedRpc(int numServers,
      RaftProperties properties, boolean formatted) {
    super(numServers, properties, formatted);
    final Collection<RaftPeer> peers = getConf().getPeers();
    final int simulateLatencyMs = properties.getInt(
        SimulatedRequestReply.SIMULATE_LATENCY_KEY,
        SimulatedRequestReply.SIMULATE_LATENCY_DEFAULT);
    LOG.info(SimulatedRequestReply.SIMULATE_LATENCY_KEY + " = " + simulateLatencyMs);
    serverRequestReply = new SimulatedRequestReply<>(peers, simulateLatencyMs);
    client2serverRequestReply = new SimulatedClientRequestReply(peers, simulateLatencyMs);

    setRpcServers(getServers());
  }

  private void setRpcServers(Collection<RaftServer> newServers) {
    newServers.forEach(s -> s.setServerRpc(
        new SimulatedServerRpc(s, serverRequestReply, client2serverRequestReply)
    ));
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
    serverRequestReply.getQueue(leaderId).blockSendRequestTo.set(block);

    // set delay takeRequest for the other queues
    getServers().stream().filter(s -> !s.getId().equals(leaderId))
        .map(s -> serverRequestReply.getQueue(s.getId()))
        .forEach(q -> q.delayTakeRequestTo.set(delayMs));

    final long sleepMs = 3 * RaftServerConstants.ELECTION_TIMEOUT_MAX_MS / 2;
    Thread.sleep(sleepMs);
  }

  @Override
  public void setBlockRequestsFrom(String src, boolean block) {
    serverRequestReply.getQueue(src).blockTakeRequestFrom.set(block);
  }
}
