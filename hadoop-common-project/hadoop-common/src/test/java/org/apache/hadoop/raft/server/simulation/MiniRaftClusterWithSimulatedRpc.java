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
import org.apache.hadoop.raft.server.simulation.SimulatedClientRequestReply;
import org.apache.hadoop.raft.server.simulation.SimulatedRequestReply;
import org.apache.hadoop.raft.server.simulation.SimulatedServerRpc;
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

  private void setTakeRequestDelayMs(String leaderId, int delayMs) {
    getServers().stream().filter(s -> !s.getId().equals(leaderId)).forEach(
        s -> serverRequestReply.setTakeRequestDelayMs(s.getId(), delayMs));
  }

  @Override
  public boolean tryEnforceLeader(String leaderId) throws InterruptedException {
    final RaftServer leader = getLeader();
    if (leader != null && leader.getId().equals(leaderId)) {
      return true;
    }
    // Blocking all other server's RPC read process to make sure a read takes at
    // least ELECTION_TIMEOUT_MIN. In this way when the target leader request a
    // vote, all non-leader servers can grant the vote.
    LOG.debug("begin blocking queue for target leader");
    setTakeRequestDelayMs(leaderId, RaftConstants.ELECTION_TIMEOUT_MIN_MS);
    // Disable the target leader server RPC so that it can request a vote.
    serverRequestReply.setIsOpenForMessage(leaderId, false);
    LOG.debug("Closed queue for target leader");

    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 100);
    LOG.debug("target leader should have became candidate. open queue");

    // Reopen queues so that the vote can make progress.
    setTakeRequestDelayMs(leaderId, 0);
    serverRequestReply.setIsOpenForMessage(leaderId, true);
    // Wait for a quiescence.
    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 100);

    return getServer(leaderId).isLeader();
  }
}
