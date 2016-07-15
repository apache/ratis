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
package org.apache.hadoop.raft;

import org.apache.hadoop.raft.client.RaftClient;
import org.apache.hadoop.raft.server.*;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerReply;
import org.apache.hadoop.raft.server.protocol.RaftServerRequest;
import org.apache.hadoop.raft.server.simulation.SimulatedClientRequestReply;
import org.apache.hadoop.raft.server.simulation.SimulatedServerRpc;
import org.apache.hadoop.raft.server.simulation.SimulatedRequestReply;
import org.apache.hadoop.raft.server.storage.MemoryRaftLog;
import org.apache.hadoop.raft.server.storage.RaftLog;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MiniRaftCluster {

  static final Logger LOG = LoggerFactory.getLogger(MiniRaftCluster.class);

  public static class PeerChanges {
    public final RaftPeer[] allPeersInNewConf;
    public final RaftPeer[] newPeers;
    public final RaftPeer[] removedPeers;

    public PeerChanges(RaftPeer[] all, RaftPeer[] newPeers, RaftPeer[] removed) {
      this.allPeersInNewConf = all;
      this.newPeers = newPeers;
      this.removedPeers = removed;
    }
  }

  private static RaftConfiguration initConfiguration(int num) {
    RaftPeer[] peers = new RaftPeer[num];
    for (int i = 0; i < num; i++) {
      peers[i] = new RaftPeer("s" + i);
    }
    return RaftConfiguration.composeConf(peers, 0);
  }

  RaftServer newRaftServer(String id, RaftConfiguration conf) {
    final RaftServer s = new RaftServer(id, conf);
    s.setServerRpc(new SimulatedServerRpc(s, serverRequestReply,
        client2serverRequestReply));
    return s;
  }

  private RaftConfiguration conf;
  private final SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply;
  private final SimulatedClientRequestReply client2serverRequestReply;
  private final Map<String, RaftServer> servers = new LinkedHashMap<>();

  public MiniRaftCluster(int numServers) {
    this.conf = initConfiguration(numServers);
    serverRequestReply = new SimulatedRequestReply<>(conf.getPeers());
    client2serverRequestReply = new SimulatedClientRequestReply(conf.getPeers());

    for (RaftPeer p : conf.getPeers()) {
      final RaftServer s = newRaftServer(p.getId(), conf);
      servers.put(p.getId(), s);
    }
  }

  public void start() {
    servers.values().forEach((server) -> server.start(conf));
  }

  public PeerChanges addNewPeers(int number, boolean startNewPeer) {
    List<RaftPeer> newPeers = new ArrayList<>(number);
    final int oldSize = conf.getPeers().size();
    for (int i = oldSize; i < oldSize + number; i++) {
      newPeers.add(new RaftPeer("s" + i));
    }
    final RaftPeer[] np = newPeers.toArray(new RaftPeer[newPeers.size()]);

    serverRequestReply.addPeers(newPeers);
    client2serverRequestReply.addPeers(newPeers);

    // create and add new RaftServers
    for (RaftPeer p : newPeers) {
      RaftServer newServer = newRaftServer(p.getId(), conf);
      servers.put(p.getId(), newServer);
      if (startNewPeer) {
        // start new peers as initializing state
        newServer.start(null);
      }
    }

    newPeers.addAll(conf.getPeers());
    RaftPeer[] p = newPeers.toArray(new RaftPeer[newPeers.size()]);
    conf = RaftConfiguration.composeConf(p, 0);
    return new PeerChanges(p, np, new RaftPeer[0]);
  }

  public void startServer(String id, RaftConfiguration conf) {
    RaftServer server = servers.get(id);
    assert server != null;
    server.start(conf);
  }

  /**
   * prepare the peer list when removing some peers from the conf
   */
  public PeerChanges removePeers(int number, boolean removeLeader,
      Collection<RaftPeer> excluded) {
    Collection<RaftPeer> peers = new ArrayList<>(conf.getPeers());
    List<RaftPeer> removedPeers = new ArrayList<>(number);
    if (removeLeader) {
      final RaftPeer leader = new RaftPeer(getLeader().getId());
      assert !excluded.contains(leader);
      peers.remove(leader);
      removedPeers.add(leader);
    }
    List<RaftServer> followers = getFollowers();
    for (int i = 0, removed = 0; i < followers.size() &&
        removed < (removeLeader ? number - 1 : number); i++) {
      RaftPeer toRemove = new RaftPeer(followers.get(i).getId());
      if (!excluded.contains(toRemove)) {
        peers.remove(toRemove);
        removedPeers.add(toRemove);
        removed++;
      }
    }
    RaftPeer[] p = peers.toArray(new RaftPeer[peers.size()]);
    conf = RaftConfiguration.composeConf(p, 0);
    return new PeerChanges(p, new RaftPeer[0],
        removedPeers.toArray(new RaftPeer[removedPeers.size()]));
  }

  void killServer(String id) {
    servers.get(id).kill();
  }

  public String printServers() {
    StringBuilder b = new StringBuilder("\n#servers = " + servers.size() + "\n");
    for (RaftServer s : servers.values()) {
      b.append("  ");
      b.append(s).append("\n");
    }
    return b.toString();
  }

  String printAllLogs() {
    StringBuilder b = new StringBuilder("\n#servers = " + servers.size() + "\n");
    for (RaftServer s : servers.values()) {
      b.append("  ");
      b.append(s).append("\n");

      final RaftLog log = s.getState().getLog();
      if (log instanceof MemoryRaftLog) {
        b.append("    ");
        b.append(((MemoryRaftLog) log).getEntryString());
      }
    }
    return b.toString();
  }

  public RaftServer getLeader() {
    final List<RaftServer> leaders = servers.values().stream()
        .filter(s -> s.isRunning() && s.isLeader())
        .collect(Collectors.toList());
    if (leaders.isEmpty()) {
      return null;
    } else {
      Assert.assertEquals(1, leaders.size());
      return leaders.get(0);
    }
  }

  List<RaftServer> getFollowers() {
    return servers.values().stream()
        .filter(s -> s.isRunning() && s.isFollower())
        .collect(Collectors.toList());
  }

  public Collection<RaftServer> getServers() {
    return servers.values();
  }

  public RaftClient createClient(String clientId, String leaderId) {
    return new RaftClient(clientId, conf.getPeers(), client2serverRequestReply, leaderId);
  }

  public void shutdown() {
    servers.values().stream().filter(RaftServer::isRunning)
        .forEach(RaftServer::kill);
  }

  /**
   * Try to enforce the leader of the cluster.
   * @param leaderId ID of the targeted leader server.
   * @return true if server has been successfully enforced to the leader, false
   *         otherwise.
   * @throws InterruptedException
   */
  public boolean tryEnforceLeader(String leaderId)
      throws InterruptedException {
    final RaftServer leader = getLeader();
    if (leader != null && leader.getId().equals(leaderId)) {
      return true;
    }
    // Blocking all other server's RPC read process to make sure a read takes at
    // least ELECTION_TIMEOUT_MIN. In this way when the target leader request a
    // vote, all non-leader servers can grant the vote.
    LOG.debug("begin blocking queue for target leader");
    for (Map.Entry<String, RaftServer> e : servers.entrySet()) {
      if (!e.getKey().equals(leaderId)) {
        serverRequestReply.setTakeRequestDelayMs(e.getKey(),
            RaftConstants.ELECTION_TIMEOUT_MIN_MS);
      }
    }
    // Disable the RPC queue for the target leader server so that it can request
    // a vote.
    serverRequestReply.setIsOpenForMessage(leaderId, false);
    LOG.debug("Closed queue for target leader");

    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 100);
    LOG.debug("target leader should have became candidate. open queue");

    // Reopen queues so that the vote can make progress.
    servers.entrySet().stream().filter(e -> !e.getKey().equals(leaderId))
        .forEach(e -> serverRequestReply.setTakeRequestDelayMs(e.getKey(), 0));
    serverRequestReply.setIsOpenForMessage(leaderId, true);
    // Wait for a quiescence.
    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 100);

    return servers.get(leaderId).isLeader();
  }

  SimulatedRequestReply<RaftServerRequest, RaftServerReply> getServerRequestReply() {
    return serverRequestReply;
  }

  public RaftServer getRaftServer(String id) {
    return servers.get(id);
  }
}
