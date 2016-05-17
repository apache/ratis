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
import org.apache.hadoop.raft.protocol.RaftClientReply;
import org.apache.hadoop.raft.protocol.RaftClientRequest;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerReply;
import org.apache.hadoop.raft.server.protocol.RaftServerRequest;
import org.apache.hadoop.raft.server.simulation.SimulatedRpc;
import org.junit.Assert;

import java.io.PrintStream;
import java.util.*;

import static javafx.scene.input.KeyCode.R;

public class MiniRaftCluster {
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
    return new RaftConfiguration(peers, 0);
  }

  private RaftConfiguration conf;
  private final SimulatedRpc<RaftServerRequest, RaftServerReply> serverRpc;
  private final SimulatedRpc<RaftClientRequest, RaftClientReply> client2serverRpc;
  private final Map<String, RaftServer> servers = new LinkedHashMap<>();

  public MiniRaftCluster(int numServers) {
    this.conf = initConfiguration(numServers);
    serverRpc = new SimulatedRpc<>(conf.getPeers());
    client2serverRpc = new SimulatedRpc<>(conf.getPeers());

    for (RaftPeer p : conf.getPeers()) {
      servers.put(p.getId(), new RaftServer(p.getId(), conf, serverRpc,
          client2serverRpc));
    }
  }

  public void start() {
    for (RaftServer server : servers.values()) {
      server.start();
    }
  }

  public PeerChanges addNewPeers(int number, boolean startNewPeer) {
    List<RaftPeer> newPeers = new ArrayList<>(number);
    final int oldSize = conf.getPeers().size();
    for (int i = oldSize; i < oldSize + number; i++) {
      newPeers.add(new RaftPeer("s" + i));
    }
    final RaftPeer[] np = newPeers.toArray(new RaftPeer[newPeers.size()]);

    serverRpc.addPeers(newPeers);
    client2serverRpc.addPeers(newPeers);

    // create and add new RaftServers. Still assign old conf to new peers.
    for (RaftPeer p : newPeers) {
      RaftServer newServer = new RaftServer(p.getId(), conf, serverRpc,
          client2serverRpc);
      servers.put(p.getId(), newServer);
      if (startNewPeer) {
        newServer.start();
      }
    }

    newPeers.addAll(conf.getPeers());
    RaftPeer[] p = newPeers.toArray(new RaftPeer[newPeers.size()]);
    conf = new RaftConfiguration(p, 0);
    return new PeerChanges(p, np, new RaftPeer[0]);
  }

  public void startServer(String id) {
    RaftServer server = servers.get(id);
    assert server != null;
    server.start();
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
    conf = new RaftConfiguration(p, 0);
    return new PeerChanges(p, new RaftPeer[0],
        removedPeers.toArray(new RaftPeer[removedPeers.size()]));
  }

  void killServer(String id) {
    servers.get(id).kill();
  }

  public void printServers(PrintStream out) {
    out.println("#servers = " + servers.size());
    for (RaftServer s : servers.values()) {
      out.print("  ");
      out.println(s);
    }
  }

  void printAllLogs(PrintStream out) {
    out.println("#servers = " + servers.size());
    for (RaftServer s : servers.values()) {
      out.print("  ");
      out.println(s);
      out.print("    ");
      s.getState().getLog().printEntries(out);
    }
  }

  public RaftServer getLeader() {
    final List<RaftServer> leaders = new ArrayList<>();
    for (RaftServer s : servers.values()) {
      if (s.isRunning() && s.isLeader()) {
        leaders.add(s);
      }
    }
    if (leaders.isEmpty()) {
      return null;
    } else {
      Assert.assertEquals(1, leaders.size());
      return leaders.get(0);
    }
  }

  List<RaftServer> getFollowers() {
    final List<RaftServer> followers = new ArrayList<>();
    for (RaftServer s : servers.values()) {
      if (s.isRunning() && s.isFollower()) {
        followers.add(s);
      }
    }
    return followers;
  }

  public Collection<RaftServer> getServers() {
    return servers.values();
  }

  public RaftClient createClient(String clientId, String leaderId) {
    return new RaftClient(clientId, conf.getPeers(), client2serverRpc, leaderId);
  }

  public void shutdown() {
    for (RaftServer server : servers.values()) {
      server.kill();
    }
  }
}
