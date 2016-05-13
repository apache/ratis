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

public class MiniRaftCluster {
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

  MiniRaftCluster(int numServers) {
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

  public RaftPeer[] addNewPeers(int number) {
    List<RaftPeer> newPeers = new ArrayList<>(number);
    final int oldSize = conf.getPeers().size();
    for (int i = oldSize; i < oldSize + number; i++) {
      newPeers.add(new RaftPeer("s" + i));
    }

    serverRpc.addPeers(newPeers);
    client2serverRpc.addPeers(newPeers);

    // create and add new RaftServers. Still assign old conf to new peers.
    for (RaftPeer p : newPeers) {
      RaftServer newServer = new RaftServer(p.getId(), conf, serverRpc,
          client2serverRpc);
      servers.put(p.getId(), newServer);
      newServer.start();
    }

    newPeers.addAll(conf.getPeers());
    RaftPeer[] p = newPeers.toArray(new RaftPeer[newPeers.size()]);
    conf = new RaftConfiguration(p, 0);
    return p;
  }

  /**
   * prepare the peer list when removing some peers from the conf
   */
  public RaftPeer[] removePeers(int number, boolean removeLeader) {
    Collection<RaftPeer> peers = new ArrayList<>(conf.getPeers());
    if (removeLeader) {
      peers.remove(new RaftPeer(getLeader().getId()));
    }
    List<RaftServer> followers = getFollowers();
    for (int i = 0; i < (removeLeader ? number - 1 : number); i++) {
      // remove old peers first
      peers.remove(new RaftPeer(followers.get(i).getId()));
    }
    RaftPeer[] p = peers.toArray(new RaftPeer[peers.size()]);
    conf = new RaftConfiguration(p, 0);
    return p;
  }

  void killServer(String id) {
    servers.get(id).kill();
  }

  void printServers(PrintStream out) {
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

  RaftServer getLeader() {
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

  Collection<RaftServer> getServers() {
    return servers.values();
  }

  RaftClient createClient(String clientId, String leaderId) {
    return new RaftClient(clientId, conf.getPeers(), client2serverRpc, leaderId);
  }

  public void shutdown() {
    for (RaftServer server : servers.values()) {
      server.kill();
    }
  }
}
