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
package org.apache.raft;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.ExitUtil;
import org.apache.raft.client.RaftClient;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.DelayInjection;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.storage.MemoryRaftLog;
import org.apache.raft.server.storage.RaftLog;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class MiniRaftCluster {
  public static final Logger LOG = LoggerFactory.getLogger(MiniRaftCluster.class);
  public static final DelayInjection logSyncDelay = new DelayInjection(RaftLog.LOG_SYNC);

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

  public static RaftConfiguration initConfiguration(String[] ids) {
    RaftPeer[] peers = new RaftPeer[ids.length];
    for (int i = 0; i < ids.length; i++) {
      peers[i] = new RaftPeer(ids[i]);
    }
    return RaftConfiguration.composeConf(peers, RaftServerConstants.INVALID_LOG_INDEX);
  }

  private static String getBaseDirectory() {
    return System.getProperty("test.build.data", "target/test/data") + "/raft/";
  }

  private static void formatDir(String dirStr) {
    final File serverDir = new File(dirStr);
    FileUtil.fullyDelete(serverDir);
  }

  public static String[] generateIds(int numServers, int base) {
    String[] ids = new String[numServers];
    for (int i = 0; i < numServers; i++) {
      ids[i] = "s" + (i + base);
    }
    return ids;
  }

  protected RaftConfiguration conf;
  protected final RaftProperties properties;
  private final String testBaseDir;
  protected final Map<String, RaftServer> servers =
      Collections.synchronizedMap(new LinkedHashMap<>());

  public MiniRaftCluster(String[] ids, RaftProperties properties,
      boolean formatted) {
    this.conf = initConfiguration(ids);
    this.properties = properties;
    this.testBaseDir = getBaseDirectory();

    for (RaftPeer p : conf.getPeers()) {
      final RaftServer s = newRaftServer(p.getId(), conf, formatted);
      servers.put(p.getId(), s);
    }

    ExitUtil.disableSystemExit();
  }

  public void start() {
    servers.values().forEach(RaftServer::start);
  }

  public void restart(boolean format) throws IOException {
    servers.values().stream().filter(RaftServer::isRunning)
        .forEach(RaftServer::kill);
    List<String> idList = new ArrayList<>(servers.keySet());
    for (String id : idList) {
      servers.remove(id);
      servers.put(id, newRaftServer(id, conf, format));
    }
  }

  public RaftConfiguration getConf() {
    return conf;
  }

  private RaftServer newRaftServer(String id, RaftConfiguration conf,
      boolean format) {
    final RaftServer s;
    try {
      final String dirStr = testBaseDir + id;
      if (format) {
        formatDir(dirStr);
      }
      properties.set(RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_KEY, dirStr);
      s = new RaftServer(id, conf, properties);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return s;
  }

  public abstract RaftClientRequestSender getRaftClientRequestSender();

  protected abstract Collection<RaftPeer> addNewPeers(
      Collection<RaftPeer> newPeers, Collection<RaftServer> newServers)
      throws IOException;

  public PeerChanges addNewPeers(int number, boolean startNewPeer)
      throws IOException {
    return addNewPeers(generateIds(number, servers.size()), startNewPeer);
  }

  public PeerChanges addNewPeers(String[] ids,
      boolean startNewPeer) throws IOException {
    LOG.info("Add new peers {}", Arrays.asList(ids));
    Collection<RaftPeer> newPeers = new ArrayList<>(ids.length);
    for (String id : ids) {
      newPeers.add(new RaftPeer(id));
    }

    // create and add new RaftServers
    final List<RaftServer> newServers = new ArrayList<>(ids.length);
    for (RaftPeer p : newPeers) {
      RaftServer newServer = newRaftServer(p.getId(), conf, true);
      Preconditions.checkArgument(!servers.containsKey(p.getId()));
      servers.put(p.getId(), newServer);
      newServers.add(newServer);
    }

    // for hadoop-rpc-enabled peer, we assign inetsocketaddress here
    newPeers = addNewPeers(newPeers, newServers);

    if (startNewPeer) {
      newServers.forEach(RaftServer::start);
    }

    final RaftPeer[] np = newPeers.toArray(new RaftPeer[newPeers.size()]);
    newPeers.addAll(conf.getPeers());
    RaftPeer[] p = newPeers.toArray(new RaftPeer[newPeers.size()]);
    conf = RaftConfiguration.composeConf(p, 0);
    return new PeerChanges(p, np, new RaftPeer[0]);
  }

  public void startServer(String id) {
    RaftServer server = servers.get(id);
    assert server != null;
    server.start();
  }

  private RaftPeer getPeer(RaftServer s) {
    return new RaftPeer(s.getId(), s.getServerRpc().getInetSocketAddress());
  }

  /**
   * prepare the peer list when removing some peers from the conf
   */
  public PeerChanges removePeers(int number, boolean removeLeader,
      Collection<RaftPeer> excluded) {
    Collection<RaftPeer> peers = new ArrayList<>(conf.getPeers());
    List<RaftPeer> removedPeers = new ArrayList<>(number);
    if (removeLeader) {
      final RaftPeer leader = getPeer(getLeader());
      assert !excluded.contains(leader);
      peers.remove(leader);
      removedPeers.add(leader);
    }
    List<RaftServer> followers = getFollowers();
    for (int i = 0, removed = 0; i < followers.size() &&
        removed < (removeLeader ? number - 1 : number); i++) {
      RaftPeer toRemove = getPeer(followers.get(i));
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

  public void killServer(String id) {
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

  public String printAllLogs() {
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
    final List<RaftServer> leaders = new ArrayList<>();
    servers.values().stream()
        .filter(s -> s.isRunning() && s.isLeader())
        .forEach(s -> {
      if (leaders.isEmpty()) {
        leaders.add(s);
      } else {
        final long leaderTerm = leaders.get(0).getState().getCurrentTerm();
        final long term = s.getState().getCurrentTerm();
        if (term >= leaderTerm) {
          if (term > leaderTerm) {
            leaders.clear();
          }
          leaders.add(s);
        }
      }
    });
    if (leaders.isEmpty()) {
      return null;
    } else if (leaders.size() != 1) {
      Assert.fail(printServers() + leaders.toString()
          + "leaders.size() = " + leaders.size() + " != 1");
    }
    return leaders.get(0);
  }

  public boolean isLeader(String leaderId) throws InterruptedException {
    final RaftServer leader = getLeader();
    return leader != null && leader.getId().equals(leaderId);
  }

  public List<RaftServer> getFollowers() {
    return servers.values().stream()
        .filter(s -> s.isRunning() && s.isFollower())
        .collect(Collectors.toList());
  }

  public Collection<RaftServer> getServers() {
    return servers.values();
  }

  public RaftServer getServer(String id) {
    return servers.get(id);
  }

  public Collection<RaftPeer> getPeers() {
    return getServers().stream().map(s ->
        new RaftPeer(s.getId(), s.getServerRpc().getInetSocketAddress()))
        .collect(Collectors.toList());
  }

  public RaftClient createClient(String clientId, String leaderId) {
    return new RaftClient(clientId, conf.getPeers(),
        getRaftClientRequestSender(), leaderId);
  }

  public void shutdown() {
    servers.values().stream().filter(RaftServer::isRunning)
        .forEach(RaftServer::kill);

    if (ExitUtil.terminateCalled()) {
      LOG.error("Test resulted in an unexpected exit",
          ExitUtil.getFirstExitException());
      throw new AssertionError("Test resulted in an unexpected exit");
    }
  }

  public abstract void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException;

  /**
   * Try to enforce the leader of the cluster.
   * @param leaderId ID of the targeted leader server.
   * @return true if server has been successfully enforced to the leader, false
   *         otherwise.
   */
  public boolean tryEnforceLeader(String leaderId) throws InterruptedException {
    // do nothing and see if the given id is already a leader.
    if (isLeader(leaderId)) {
      return true;
    }

    // Blocking all other server's RPC read process to make sure a read takes at
    // least ELECTION_TIMEOUT_MIN. In this way when the target leader request a
    // vote, all non-leader servers can grant the vote.
    // Disable the target leader server RPC so that it can request a vote.
    blockQueueAndSetDelay(leaderId, RaftServerConstants.ELECTION_TIMEOUT_MIN_MS);

    // Reopen queues so that the vote can make progress.
    blockQueueAndSetDelay(leaderId, 0);

    return isLeader(leaderId);
  }

  /** Block/unblock the requests sent from the given source. */
  public abstract void setBlockRequestsFrom(String src, boolean block);
}
