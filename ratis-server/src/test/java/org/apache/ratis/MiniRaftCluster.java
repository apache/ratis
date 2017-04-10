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
package org.apache.ratis;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.server.impl.LeaderState;
import org.apache.ratis.server.impl.RaftConfiguration;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.storage.MemoryRaftLog;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.statemachine.BaseStateMachine;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReflectionUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MiniRaftCluster {
  public static final Logger LOG = LoggerFactory.getLogger(MiniRaftCluster.class);
  public static final DelayLocalExecutionInjection logSyncDelay =
      new DelayLocalExecutionInjection(RaftLog.LOG_SYNC);
  public static final DelayLocalExecutionInjection leaderPlaceHolderDelay =
      new DelayLocalExecutionInjection(LeaderState.APPEND_PLACEHOLDER);

  public static final String CLASS_NAME = MiniRaftCluster.class.getSimpleName();
  public static final String STATEMACHINE_CLASS_KEY = CLASS_NAME + ".statemachine.class";
  public static final Class<? extends StateMachine> STATEMACHINE_CLASS_DEFAULT = BaseStateMachine.class;

  public static abstract class Factory<CLUSTER extends MiniRaftCluster> {
    public abstract CLUSTER newCluster(
        String[] ids, RaftProperties prop);

    public CLUSTER newCluster(int numServer, RaftProperties prop) {
      return newCluster(generateIds(numServer, 0), prop);
    }
  }

  public static abstract class RpcBase extends MiniRaftCluster {
    public RpcBase(String[] ids, RaftProperties properties, Parameters parameters) {
      super(ids, properties, parameters);
    }

    @Override
    public void setBlockRequestsFrom(String src, boolean block) {
      RaftTestUtil.setBlockRequestsFrom(src, block);
    }

    public static int getPort(RaftPeerId id, RaftConfiguration conf) {
      final RaftPeer peer = conf.getPeer(id);
      final String address = peer != null? peer.getAddress(): null;
      final InetSocketAddress inetAddress = address != null?
          NetUtils.createSocketAddr(address): NetUtils.createLocalServerAddress();
      return inetAddress.getPort();
    }
  }

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

  public static RaftConfiguration initConfiguration(int numServers) {
    return initConfiguration(generateIds(numServers, 0));
  }

  public static RaftConfiguration initConfiguration(String[] ids) {
    return RaftConfiguration.newBuilder()
        .setConf(Arrays.stream(ids)
            .map(id -> new RaftPeerId(id))
            .map(id -> new RaftPeer(id, NetUtils.createLocalServerAddress()))
            .collect(Collectors.toList()))
        .build();
  }

  private static String getBaseDirectory() {
    return System.getProperty("test.build.data", "target/test/data") + "/raft/";
  }

  private static void formatDir(String dirStr) {
    final File serverDir = new File(dirStr);
    Preconditions.assertTrue(FileUtils.fullyDelete(serverDir),
        "Failed to format directory %s", dirStr);
    LOG.info("Formatted directory {}", dirStr);
  }

  public static String[] generateIds(int numServers, int base) {
    String[] ids = new String[numServers];
    for (int i = 0; i < numServers; i++) {
      ids[i] = "s" + (i + base);
    }
    return ids;
  }

  protected final ClientFactory clientFactory;
  protected RaftConfiguration conf;
  protected final RaftProperties properties;
  protected final Parameters parameters;
  private final String testBaseDir;
  protected final Map<RaftPeerId, RaftServerImpl> servers = new ConcurrentHashMap<>();

  protected MiniRaftCluster(String[] ids, RaftProperties properties, Parameters parameters) {
    this.conf = initConfiguration(ids);
    this.properties = new RaftProperties(properties);
    this.parameters = parameters;

    final RpcType rpcType = RaftConfigKeys.Rpc.type(properties);
    this.clientFactory = ClientFactory.cast(
        rpcType.newFactory(properties, parameters));
    this.testBaseDir = getBaseDirectory();

    ExitUtils.disableSystemExit();
  }

  public MiniRaftCluster initServers() {
    if (servers.isEmpty()) {
      putNewServers(CollectionUtils.as(conf.getPeers(), RaftPeer::getId), true);
    }
    return this;
  }

  private RaftServerImpl putNewServer(RaftPeerId id, boolean format) {
    final RaftServerImpl s = newRaftServer(id, format);
    Preconditions.assertTrue(servers.put(id, s) == null);
    return s;
  }

  private Collection<RaftServerImpl> putNewServers(
      Iterable<RaftPeerId> peers, boolean format) {
    return StreamSupport.stream(peers.spliterator(), false)
        .map(id -> putNewServer(id, format))
        .collect(Collectors.toList());
  }

  public void start() {
    LOG.info("Starting " + getClass().getSimpleName());
    initServers();
    servers.values().forEach(RaftServerImpl::start);
  }

  /**
   * start a stopped server again.
   */
  public void restartServer(String id, boolean format) throws IOException {
    final RaftPeerId newId = new RaftPeerId(id);
    killServer(newId);
    servers.remove(newId);

    startServer(putNewServer(newId, format), true);
  }

  public void restart(boolean format) throws IOException {
    servers.values().stream().filter(RaftServerImpl::isAlive)
        .forEach(RaftServerImpl::close);
    List<RaftPeerId> idList = new ArrayList<>(servers.keySet());
    servers.clear();
    putNewServers(idList, format);
    start();
  }

  public int getMaxTimeout() {
    return RaftServerConfigKeys.Rpc.timeoutMax(properties).toInt(TimeUnit.MILLISECONDS);
  }

  public RaftConfiguration getConf() {
    return conf;
  }

  private RaftServerImpl newRaftServer(RaftPeerId id, boolean format) {
    try {
      final String dirStr = testBaseDir + id;
      if (format) {
        formatDir(dirStr);
      }
      final RaftProperties prop = new RaftProperties(properties);
      RaftServerConfigKeys.setStorageDir(prop, dirStr);
      final StateMachine stateMachine = getStateMachine4Test(properties);
      return newRaftServer(id, stateMachine, conf, prop);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract RaftServerImpl newRaftServer(
      RaftPeerId id, StateMachine stateMachine, RaftConfiguration conf,
      RaftProperties properties) throws IOException;

  static StateMachine getStateMachine4Test(RaftProperties properties) {
    final Class<? extends StateMachine> smClass = properties.getClass(
        STATEMACHINE_CLASS_KEY,
        STATEMACHINE_CLASS_DEFAULT,
        StateMachine.class);
    return ReflectionUtils.newInstance(smClass);
  }

  public static Collection<RaftPeer> toRaftPeers(
      Collection<RaftServerImpl> servers) {
    return servers.stream()
        .map(MiniRaftCluster::toRaftPeer)
        .collect(Collectors.toList());
  }

  public static RaftPeer toRaftPeer(RaftServerImpl s) {
    return new RaftPeer(s.getId(), s.getServerRpc().getInetSocketAddress());
  }

  public PeerChanges addNewPeers(int number, boolean startNewPeer)
      throws IOException {
    return addNewPeers(generateIds(number, servers.size()), startNewPeer);
  }

  public PeerChanges addNewPeers(String[] ids,
      boolean startNewPeer) throws IOException {
    LOG.info("Add new peers {}", Arrays.asList(ids));

    // create and add new RaftServers
    final Collection<RaftServerImpl> newServers = putNewServers(
        CollectionUtils.as(Arrays.asList(ids), RaftPeerId::new), true);
    newServers.forEach(s -> startServer(s, startNewPeer));

    final Collection<RaftPeer> newPeers = toRaftPeers(newServers);
    final RaftPeer[] np = newPeers.toArray(new RaftPeer[newPeers.size()]);
    newPeers.addAll(conf.getPeers());
    conf = RaftConfiguration.newBuilder().setConf(newPeers).setLogEntryIndex(0).build();
    RaftPeer[] p = newPeers.toArray(new RaftPeer[newPeers.size()]);
    return new PeerChanges(p, np, new RaftPeer[0]);
  }

  protected void startServer(RaftServerImpl server, boolean startService) {
    if (startService) {
      server.start();
    }
  }

  public void startServer(RaftPeerId id) {
    startServer(getServer(id), true);
  }

  /**
   * prepare the peer list when removing some peers from the conf
   */
  public PeerChanges removePeers(int number, boolean removeLeader,
      Collection<RaftPeer> excluded) {
    Collection<RaftPeer> peers = new ArrayList<>(conf.getPeers());
    List<RaftPeer> removedPeers = new ArrayList<>(number);
    if (removeLeader) {
      final RaftPeer leader = toRaftPeer(getLeader());
      assert !excluded.contains(leader);
      peers.remove(leader);
      removedPeers.add(leader);
    }
    List<RaftServerImpl> followers = getFollowers();
    for (int i = 0, removed = 0; i < followers.size() &&
        removed < (removeLeader ? number - 1 : number); i++) {
      RaftPeer toRemove = toRaftPeer(followers.get(i));
      if (!excluded.contains(toRemove)) {
        peers.remove(toRemove);
        removedPeers.add(toRemove);
        removed++;
      }
    }
    conf = RaftConfiguration.newBuilder().setConf(peers).setLogEntryIndex(0).build();
    RaftPeer[] p = peers.toArray(new RaftPeer[peers.size()]);
    return new PeerChanges(p, new RaftPeer[0],
        removedPeers.toArray(new RaftPeer[removedPeers.size()]));
  }

  public void killServer(RaftPeerId id) {
    servers.get(id).close();
  }

  public String printServers() {
    StringBuilder b = new StringBuilder("\n#servers = " + servers.size() + "\n");
    for (RaftServerImpl s : servers.values()) {
      b.append("  ");
      b.append(s).append("\n");
    }
    return b.toString();
  }

  public String printAllLogs() {
    StringBuilder b = new StringBuilder("\n#servers = " + servers.size() + "\n");
    for (RaftServerImpl s : servers.values()) {
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

  public RaftServerImpl getLeader() {
    final List<RaftServerImpl> leaders = new ArrayList<>();
    servers.values().stream()
        .filter(s -> s.isAlive() && s.isLeader())
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

  boolean isLeader(String leaderId) throws InterruptedException {
    final RaftServerImpl leader = getLeader();
    return leader != null && leader.getId().toString().equals(leaderId);
  }

  public List<RaftServerImpl> getFollowers() {
    return servers.values().stream()
        .filter(s -> s.isAlive() && s.isFollower())
        .collect(Collectors.toList());
  }

  public Collection<RaftServerImpl> getServers() {
    return servers.values();
  }

  public RaftServerImpl getServer(String id) {
    return getServer(new RaftPeerId(id));
  }

  public RaftServerImpl getServer(RaftPeerId id) {
    return servers.get(id);
  }

  public Collection<RaftPeer> getPeers() {
    return toRaftPeers(getServers());
  }

  public RaftClient createClient(RaftPeerId leaderId) {
    return RaftClient.newBuilder()
        .setServers(conf.getPeers())
        .setLeaderId(leaderId)
        .setClientRpc(clientFactory.newRaftClientRpc())
        .setProperties(properties)
        .build();
  }

  public void shutdown() {
    LOG.info("Stopping " + getClass().getSimpleName());
    servers.values().stream().filter(RaftServerImpl::isAlive)
        .forEach(RaftServerImpl::close);

    if (ExitUtils.isTerminated()) {
      LOG.error("Test resulted in an unexpected exit",
          ExitUtils.getFirstExitException());
      throw new AssertionError("Test resulted in an unexpected exit");
    }
  }

  /**
   * Block all the incoming requests for the peer with leaderId. Also delay
   * outgoing or incoming msg for all other peers.
   */
  protected abstract void blockQueueAndSetDelay(String leaderId, int delayMs)
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
    blockQueueAndSetDelay(leaderId,
        RaftServerConfigKeys.Rpc.TIMEOUT_MIN_DEFAULT.toInt(TimeUnit.MILLISECONDS));

    // Reopen queues so that the vote can make progress.
    blockQueueAndSetDelay(leaderId, 0);

    return isLeader(leaderId);
  }

  /** Block/unblock the requests sent from the given source. */
  public abstract void setBlockRequestsFrom(String src, boolean block);
}
