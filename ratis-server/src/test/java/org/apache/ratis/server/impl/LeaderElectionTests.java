/*
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
package org.apache.ratis.server.impl;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.impl.RatisMetricRegistryImpl;
import org.apache.ratis.metrics.impl.DefaultTimekeeperImpl;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.metrics.LeaderElectionMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogTestUtils;
import org.apache.ratis.test.tag.Flaky;
import org.apache.ratis.thirdparty.com.codahale.metrics.Timer;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.ratis.RaftTestUtil.getPeersWithPriority;
import static org.apache.ratis.RaftTestUtil.waitForLeader;
import static org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LAST_LEADER_ELECTION_ELAPSED_TIME;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_COUNT_METRIC;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_TIME_TAKEN;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_TIMEOUT_COUNT_METRIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.slf4j.event.Level;

public abstract class LeaderElectionTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Slf4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  @Test
  public void testBasicLeaderElection() throws Exception {
    LOG.info("Running testBasicLeaderElection");
    final MiniRaftCluster cluster = newCluster(5);
    cluster.start();
    RaftTestUtil.waitAndKillLeader(cluster);
    RaftTestUtil.waitAndKillLeader(cluster);
    RaftTestUtil.waitAndKillLeader(cluster);
    testFailureCase("waitForLeader after killed a majority of servers",
        () -> RaftTestUtil.waitForLeader(cluster, null, false),
        IllegalStateException.class);
    cluster.shutdown();
  }

  static class SleepCode implements CodeInjectionForTesting.Code {
    private final long sleepMs;

    SleepCode(long sleepMs) {
      this.sleepMs = sleepMs;
    }

    @Override
    public boolean execute(Object localId, Object remoteId, Object... args) {
      try {
        LOG.info("{}: Simulate RaftServer startup blocking", localId);
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return true;
    }
  }

  @Test
  public void testWaitServerReady() throws Exception {
    final int sleepMs = 1000 + ThreadLocalRandom.current().nextInt(1000);
    LOG.info("Running testWaitServerReady, sleep = {}ms", sleepMs);
    CodeInjectionForTesting.put(RaftServerImpl.START_COMPLETE, new SleepCode(sleepMs));
    final MiniRaftCluster cluster = newCluster(1);
    final Timestamp startTime = Timestamp.currentTime();
    cluster.start();
    LOG.info("Cluster started at {}ms", startTime.elapsedTimeMs());
    final RaftGroupId groupId = cluster.getGroupId();
    final RaftServerImpl server = (RaftServerImpl) cluster.getServers().iterator().next().getDivision(groupId);
    final boolean isRunning = server.isRunning();
    LOG.info("{} isRunning at {}ms? {}", server.getId(), startTime.elapsedTimeMs(), isRunning);

    // Leader will be elected if the server is ready
    assertNotNull(waitForLeader(cluster), "No leader is elected.");
    final long elapsedMs = startTime.elapsedTimeMs();
    // allow a small difference to tolerate system timer inaccuracy
    assertTrue(elapsedMs > sleepMs - 10, () -> "elapseMs = " + elapsedMs + " but sleepMs = " + sleepMs);
    cluster.shutdown();
    CodeInjectionForTesting.remove(RaftServerImpl.START_COMPLETE);
  }

  @Test
  public void testAddServerForWaitReady() throws IOException, InterruptedException {
    LOG.info("Running testAddServerForWaitReady");
    // normal startup cluster with 3 server
    final MiniRaftCluster cluster = newCluster(3);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
    try (RaftClient client = cluster.createClient()) {
      for (int i = 0; i < 10; ++i) {
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("message_" + i));
        assertTrue(reply.isSuccess());
      }
      // add 3 new servers and wait longer time
      CodeInjectionForTesting.put(RaftServerImpl.START_COMPLETE, new SleepCode(2000));
      final PeerChanges peerChanges = cluster.addNewPeers(2, true);
      LOG.info("add new 3 servers");
      LOG.info(cluster.printServers());
      RaftClientReply reply = client.admin().setConfiguration(SetConfigurationRequest.Arguments.newBuilder()
              .setServersInNewConf(peerChanges.getAddedPeers())
              .setMode(SetConfigurationRequest.Mode.ADD).build());
      assertTrue(reply.isSuccess());
      for (RaftServer server : cluster.getServers()) {
        RaftServerProxy proxy = (RaftServerProxy) server;
        proxy.getImpls().forEach(s -> {
          assertTrue(s.isRunning());
        });
      }
    }
    cluster.shutdown();;
    CodeInjectionForTesting.remove(RaftServerImpl.START_COMPLETE);
  }

  @Test
  public void testChangeLeader() throws Exception {
    SegmentedRaftLogTestUtils.setRaftLogWorkerLogLevel(Level.TRACE);
    LOG.info("Running testChangeLeader");
    final MiniRaftCluster cluster = newCluster(3);
    cluster.start();

    RaftPeerId leader = RaftTestUtil.waitForLeader(cluster).getId();
    for(int i = 0; i < 10; i++) {
      leader = RaftTestUtil.changeLeader(cluster, leader, Assertions::fail);
    }
    SegmentedRaftLogTestUtils.setRaftLogWorkerLogLevel(Level.INFO);
    cluster.shutdown();
  }

  @Test
  public void testLostMajorityHeartbeats() throws Exception {
    runWithNewCluster(3, this::runTestLostMajorityHeartbeats);
  }

  void runTestLostMajorityHeartbeats(CLUSTER cluster) throws Exception {
    final TimeDuration maxTimeout = RaftServerConfigKeys.Rpc.timeoutMax(getProperties());
    final RaftServer.Division leader = waitForLeader(cluster);
    try {
      RaftTestUtil.isolate(cluster, leader.getId());
      maxTimeout.sleep();
      maxTimeout.sleep();
      RaftServerTestUtil.assertLostMajorityHeartbeatsRecently(leader);
    } finally {
      RaftTestUtil.deIsolate(cluster, leader.getId());
    }
  }

  @Test
  public void testLeaderNotCountListenerForMajority() throws Exception {
    runWithNewCluster(3, 2, this::runTestLeaderNotCountListenerForMajority);
  }

  void runTestLeaderNotCountListenerForMajority(CLUSTER cluster) throws Exception {
    final RaftServer.Division leader = waitForLeader(cluster);
    assertEquals(2, ((RaftConfigurationImpl)cluster.getLeader().getRaftConf()).getMajorityCount());
    try (RaftClient client = cluster.createClient(leader.getId())) {
      client.io().send(new RaftTestUtil.SimpleMessage("message"));
      List<RaftPeer> listeners = cluster.getListeners()
          .stream().map(RaftServer.Division::getPeer).collect(Collectors.toList());
      assertEquals(2, listeners.size());
      RaftClientReply reply = client.admin().setConfiguration(cluster.getPeers());
      assertTrue(reply.isSuccess());
      Collection<RaftPeer> peer = leader.getRaftConf().getAllPeers(RaftProtos.RaftPeerRole.LISTENER);
      assertEquals(0, peer.size());
    }
    assertEquals(3, ((RaftConfigurationImpl)cluster.getLeader().getRaftConf()).getMajorityCount());
  }

  @Test
  public void testListenerNotStartLeaderElection() throws Exception {
    runWithNewCluster(3, 2, this::runTestListenerNotStartLeaderElection);
  }

  void runTestListenerNotStartLeaderElection(CLUSTER cluster) throws Exception {
    final RaftServer.Division leader = waitForLeader(cluster);
    final TimeDuration maxTimeout = RaftServerConfigKeys.Rpc.timeoutMax(getProperties());

    final RaftServer.Division listener = cluster.getListeners().get(0);
    final RaftPeerId listenerId = listener.getId();
    try {
      RaftTestUtil.isolate(cluster, listenerId);
      maxTimeout.sleep();
      maxTimeout.sleep();
      assertEquals(RaftProtos.RaftPeerRole.LISTENER, listener.getInfo().getCurrentRole());
    } finally {
      RaftTestUtil.deIsolate(cluster, listener.getId());
    }
  }

  @Test
  public void testTransferLeader() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));

        List<RaftServer.Division> followers = cluster.getFollowers();
        assertEquals(2, followers.size());
        RaftServer.Division newLeader = followers.get(0);

        RaftClientReply reply = client.admin().transferLeadership(newLeader.getId(), 20000);
        assertTrue(reply.isSuccess());

        final RaftServer.Division currLeader = waitForLeader(cluster);
        assertEquals(newLeader.getId(), currLeader.getId());

        reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        assertEquals(newLeader.getId().toString(), reply.getReplierId());
        assertTrue(reply.isSuccess());
      }

      cluster.shutdown();
    }
  }

  @Test
  public void testYieldLeaderToHigherPriority() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));

        List<RaftServer.Division> followers = cluster.getFollowers();
        assertEquals(2, followers.size());
        RaftServer.Division newLeader = followers.get(0);

        List<RaftPeer> peers = cluster.getPeers();
        List<RaftPeer> peersWithNewPriority = getPeersWithPriority(peers, newLeader.getPeer());
        RaftClientReply reply = client.admin().setConfiguration(peersWithNewPriority.toArray(new RaftPeer[0]));
        assertTrue(reply.isSuccess());

        // Wait the old leader to step down.
        // TODO: make it more deterministic.
        TimeDuration.valueOf(1, TimeUnit.SECONDS).sleep();

        final RaftServer.Division currLeader = waitForLeader(cluster);
        assertEquals(newLeader.getId(), currLeader.getId());

        reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        assertEquals(newLeader.getId().toString(), reply.getReplierId());
        assertTrue(reply.isSuccess());
      }

      cluster.shutdown();
    }
  }

  @Test
  public void testTransferLeaderTimeout() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        List<RaftServer.Division> followers = cluster.getFollowers();
        assertEquals(followers.size(), 2);
        RaftServer.Division newLeader = followers.get(0);

        // isolate new leader, so that transfer leadership will timeout
        RaftTestUtil.isolate(cluster, newLeader.getId());

        List<RaftPeer> peers = cluster.getPeers();

        CompletableFuture<Boolean> transferTimeoutFuture = CompletableFuture.supplyAsync(() -> {
          try {
            long timeoutMs = 5000;
            long start = System.currentTimeMillis();
            try {
              client.admin().transferLeadership(newLeader.getId(), timeoutMs);
            } catch (TransferLeadershipException e) {
              long cost = System.currentTimeMillis() - start;
              assertTrue(cost > timeoutMs);
              assertTrue(e.getMessage().contains("Failed to transfer leadership to"));
              assertTrue(e.getMessage().contains(TransferLeadership.Result.Type.TIMED_OUT.toString()));
            }

            return true;
          } catch (IOException e) {
            return false;
          }
        });

        // before transfer timeout, leader should in steppingDown
        JavaUtils.attemptRepeatedly(() -> {
          try {
            client.io().send(new RaftTestUtil.SimpleMessage("message"));
          } catch (LeaderSteppingDownException e) {
            assertTrue(e.getMessage().contains("is stepping down"));
          }
          return null;
        }, 5, TimeDuration.ONE_SECOND, "check leader steppingDown", RaftServer.LOG);

        assertTrue(transferTimeoutFuture.get());

        // after transfer timeout, leader should accept request
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        assertEquals(leader.getId().toString(), reply.getReplierId());
        assertTrue(reply.isSuccess());

        RaftTestUtil.deIsolate(cluster, newLeader.getId());
      }

      cluster.shutdown();
    }
  }

  @Test
  public void testEnforceLeader() throws Exception {
    LOG.info("Running testEnforceLeader");
    final int numServer = 5;
    try(final MiniRaftCluster cluster = newCluster(numServer)) {
      cluster.start();

      final RaftPeerId firstLeader = waitForLeader(cluster).getId();
      LOG.info("firstLeader = {}", firstLeader);
      final int first = MiniRaftCluster.getIdIndex(firstLeader.toString());

      final int random = ThreadLocalRandom.current().nextInt(numServer - 1);
      final String newLeader = "s" + (random < first? random: random + 1);
      LOG.info("enforce leader to {}", newLeader);
      enforceLeader(cluster, newLeader, LOG);
    }
  }

  static void enforceLeader(MiniRaftCluster cluster, final String newLeader, Logger log) throws InterruptedException {
    log.info(cluster.printServers());
    for(int i = 0; !cluster.tryEnforceLeader(newLeader) && i < 10; i++) {
      final RaftServer.Division currLeader = cluster.getLeader();
      log.info("try enforcing leader to " + newLeader + " but " +
          (currLeader == null ? "no leader for round " + i : "new leader is " + currLeader.getId()));
      TimeDuration.ONE_SECOND.sleep();
    }
    log.info(cluster.printServers());

    final RaftServer.Division leader = cluster.getLeader();
    assertEquals(newLeader, leader.getId().toString());
  }

  @Test
  public void testLateServerStart() throws Exception {
    final int numServer = 3;
    LOG.info("Running testLateServerStart");
    final MiniRaftCluster cluster = newCluster(numServer);
    cluster.initServers();

    // start all except one servers
    final Iterator<RaftServer> i = cluster.getServers().iterator();
    for(int j = 1; j < numServer; j++) {
      i.next().start();
    }

    final RaftServer.Division leader = waitForLeader(cluster);
    final TimeDuration sleepTime = TimeDuration.valueOf(3, TimeUnit.SECONDS);
    LOG.info("sleep " + sleepTime);
    sleepTime.sleep();

    // start the last server
    final RaftServerProxy lastServer = (RaftServerProxy) i.next();
    lastServer.start();
    final RaftPeerId lastServerLeaderId = JavaUtils.attemptRepeatedly(
        () -> Optional.ofNullable(lastServer.getImpls().iterator().next().getState().getLeaderId())
            .orElseThrow(() -> new IllegalStateException("No leader yet")),
        10, ONE_SECOND, "getLeaderId", LOG);
    LOG.info(cluster.printServers());
    assertEquals(leader.getId(), lastServerLeaderId);
    cluster.shutdown();
  }

  protected void testDisconnectLeader() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Thread.sleep(1000);
        RaftTestUtil.isolate(cluster, leader.getId());
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Assertions.assertNotEquals(reply.getReplierId(), leader.getId().toString());
        assertTrue(reply.isSuccess());
      } finally {
        RaftTestUtil.deIsolate(cluster, leader.getId());
      }

      cluster.shutdown();
    }
  }

  @Test
  public void testAddListener() throws Exception {
    try (final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();
      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        List<RaftPeer> servers = cluster.getPeers();
        assertEquals(servers.size(), 3);
        final PeerChanges changes = cluster.addNewPeers(1, true);
        final List<RaftPeer> added = changes.getAddedPeers();
        final RaftClientReply reply = client.admin().setConfiguration(servers, added);
        assertTrue(reply.isSuccess());
        Collection<RaftPeer> listener =
            leader.getRaftConf().getAllPeers(RaftProtos.RaftPeerRole.LISTENER);
        assertEquals(1, listener.size());
        assertEquals(added.get(0).getId(), listener.iterator().next().getId());
      }
      cluster.shutdown();
    }
  }

  @Test
  public void testAddFollowerWhenExistsListener() throws Exception {
    try (final MiniRaftCluster cluster = newCluster(3, 1)) {
      cluster.start();
      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        List<RaftPeer> servers = cluster.getPeers();
        assertEquals(4, servers.size());
        List<RaftPeer> listener = new ArrayList<>(
            leader.getRaftConf().getAllPeers(RaftProtos.RaftPeerRole.LISTENER));
        assertEquals(1, listener.size());
        final PeerChanges changes = cluster.addNewPeers(1, true);
        final List<RaftPeer> newPeers = new ArrayList<>(changes.getAddedPeers());
        newPeers.addAll(leader.getRaftConf().getAllPeers(RaftProtos.RaftPeerRole.FOLLOWER));
        RaftClientReply reply = client.admin().setConfiguration(newPeers, listener);
        assertTrue(reply.isSuccess());
        assertEquals(4,
            leader.getRaftConf().getAllPeers(RaftProtos.RaftPeerRole.FOLLOWER).size());
        assertEquals(1,
            leader.getRaftConf().getAllPeers(RaftProtos.RaftPeerRole.LISTENER).size());
      }
      cluster.shutdown();
    }
  }

  @Test
  public void testRemoveListener() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3,1)) {
      cluster.start();
      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        assertEquals(1, cluster.getListeners().size());
        List<RaftPeer> servers = cluster.getFollowers().stream().map(RaftServer.Division::getPeer).collect(
            Collectors.toList());
        servers.add(leader.getPeer());
        RaftClientReply reply = client.admin().setConfiguration(servers);
        assertTrue(reply.isSuccess());
        assertEquals(0, leader.getRaftConf().getAllPeers(RaftProtos.RaftPeerRole.LISTENER).size());
      }
      cluster.shutdown();
    }
  }

  @Test
  public void testChangeFollowerToListener() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();
      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient()) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        List<RaftPeer> followers = cluster.getFollowers().stream().map(
            RaftServer.Division::getPeer).collect(Collectors.toList());
        assertEquals(2, followers.size());
        List<RaftPeer> listeners = new ArrayList<>();
        listeners.add(followers.get(1));
        followers.remove(1);
        RaftClientReply reply = client.admin().setConfiguration(followers, listeners);
        assertTrue(reply.isSuccess());
        Collection<RaftPeer> peer = leader.getRaftConf().getAllPeers(RaftProtos.RaftPeerRole.LISTENER);
        assertEquals(1, peer.size());
        assertEquals(listeners.get(0).getId(), new ArrayList<>(peer).get(0).getId());
      }
      cluster.shutdown();
    }
  }

  @Test
  public void testChangeListenerToFollower() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(2, 1)) {
      cluster.start();
      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        List<RaftPeer> listeners = cluster.getListeners()
            .stream().map(RaftServer.Division::getPeer).collect(Collectors.toList());
        assertEquals(listeners.size(), 1);
        RaftClientReply reply = client.admin().setConfiguration(cluster.getPeers());
        assertTrue(reply.isSuccess());
        Collection<RaftPeer> peer = leader.getRaftConf().getAllPeers(RaftProtos.RaftPeerRole.LISTENER);
        assertEquals(0, peer.size());

        // multiple messages to trigger listener change role to follower
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        client.io().send(new RaftTestUtil.SimpleMessage("message"));

        listeners = cluster.getListeners()
                  .stream().map(RaftServer.Division::getPeer).collect(Collectors.toList());
        assertEquals(0, listeners.size());
      }
      cluster.shutdown();
    }
  }

  @Test
  public void testLeaderElectionMetrics() throws IOException, InterruptedException {
    Timestamp timestamp = Timestamp.currentTime();
    final MiniRaftCluster cluster = newCluster(3);
    cluster.start();
    final RaftServer.Division leaderServer = waitForLeader(cluster);

    final RatisMetricRegistryImpl ratisMetricRegistry = (RatisMetricRegistryImpl)
        LeaderElectionMetrics.createRegistry(leaderServer.getMemberId());

    // Verify each metric individually.
    long numLeaderElections = ratisMetricRegistry.counter(LEADER_ELECTION_COUNT_METRIC).getCount();
    assertTrue(numLeaderElections > 0);

    long numLeaderElectionTimeout = ratisMetricRegistry.counter(LEADER_ELECTION_TIMEOUT_COUNT_METRIC).getCount();
    assertTrue(numLeaderElectionTimeout > 0);

    final DefaultTimekeeperImpl timekeeper =
        (DefaultTimekeeperImpl) ratisMetricRegistry.timer(LEADER_ELECTION_TIME_TAKEN);
    final Timer timer = timekeeper.getTimer();
    double meanTimeNs = timer.getSnapshot().getMean();
    long elapsedNs = timestamp.elapsedTime().toLong(TimeUnit.NANOSECONDS);
    assertTrue(timer.getCount() > 0 && meanTimeNs < elapsedNs);
    Long leaderElectionLatency = (Long) ratisMetricRegistry.getGauges((s, metric) ->
        s.contains(LAST_LEADER_ELECTION_ELAPSED_TIME)).values().iterator().next().getValue();
    assertTrue(leaderElectionLatency > 0L);
  }

  @Test
  public void testImmediatelyRevertedToFollower() {
    RaftServerImpl server = createMockServer(true);
    LeaderElection subject = LeaderElection.newInstance(server, false);

    try {
      subject.startInForeground();
      assertEquals(LifeCycle.State.CLOSED, subject.getCurrentState());
    } catch (Exception e) {
      LOG.info("Error starting LeaderElection", e);
      fail(e.getMessage());
    }
  }

  @Test
  public void testShutdownBeforeStart() {
    RaftServerImpl server = createMockServer(false);
    LeaderElection subject = LeaderElection.newInstance(server, false);

    try {
      subject.shutdown();
      subject.startInForeground();
      assertEquals(LifeCycle.State.CLOSED, subject.getCurrentState());
    } catch (Exception e) {
      LOG.info("Error starting LeaderElection", e);
      fail(e.getMessage());
    }
  }

  @Test
  public void testPreVote() {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      RaftServer.Division leader = waitForLeader(cluster);

      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));

        final List<RaftServer.Division> followers = cluster.getFollowers();
        assertEquals(followers.size(), 2);

        RaftServer.Division follower = followers.get(0);
        RaftTestUtil.isolate(cluster, follower.getId());
        // send message so that the isolated follower's log lag the others
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        assertTrue(reply.isSuccess());

        final long savedTerm = leader.getInfo().getCurrentTerm();
        LOG.info("Wait follower {} timeout and trigger pre-vote", follower.getId());
        Thread.sleep(2000);
        RaftTestUtil.deIsolate(cluster, follower.getId());
        Thread.sleep(2000);
        // with pre-vote leader will not step down
        RaftServer.Division newleader = waitForLeader(cluster);
        assertNotNull(newleader);
        assertEquals(newleader.getId(), leader.getId());
        // with pre-vote, term will not change
        assertEquals(savedTerm, leader.getInfo().getCurrentTerm());

        reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        assertTrue(reply.isSuccess());
      }

      cluster.shutdown();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testListenerRejectRequestVote() throws Exception {
    runWithNewCluster(3, 2, this::runTestListenerRejectRequestVote);
  }
  void runTestListenerRejectRequestVote(CLUSTER cluster) throws IOException, InterruptedException {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final TermIndex lastEntry = leader.getRaftLog().getLastEntryTermIndex();
    RaftServer.Division listener = cluster.getListeners().get(0);
    final RaftProtos.RequestVoteRequestProto r = ServerProtoUtils.toRequestVoteRequestProto(
        leader.getMemberId(), listener.getId(),
        leader.getRaftLog().getLastEntryTermIndex().getTerm() + 1, lastEntry, true);
    RaftProtos.RequestVoteReplyProto listenerReply = listener.getRaftServer().requestVote(r);
    Assertions.assertFalse(listenerReply.getServerReply().getSuccess());
  }


  @Test
  public void testPauseResumeLeaderElection() throws Exception {
    runWithNewCluster(3, this::runTestPauseResumeLeaderElection);
  }

  void runTestPauseResumeLeaderElection(CLUSTER cluster) throws IOException, InterruptedException {
    final RaftClientReply pauseLeaderReply;
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    final List<RaftServer.Division> followers = cluster.getFollowers();
    assertTrue(followers.size() >= 1);
    final RaftServerImpl f1 = (RaftServerImpl)followers.get(0);

    try (final RaftClient client = cluster.createClient()) {
      pauseLeaderReply = client.getLeaderElectionManagementApi(f1.getId()).pause();
      assertTrue(pauseLeaderReply.isSuccess());
      client.io().send(new RaftTestUtil.SimpleMessage("message"));
      RaftServer.Division newLeader = followers.get(0);
      List<RaftPeer> peers = cluster.getPeers();
      List<RaftPeer> peersWithNewPriority = getPeersWithPriority(peers, newLeader.getPeer());
      RaftClientReply reply = client.admin().setConfiguration(peersWithNewPriority.toArray(new RaftPeer[0]));
      assertTrue(reply.isSuccess());
      JavaUtils.attempt(() -> assertEquals(leaderId, leader.getId()),
          20, HUNDRED_MILLIS, "check leader id", LOG);
      final RaftClientReply resumeLeaderReply = client.getLeaderElectionManagementApi(f1.getId()).resume();
      assertTrue(resumeLeaderReply.isSuccess());
      JavaUtils.attempt(() -> assertEquals(f1.getId(), cluster.getLeader().getId()),
          20, HUNDRED_MILLIS, "check new leader", LOG);
    }
  }

  private void runLeaseTest(CLUSTER cluster, CheckedBiConsumer<CLUSTER, Long, Exception> testCase) throws Exception {
    final double leaseRatio = RaftServerConfigKeys.Read.leaderLeaseTimeoutRatio(getProperties());
    final long leaseTimeoutMs = RaftServerConfigKeys.Rpc.timeoutMin(getProperties())
        .multiply(leaseRatio)
        .toIntExact(TimeUnit.MILLISECONDS);
    testCase.accept(cluster, leaseTimeoutMs);
  }

  @Test
  public void testLeaderLease() throws Exception {
    // use a strict lease
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(getProperties(), true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(getProperties(), 0.5);
    runWithNewCluster(3, c -> runLeaseTest(c, this::runTestLeaderLease));
  }

  void runTestLeaderLease(CLUSTER cluster, long leaseTimeoutMs) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    try (final RaftClient client = cluster.createClient(leader.getId())) {
      client.io().send(new RaftTestUtil.SimpleMessage("message"));

      assertTrue(leader.getInfo().isLeader());
      assertTrue(leader.getInfo().isLeaderReady());
      RaftServerTestUtil.assertLeaderLease(leader, true);

      RaftTestUtil.isolate(cluster, leader.getId());
      Thread.sleep(leaseTimeoutMs);

      assertTrue(leader.getInfo().isLeader());
      assertTrue(leader.getInfo().isLeaderReady());
      RaftServerTestUtil.assertLeaderLease(leader, false);
    } finally {
      RaftTestUtil.deIsolate(cluster, leader.getId());
    }
  }

  @Flaky("RATIS-2108")
  @Test
  public void testLeaderLeaseDuringReconfiguration() throws Exception {
    // use a strict lease
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(getProperties(), true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(getProperties(), 0.5);
    runWithNewCluster(3, c -> runLeaseTest(c, this::runTestLeaderLeaseDuringReconfiguration));
  }

  void runTestLeaderLeaseDuringReconfiguration(CLUSTER cluster, long leaseTimeoutMs) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    try (final RaftClient client = cluster.createClient(leader.getId())) {
      client.io().send(new RaftTestUtil.SimpleMessage("message"));

      assertTrue(leader.getInfo().isLeader());
      assertTrue(leader.getInfo().isLeaderReady());
      RaftServerTestUtil.assertLeaderLease(leader, true);

      final List<RaftServer.Division> followers = cluster.getFollowers();
      final PeerChanges changes = cluster.addNewPeers(2, true);

      // blocking the original 2 followers
      BlockRequestHandlingInjection.getInstance().blockReplier(followers.get(0).getId().toString());
      BlockRequestHandlingInjection.getInstance().blockReplier(followers.get(1).getId().toString());

      // start reconfiguration in another thread, shall fail eventually
      new Thread(() -> {
        try {
          client.admin().setConfiguration(changes.getPeersInNewConf());
        } catch (IOException e) {
          System.out.println("as expected: " + e.getMessage());
        }
      }).start();

      Thread.sleep(leaseTimeoutMs);

      assertTrue(leader.getInfo().isLeader());
      assertTrue(leader.getInfo().isLeaderReady());
      RaftServerTestUtil.assertLeaderLease(leader, false);

    } finally {
      BlockRequestHandlingInjection.getInstance().unblockAll();
    }
  }




  private static RaftServerImpl createMockServer(boolean alive) {
    final DivisionInfo info = mock(DivisionInfo.class);
    when(info.isAlive()).thenReturn(alive);
    when(info.isCandidate()).thenReturn(false);
    RaftServerImpl server = mock(RaftServerImpl.class);
    when(server.getInfo()).thenReturn(info);
    final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("any"), RaftGroupId.randomId());
    when(server.getMemberId()).thenReturn(memberId);
    LeaderElectionMetrics leaderElectionMetrics = LeaderElectionMetrics.getLeaderElectionMetrics(memberId, () -> 0);
    when(server.getLeaderElectionMetrics()).thenReturn(leaderElectionMetrics);
    RaftServerProxy proxy = mock(RaftServerProxy.class);
    RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.LeaderElection.setPreVote(properties, true);
    when(proxy.getProperties()).thenReturn(properties);
    when(server.getRaftServer()).thenReturn(proxy);
    return server;
  }
}
