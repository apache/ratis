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

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.metrics.LeaderElectionMetrics;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.RaftTestUtil.waitForLeader;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LAST_LEADER_ELECTION_ELAPSED_TIME;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_COUNT_METRIC;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_TIME_TAKEN;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_TIMEOUT_COUNT_METRIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Timer;

public abstract class LeaderElectionTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
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

  @Test
  public void testChangeLeader() throws Exception {
    SegmentedRaftLogTestUtils.setRaftLogWorkerLogLevel(Level.TRACE);
    LOG.info("Running testChangeLeader");
    final MiniRaftCluster cluster = newCluster(3);
    cluster.start();

    RaftPeerId leader = RaftTestUtil.waitForLeader(cluster).getId();
    for(int i = 0; i < 10; i++) {
      leader = RaftTestUtil.changeLeader(cluster, leader, IllegalStateException::new);
      ExitUtils.assertNotTerminated();
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
      isolate(cluster, leader.getId());
      maxTimeout.sleep();
      maxTimeout.sleep();
      RaftServerTestUtil.assertLostMajorityHeartbeatsRecently(leader);
    } finally {
      deIsolate(cluster, leader.getId());
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

  static void enforceLeader(MiniRaftCluster cluster, final String newLeader, Logger LOG) throws InterruptedException {
    LOG.info(cluster.printServers());
    for(int i = 0; !cluster.tryEnforceLeader(newLeader) && i < 10; i++) {
      final RaftServer.Division currLeader = cluster.getLeader();
      LOG.info("try enforcing leader to " + newLeader + " but " +
          (currLeader == null ? "no leader for round " + i : "new leader is " + currLeader.getId()));
    }
    LOG.info(cluster.printServers());

    final RaftServer.Division leader = cluster.getLeader();
    Assert.assertEquals(newLeader, leader.getId().toString());
  }

  @Test
  public void testLateServerStart() throws Exception {
    final int numServer = 3;
    LOG.info("Running testLateServerStart");
    final MiniRaftCluster cluster = newCluster(numServer);
    cluster.initServers();

    // start all except one servers
    final Iterator<RaftServerProxy> i = cluster.getServers().iterator();
    for(int j = 1; j < numServer; j++) {
      i.next().start();
    }

    final RaftServer.Division leader = waitForLeader(cluster);
    final TimeDuration sleepTime = TimeDuration.valueOf(3, TimeUnit.SECONDS);
    LOG.info("sleep " + sleepTime);
    sleepTime.sleep();

    // start the last server
    final RaftServerProxy lastServer = i.next();
    lastServer.start();
    final RaftPeerId lastServerLeaderId = JavaUtils.attemptRepeatedly(
        () -> Optional.ofNullable(lastServer.getImpls().iterator().next().getState().getLeaderId())
            .orElseThrow(() -> new IllegalStateException("No leader yet")),
        10, ONE_SECOND, "getLeaderId", LOG);
    LOG.info(cluster.printServers());
    Assert.assertEquals(leader.getId(), lastServerLeaderId);
  }

  protected void testDisconnectLeader() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(3)) {
      cluster.start();

      final RaftServer.Division leader = waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Thread.sleep(1000);
        isolate(cluster, leader.getId());
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("message"));
        Assert.assertNotEquals(reply.getReplierId(), leader.getId().toString());
        Assert.assertTrue(reply.isSuccess());
      } finally {
        deIsolate(cluster, leader.getId());
      }

      cluster.shutdown();
    }
  }

  private void isolate(MiniRaftCluster cluster, RaftPeerId id) {
    try {
      BlockRequestHandlingInjection.getInstance().blockReplier(id.toString());
      cluster.setBlockRequestsFrom(id.toString(), true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void deIsolate(MiniRaftCluster cluster, RaftPeerId id) {
    BlockRequestHandlingInjection.getInstance().unblockReplier(id.toString());
    cluster.setBlockRequestsFrom(id.toString(), false);
  }

  @Test
  public void testLeaderElectionMetrics() throws IOException, InterruptedException {
    Timestamp timestamp = Timestamp.currentTime();
    final MiniRaftCluster cluster = newCluster(3);
    cluster.start();
    final RaftServer.Division leaderServer = waitForLeader(cluster);

    final RatisMetricRegistry ratisMetricRegistry = LeaderElectionMetrics.getMetricRegistryForLeaderElection(
        leaderServer.getMemberId());

    // Verify each metric individually.
    long numLeaderElections = ratisMetricRegistry.counter(LEADER_ELECTION_COUNT_METRIC).getCount();
    assertTrue(numLeaderElections > 0);

    long numLeaderElectionTimeout = ratisMetricRegistry.counter(LEADER_ELECTION_TIMEOUT_COUNT_METRIC).getCount();
    assertTrue(numLeaderElectionTimeout > 0);

    Timer timer = ratisMetricRegistry.timer(LEADER_ELECTION_TIME_TAKEN);
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
    LeaderElection subject = new LeaderElection(server);

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
    LeaderElection subject = new LeaderElection(server);

    try {
      subject.shutdown();
      subject.startInForeground();
      assertEquals(LifeCycle.State.CLOSED, subject.getCurrentState());
    } catch (Exception e) {
      LOG.info("Error starting LeaderElection", e);
      fail(e.getMessage());
    }
  }

  private static RaftServerImpl createMockServer(boolean alive) {
    RaftServerImpl server = mock(RaftServerImpl.class);
    when(server.isAlive()).thenReturn(alive);
    when(server.isCandidate()).thenReturn(false);
    final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("any"), RaftGroupId.randomId());
    when(server.getMemberId()).thenReturn(memberId);
    LeaderElectionMetrics leaderElectionMetrics = LeaderElectionMetrics.getLeaderElectionMetrics(memberId, () -> 0);
    when(server.getLeaderElectionMetrics()).thenReturn(leaderElectionMetrics);
    return server;
  }
}
