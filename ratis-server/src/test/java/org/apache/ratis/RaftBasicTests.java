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
package org.apache.ratis;

import com.codahale.metrics.Gauge;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.impl.RetryCacheTestUtil;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.ratis.RaftTestUtil.waitForLeader;
import static org.apache.ratis.metrics.RatisMetrics.RATIS_APPLICATION_NAME_METRICS;
import static org.apache.ratis.server.impl.StateMachineMetrics.RATIS_STATEMACHINE_METRICS;
import static org.apache.ratis.server.impl.StateMachineMetrics.RATIS_STATEMACHINE_METRICS_DESC;
import static org.apache.ratis.server.impl.StateMachineMetrics.STATEMACHINE_APPLIED_INDEX_GAUGE;
import static org.apache.ratis.server.impl.StateMachineMetrics.STATEMACHINE_APPLY_COMPLETED_GAUGE;

public abstract class RaftBasicTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    RaftServerTestUtil.setStateMachineUpdaterLogLevel(Level.DEBUG);

    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), TimeDuration.valueOf(5, TimeUnit.SECONDS));
  }

  public static final int NUM_SERVERS = 5;

  @Test
  public void testBasicAppendEntries() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster ->
        runTestBasicAppendEntries(false, false, 10, cluster, LOG));
  }

  @Test
  public void testBasicAppendEntriesKillLeader() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster ->
        runTestBasicAppendEntries(false, true, 10, cluster, LOG));
  }

  static CompletableFuture<Void> killAndRestartServer(
      RaftPeerId id, long killSleepMs, long restartSleepMs, MiniRaftCluster cluster, Logger LOG) {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    new Thread(() -> {
      try {
        Thread.sleep(killSleepMs);
        cluster.killServer(id);
        Thread.sleep(restartSleepMs);
        LOG.info("restart server: " + id);
        cluster.restartServer(id, false);
        future.complete(null);
      } catch (Exception e) {
        ExitUtils.terminate(-1, "Failed to kill/restart server: " + id, e, LOG);
      }
    }).start();
    return future;
  }

  static void runTestBasicAppendEntries(
      boolean async, boolean killLeader, int numMessages, MiniRaftCluster cluster, Logger LOG)
      throws Exception {
    LOG.info("runTestBasicAppendEntries: async? {}, killLeader={}, numMessages={}",
        async, killLeader, numMessages);
    for (RaftServer s : cluster.getServers()) {
      cluster.restartServer(s.getId(), false);
    }
    RaftServer.Division leader = waitForLeader(cluster);
    final long term = leader.getInfo().getCurrentTerm();

    final CompletableFuture<Void> killAndRestartFollower = killAndRestartServer(
        cluster.getFollowers().get(0).getId(), 0, 1000, cluster, LOG);
    final CompletableFuture<Void> killAndRestartLeader;
    if (killLeader) {
      LOG.info("killAndRestart leader " + leader.getId());
      killAndRestartLeader = killAndRestartServer(leader.getId(), 2000, 4000, cluster, LOG);
    } else {
      killAndRestartLeader = CompletableFuture.completedFuture(null);
    }

    LOG.info(cluster.printServers());

    final SimpleMessage[] messages = SimpleMessage.create(numMessages);

    try (final RaftClient client = cluster.createClient()) {
      final AtomicInteger asyncReplyCount = new AtomicInteger();
      final CompletableFuture<Void> f = new CompletableFuture<>();

      for (SimpleMessage message : messages) {
        if (async) {
          client.async().send(message).thenAcceptAsync(reply -> {
            if (!reply.isSuccess()) {
              f.completeExceptionally(
                  new AssertionError("Failed with reply " + reply));
            } else if (asyncReplyCount.incrementAndGet() == messages.length) {
              f.complete(null);
            }
          });
        } else {
          final RaftClientReply reply = client.io().send(message);
          Assert.assertTrue(reply.isSuccess());
        }
      }
      if (async) {
        f.join();
        Assert.assertEquals(messages.length, asyncReplyCount.get());
      }
    }
    Thread.sleep(cluster.getTimeoutMax().toIntExact(TimeUnit.MILLISECONDS) + 100);
    LOG.info(cluster.printAllLogs());
    killAndRestartFollower.join();
    killAndRestartLeader.join();


    final List<RaftServer.Division> divisions = cluster.getServerAliveStream().collect(Collectors.toList());
    for(RaftServer.Division impl: divisions) {
        JavaUtils.attempt(() -> RaftTestUtil.assertLogEntries(impl, term, messages),
            50, TimeDuration.valueOf(1, TimeUnit.SECONDS), impl.getId() + " assertLogEntries", LOG);
    }
  }


  @Test
  public void testOldLeaderCommit() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestOldLeaderCommit);
  }

  void runTestOldLeaderCommit(CLUSTER cluster) throws Exception {
    final RaftServer.Division leader = waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    final long term = leader.getInfo().getCurrentTerm();

    final List<RaftServer.Division> followers = cluster.getFollowers();
    final List<RaftServer.Division> followersToSendLog = followers.subList(0, followers.size() / 2);
    for (int i = followers.size() / 2; i < NUM_SERVERS - 1; i++) {
      cluster.killServer(followers.get(i).getId());
    }

    SimpleMessage[] messages = SimpleMessage.create(1);
    RaftTestUtil.sendMessageInNewThread(cluster, leaderId, messages);

    Thread.sleep(cluster.getTimeoutMax().toLong(TimeUnit.MILLISECONDS) + 100);
    for (RaftServer.Division followerToSendLog : followersToSendLog) {
      RaftLog followerLog = followerToSendLog.getRaftLog();
      Assert.assertTrue(RaftTestUtil.logEntriesContains(followerLog, messages));
    }

    LOG.info(String.format("killing old leader: %s", leaderId.toString()));
    cluster.killServer(leaderId);

    for (int i = followers.size() / 2; i < NUM_SERVERS - 1; i++) {
      final RaftPeerId followerId = followers.get(i).getId();
      LOG.info(String.format("restarting follower: %s", followerId));
      cluster.restartServer(followerId, false);
    }

    Thread.sleep(cluster.getTimeoutMax().toLong(TimeUnit.MILLISECONDS) * 5);
    // confirm the server with log is elected as new leader.
    final RaftPeerId newLeaderId = waitForLeader(cluster).getId();
    Set<RaftPeerId> followersToSendLogIds =
        followersToSendLog.stream().map(f -> f.getId()).collect(Collectors.toSet());

    Assert.assertTrue(followersToSendLogIds.contains(newLeaderId));

    cluster.getServerAliveStream()
        .map(RaftServer.Division::getRaftLog)
        .forEach(log -> RaftTestUtil.assertLogEntries(log, term, messages));
  }

  @Test
  public void testOldLeaderNotCommit() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestOldLeaderNotCommit);
  }

  void runTestOldLeaderNotCommit(CLUSTER cluster) throws Exception {
    final RaftPeerId leaderId = waitForLeader(cluster).getId();

    final List<RaftServer.Division> followers = cluster.getFollowers();
    final RaftServer.Division followerToCommit = followers.get(0);
    try {
      for (int i = 1; i < NUM_SERVERS - 1; i++) {
        cluster.killServer(followers.get(i).getId());
      }
    } catch (IndexOutOfBoundsException e) {
      throw new org.junit.AssumptionViolatedException("The assumption is follower.size() = NUM_SERVERS - 1, "
              + "actual NUM_SERVERS is " + NUM_SERVERS + ", and actual follower.size() is " + followers.size(), e);
    }

    SimpleMessage[] messages = SimpleMessage.create(1);
    RaftTestUtil.sendMessageInNewThread(cluster, leaderId, messages);

    Thread.sleep(cluster.getTimeoutMax().toLong(TimeUnit.MILLISECONDS) + 100);
    RaftTestUtil.logEntriesContains(followerToCommit.getRaftLog(), messages);

    cluster.killServer(leaderId);
    cluster.killServer(followerToCommit.getId());

    for (int i = 1; i < NUM_SERVERS - 1; i++) {
      cluster.restartServer(followers.get(i).getId(), false );
    }
    waitForLeader(cluster);
    Thread.sleep(cluster.getTimeoutMax().toLong(TimeUnit.MILLISECONDS) + 100);

    final Predicate<LogEntryProto> predicate = l -> l.getTerm() != 1;
    cluster.getServerAliveStream()
        .map(RaftServer.Division::getRaftLog)
        .forEach(log -> RaftTestUtil.checkLogEntries(log, messages, predicate));
  }

  static class Client4TestWithLoad extends Thread {
    boolean useAsync;
    final int index;
    final SimpleMessage[] messages;

    final AtomicBoolean isRunning = new AtomicBoolean(true);
    final AtomicInteger step = new AtomicInteger();
    final AtomicReference<Throwable> exceptionInClientThread = new AtomicReference<>();

    final MiniRaftCluster cluster;
    final Logger LOG;

    Client4TestWithLoad(int index, int numMessages, boolean useAsync,
        MiniRaftCluster cluster, Logger LOG) {
      super("client-" + index);
      this.index = index;
      this.messages = SimpleMessage.create(numMessages, index + "-");
      this.useAsync = useAsync;
      this.cluster = cluster;
      this.LOG = LOG;
    }

    boolean isRunning() {
      return isRunning.get();
    }

    @Override
    public void run() {
      try (RaftClient client = cluster.createClient()) {
        final CompletableFuture<Void> f = new CompletableFuture<>();
        for (int i = 0; i < messages.length; i++) {
          if (!useAsync) {
            final RaftClientReply reply =
                client.io().send(messages[step.getAndIncrement()]);
            Assert.assertTrue(reply.isSuccess());
          } else {
            final CompletableFuture<RaftClientReply> replyFuture =
                client.async().send(messages[i]);
            replyFuture.thenAcceptAsync(r -> {
              if (!r.isSuccess()) {
                f.completeExceptionally(
                    new AssertionError("Failed with reply: " + r));
              }
              if (step.incrementAndGet() == messages.length) {
                f.complete(null);
              }
              Assert.assertTrue(r.isSuccess());
            });
          }
        }
        if (useAsync) {
          f.join();
          Assert.assertTrue(step.get() == messages.length);
        }
      } catch(Exception t) {
        if (exceptionInClientThread.compareAndSet(null, t)) {
          LOG.error(this + " failed", t);
        } else {
          exceptionInClientThread.get().addSuppressed(t);
          LOG.error(this + " failed again!", t);
        }
      } finally {
        isRunning.set(false);
      }
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + index
          + "(step=" + step + "/" + messages.length
          + ", isRunning=" + isRunning
          + ", isAlive=" + isAlive()
          + ", exception=" + exceptionInClientThread
          + ")";
    }
  }

  @Test
  public void testWithLoad() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster -> testWithLoad(10, 300, false, cluster, LOG));
  }

  static void testWithLoad(final int numClients, final int numMessages,
      boolean useAsync, MiniRaftCluster cluster, Logger LOG) throws Exception {
    LOG.info("Running testWithLoad: numClients=" + numClients
        + ", numMessages=" + numMessages + ", async=" + useAsync);

    waitForLeader(cluster);

    final List<Client4TestWithLoad> clients
        = Stream.iterate(0, i -> i+1).limit(numClients)
        .map(i -> new Client4TestWithLoad(i, numMessages, useAsync, cluster, LOG))
        .collect(Collectors.toList());
    final AtomicInteger lastStep = new AtomicInteger();

    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      private int previousLastStep = lastStep.get();

      @Override
      public void run() {
        LOG.info(cluster.printServers());
        LOG.info(BlockRequestHandlingInjection.getInstance().toString());
        LOG.info(cluster.toString());
        clients.forEach(c -> LOG.info("  " + c));
        JavaUtils.dumpAllThreads(s -> LOG.info(s));

        final int last = lastStep.get();
        if (last != previousLastStep) {
          previousLastStep = last;
        } else {
          final RaftServer.Division leader = cluster.getLeader();
          LOG.info("NO PROGRESS at " + last + ", try to restart leader=" + leader);
          if (leader != null) {
            try {
              cluster.restartServer(leader.getId(), false);
              LOG.info("Restarted leader=" + leader);
            } catch (IOException e) {
              LOG.error("Failed to restart leader=" + leader);
            }
          }
        }
      }
    }, 5_000L, 10_000L);

    clients.forEach(Thread::start);

    int count = 0;
    for(;; ) {
      if (clients.stream().noneMatch(Client4TestWithLoad::isRunning)) {
        break;
      }

      final int n = clients.stream().mapToInt(c -> c.step.get()).sum();
      Assert.assertTrue(n >= lastStep.get());

      if (n - lastStep.get() < 50 * numClients) { // Change leader at least 50 steps.
        Thread.sleep(10);
        continue;
      }
      lastStep.set(n);
      count++;

      try {
        RaftServer.Division leader = cluster.getLeader();
        if (leader != null) {
          RaftTestUtil.changeLeader(cluster, leader.getId());
        }
      } catch (IllegalStateException e) {
        LOG.error("Failed to change leader ", e);
      }
    }
    LOG.info("Leader change count=" + count);
    timer.cancel();

    for(Client4TestWithLoad c : clients) {
      if (c.exceptionInClientThread.get() != null) {
        throw new AssertionError(c.exceptionInClientThread.get());
      }
      RaftTestUtil.assertLogEntries(cluster, c.messages);
    }
  }

  public static void testRequestTimeout(boolean async, MiniRaftCluster cluster, Logger LOG) throws Exception {
    waitForLeader(cluster);
    final Timestamp startTime = Timestamp.currentTime();
    try (final RaftClient client = cluster.createClient()) {
      // Get the next callId to be used by the client
      final ClientInvocationId invocationId = RaftClientTestUtil.getClientInvocationId(client);
      // Create an entry corresponding to the callId and clientId
      // in each server's retry cache.
      cluster.getServerAliveStream().forEach(
          raftServer -> RetryCacheTestUtil.getOrCreateEntry(raftServer, invocationId));
      // Client request for the callId now waits
      // as there is already a cache entry in the server for the request.
      // Ideally the client request should timeout and the client should retry.
      // The retry is successful when the retry cache entry for the corresponding callId and clientId expires.
      if (async) {
        CompletableFuture<RaftClientReply> replyFuture = client.async().send(new SimpleMessage("abc"));
        replyFuture.get();
      } else {
        client.io().send(new SimpleMessage("abc"));
      }
      // Eventually the request would be accepted by the server
      // when the retry cache entry is invalidated.
      // The duration for which the client waits should be more than the retryCacheExpiryDuration.
      final TimeDuration duration = startTime.elapsedTime();
      TimeDuration retryCacheExpiryDuration = RaftServerConfigKeys.RetryCache.expiryTime(cluster.getProperties());
      Assert.assertTrue(duration.compareTo(retryCacheExpiryDuration) >= 0);
    }
  }

  public static void testStateMachineMetrics(boolean async,
      MiniRaftCluster cluster, Logger LOG) throws Exception {
    RaftServer.Division leader = waitForLeader(cluster);
    try (final RaftClient client = cluster.createClient()) {
      Gauge appliedIndexGauge = getStatemachineGaugeWithName(leader,
          STATEMACHINE_APPLIED_INDEX_GAUGE);
      Gauge smAppliedIndexGauge = getStatemachineGaugeWithName(leader,
          STATEMACHINE_APPLY_COMPLETED_GAUGE);

      long appliedIndexBefore = (Long) appliedIndexGauge.getValue();
      long smAppliedIndexBefore = (Long) smAppliedIndexGauge.getValue();
      checkFollowerCommitLagsLeader(cluster);

      if (async) {
        CompletableFuture<RaftClientReply> replyFuture = client.async().send(new SimpleMessage("abc"));
        replyFuture.get();
      } else {
        client.io().send(new SimpleMessage("abc"));
      }

      long appliedIndexAfter = (Long) appliedIndexGauge.getValue();
      long smAppliedIndexAfter = (Long) smAppliedIndexGauge.getValue();
      checkFollowerCommitLagsLeader(cluster);

      Assert.assertTrue("StateMachine Applied Index not incremented",
          appliedIndexAfter > appliedIndexBefore);
      Assert.assertTrue("StateMachine Apply completed Index not incremented",
          smAppliedIndexAfter > smAppliedIndexBefore);
    }
  }

  private static void checkFollowerCommitLagsLeader(MiniRaftCluster cluster) {
    final List<RaftServer.Division> followers = cluster.getFollowers();
    final RaftGroupMemberId leader = cluster.getLeader().getMemberId();

    Gauge leaderCommitGauge = RaftServerMetricsImpl.getPeerCommitIndexGauge(leader, leader.getPeerId());

    for (RaftServer.Division f : followers) {
      final RaftGroupMemberId follower = f.getMemberId();
      Gauge followerCommitGauge = RaftServerMetricsImpl.getPeerCommitIndexGauge(leader, follower.getPeerId());
      Assert.assertTrue((Long)leaderCommitGauge.getValue() >=
          (Long)followerCommitGauge.getValue());
      Gauge followerMetric = RaftServerMetricsImpl.getPeerCommitIndexGauge(follower, follower.getPeerId());
      System.out.println(followerCommitGauge.getValue());
      System.out.println(followerMetric.getValue());
      Assert.assertTrue((Long)followerCommitGauge.getValue()  <= (Long)followerMetric.getValue());
    }
  }

  private static Gauge getStatemachineGaugeWithName(RaftServer.Division server, String gaugeName) {

    MetricRegistryInfo info = new MetricRegistryInfo(server.getMemberId().toString(),
        RATIS_APPLICATION_NAME_METRICS,
        RATIS_STATEMACHINE_METRICS, RATIS_STATEMACHINE_METRICS_DESC);

    Optional<RatisMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    Assert.assertTrue(metricRegistry.isPresent());
    RatisMetricRegistry ratisStateMachineMetricRegistry = metricRegistry.get();

    SortedMap<String, Gauge> gaugeMap =
        ratisStateMachineMetricRegistry.getGauges((s, metric) ->
            s.contains(gaugeName));

    return gaugeMap.get(gaugeMap.firstKey());
  }
}
