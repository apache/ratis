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

import static org.apache.ratis.RaftTestUtil.waitForLeader;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.impl.RatisMetricRegistryImpl;
import org.apache.ratis.metrics.impl.DefaultTimekeeperImpl;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto.LogEntryBodyCase;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.ratis.thirdparty.com.codahale.metrics.Gauge;
import org.slf4j.event.Level;

public abstract class LogAppenderTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Slf4jUtils.setLogLevel(LogAppender.LOG, Level.DEBUG);
  }

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SimpleStateMachine4Testing.class, StateMachine.class);

    final SizeInBytes n = SizeInBytes.valueOf("8KB");
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, n);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(prop, n);
  }

  static SimpleMessage[] generateMsgs(int num) {
    SimpleMessage[] msgs = new SimpleMessage[num * 6];
    for (int i = 0; i < num; i++) {
      for (int j = 0; j < 6; j++) {
        byte[] bytes = new byte[1024 * (j + 1)];
        Arrays.fill(bytes, (byte) (j + '0'));
        msgs[i * 6 + j] = new SimpleMessage(new String(bytes));
      }
    }
    return msgs;
  }

  private static class Sender extends Thread {
    private final RaftClient client;
    private final CountDownLatch latch;
    private final SimpleMessage[] messages;
    private final AtomicBoolean succeed = new AtomicBoolean(false);
    private final AtomicReference<Exception> exception = new AtomicReference<>();

    Sender(RaftClient client, int numMessages, CountDownLatch latch) {
      this.latch = latch;
      this.client = client;
      this.messages = generateMsgs(numMessages);
    }

    @Override
    public void run() {
      try {
        latch.await();
        for (SimpleMessage msg : messages) {
          client.io().send(msg);
        }
        client.close();
        succeed.set(true);
      } catch (Exception e) {
        exception.compareAndSet(null, e);
      }
    }
  }

  @Test
  public void testSingleElementBuffer() throws Exception {
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(getProperties(), 1);
    runWithNewCluster(3, this::runTest);
  }

  @Test
  public void testUnlimitedElementBuffer() throws Exception {
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(getProperties(), 0);
    runWithNewCluster(3, this::runTest);
  }

  @Test
  public void testFollowerHeartbeatMetric() throws Exception {
    final MiniRaftCluster cluster = newCluster(3);
    final Map<RaftPeerId, RatisMetricRegistryImpl> registries = new HashMap<>();
    try {
      cluster.start();
      final RaftPeerId originalLeader = waitForLeader(cluster).getId();
      for (RaftServer.Division server : cluster.iterateDivisions()) {
        registries.put(server.getId(), (RatisMetricRegistryImpl)
            ((RaftServerMetricsImpl) server.getRaftServerMetrics()).getRegistry());
      }

      // Check initial leadership, step-down, and re-election without restarting the servers.
      for (int round = 0; round < 3; round++) {
        final RaftPeerId leaderId = waitForLeader(cluster).getId();
        try (RaftClient client = cluster.createClient(leaderId)) {
          for (int i = 0; i < 10; i++) {
            RaftTestUtil.assertSuccessReply(
                client.io().send(new SimpleMessage("heartbeat metrics round " + round + " message " + i)));
          }
          JavaUtils.attempt(() -> assertFollowerHeartbeatMetrics(cluster, registries),
              100, HUNDRED_MILLIS, "check heartbeat metrics", LOG);
          if (round < 2) {
            final RatisMetricRegistryImpl previousLeaderRegistry = registries.get(leaderId);
            final SortedMap<String, Gauge> commitIndexGauges = previousLeaderRegistry.getGauges(
                (s, m) -> s.endsWith("_peerCommitIndex"));
            assertEquals(3, commitIndexGauges.size());
            final RaftPeerId nextLeader = round == 0 ? cluster.getFollowers().get(0).getId() : originalLeader;
            assertTrue(client.admin().transferLeadership(nextLeader, 20_000).isSuccess());
            assertEquals(nextLeader, waitForLeader(cluster).getId());
            // Commit-index gauges read the server cache and remain useful after step-down.
            final SortedMap<String, Gauge> retainedGauges = previousLeaderRegistry.getGauges(
                (s, m) -> s.endsWith("_peerCommitIndex"));
            commitIndexGauges.forEach((name, gauge) -> assertSame(gauge, retainedGauges.get(name)));
          }
        }
      }
    } finally {
      cluster.shutdown();
    }
    registries.values().forEach(registry -> assertTrue(
        !MetricRegistries.global().get(registry.getMetricRegistryInfo()).isPresent(),
        "Server registry should be unregistered after shutdown"));
  }

  private void assertFollowerHeartbeatMetrics(MiniRaftCluster cluster,
      Map<RaftPeerId, RatisMetricRegistryImpl> registries) {
    final RaftServer.Division leader = cluster.getLeader();
    assertNotNull(leader);
    final RatisMetricRegistryImpl leaderRegistry = registries.get(leader.getId());
    final SortedMap<String, Gauge> heartbeatGauges = leaderRegistry.getGauges((s, m) ->
        s.contains("lastHeartbeatElapsedTime"));
    assertEquals(2, heartbeatGauges.size());

    for (RaftServer.Division server : cluster.iterateDivisions()) {
      final RatisMetricRegistryImpl registry = registries.get(server.getId());
      assertSame(registry, MetricRegistries.global().get(registry.getMetricRegistryInfo())
          .orElseThrow(() -> new AssertionError("Missing server registry for " + server.getId())));
      assertTrue(!registry.getGauges((s, m) -> s.endsWith(server.getId() + "_peerCommitIndex")).isEmpty());
      assertEquals(5, registry.getGauges((s, m) -> s.contains("retryCache")).size());

      if (server.getId().equals(leader.getId())) {
        continue;
      }
      final Gauge<?> heartbeat = heartbeatGauges.entrySet().stream()
          .filter(e -> e.getKey().endsWith(server.getId() + "_lastHeartbeatElapsedTime"))
          .findFirst().orElseThrow(() -> new AssertionError("Missing heartbeat gauge for " + server.getId()))
          .getValue();
      assertTrue((Long) heartbeat.getValue() > 0);
      assertTrue(registry.getGauges((s, m) -> s.contains("lastHeartbeatElapsedTime")).isEmpty());
      assertTrue(registry.getGauges((s, m) -> s.endsWith("numPendingRequestInQueue")
          || s.endsWith("numPendingRequestMegaByteSize") || s.matches(".*numWatch.*RequestInQueue")).isEmpty());
      final RaftServerMetricsImpl metrics = (RaftServerMetricsImpl) server.getRaftServerMetrics();
      for (boolean isHeartbeat : new boolean[] {true, false}) {
        final DefaultTimekeeperImpl timer = (DefaultTimekeeperImpl) metrics.getFollowerAppendEntryTimer(isHeartbeat);
        assertTrue(timer.getTimer().getMeanRate() > 0.0d);
        assertTrue(timer.getTimer().getCount() > 0L);
      }
    }
  }

  void runTest(CLUSTER cluster) throws Exception {
    final int numMsgs = 10;
    final int numClients = 5;
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();
    List<RaftClient> clients = new ArrayList<>();

    try {
      List<Sender> senders = new ArrayList<>();

      // start several clients and write concurrently
      final CountDownLatch latch = new CountDownLatch(1);

      for (int i = 0; i < numClients; i ++) {
        RaftClient client = cluster.createClient(leaderId);
        clients.add(client);
        senders.add(new Sender(client, numMsgs, latch));
      }

      senders.forEach(Thread::start);

      latch.countDown();

      for (Sender s : senders) {
        s.join();
        final Exception e = s.exception.get();
        if (e != null) {
          throw e;
        }
        Assertions.assertTrue(s.succeed.get());
      }
    } finally {
      for (int i = 0; i < clients.size(); i ++) {
        try {
          clients.get(i).close();
        } catch (Exception ignored) {
          LOG.warn("{} is ignored", JavaUtils.getClassSimpleName(ignored.getClass()), ignored);
        }
      }
    }

    final RaftServer.Division leader = cluster.getLeader();
    final RaftLog leaderLog = cluster.getLeader().getRaftLog();
    final EnumMap<LogEntryBodyCase, AtomicLong> counts = RaftTestUtil.countEntries(leaderLog);
    LOG.info("counts = " + counts);
    Assertions.assertEquals(6 * numMsgs * numClients, counts.get(LogEntryBodyCase.STATEMACHINELOGENTRY).get());

    final LogEntryProto last = RaftTestUtil.getLastEntry(LogEntryBodyCase.STATEMACHINELOGENTRY, leaderLog);
    LOG.info("last = {}", LogProtoUtils.toLogEntryString(last));
    Assertions.assertNotNull(last);
    Assertions.assertTrue(last.getIndex() <= leader.getInfo().getLastAppliedIndex());
  }

  @Test
  public void testNewAppendEntriesRequestAfterPurgeFollowerBehindStartIndex() throws Exception {
    final RaftProperties prop = getProperties();
    RaftServerConfigKeys.Log.setPurgeGap(prop, 1);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("1KB"));
    // Test when followerNextIndex < leader's logStartIndex.
    runWithNewCluster(3, cluster -> runTestNewAppendEntriesRequestAfterPurge(cluster, true));
  }

  @Test
  public void testNewAppendEntriesRequestAfterPurgeFollowerAtStartIndex() throws Exception {
    final RaftProperties prop = getProperties();
    RaftServerConfigKeys.Log.setPurgeGap(prop, 1);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("1KB"));
    // Test when followerNextIndex == leader's logStartIndex, but the previous index is already purged.
    runWithNewCluster(3, cluster -> runTestNewAppendEntriesRequestAfterPurge(cluster, false));
  }

  private void runTestNewAppendEntriesRequestAfterPurge(CLUSTER cluster,
      boolean followerBehindStartIndex) throws Exception {
    final int maxAttempts = 3;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try (RaftClient client = cluster.createClient(waitForLeader(cluster).getId())) {
        for (SimpleMessage msg : generateMsgs(5)) {
          client.io().send(msg);
        }
      }

      // Sending messages may change the leader. Use the same leader and term for purge and verification.
      final RaftServer.Division leader = waitForLeader(cluster);
      final long term = leader.getInfo().getCurrentTerm();
      try {
        final long startIndexAfterPurge = setupPurgedLeaderLog(leader);
        // Verify only if the node is still the leader in the term used for purge.
        if (isLeaderInTerm(leader, term)) {
          runTestNewAppendEntriesRequestAfterPurge(leader,
              followerBehindStartIndex ? startIndexAfterPurge - 1 : startIndexAfterPurge);
          // Leadership may change during verification; accept the result only if it is still valid.
          if (isLeaderInTerm(leader, term)) {
            return;
          }
        }
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception | AssertionError e) {
        if (isLeaderInTerm(leader, term)) {
          throw e;
        }
        LOG.info("Leader {} changed during purge verification in term {}", leader.getId(), term, e);
      }
      LOG.info("Leader {} changed in term {} during purge test (attempt {}/{})",
          leader.getId(), term, attempt, maxAttempts);
    }
    Assertions.fail("Leader changed during all " + maxAttempts + " purge test attempts");
  }

  // Check both role and term: the same server may step down and become leader again in a later term.
  private static boolean isLeaderInTerm(RaftServer.Division leader, long term) {
    return leader.getInfo().isLeader() && leader.getInfo().getCurrentTerm() == term;
  }

  private long setupPurgedLeaderLog(RaftServer.Division leader) throws Exception {
    final RaftLog leaderLog = leader.getRaftLog();
    final long lastLogIndex = leaderLog.getLastEntryTermIndex().getIndex();
    LOG.info("Leader log lastIndex={}, startIndex={}", lastLogIndex, leaderLog.getStartIndex());
    Assertions.assertTrue(lastLogIndex > 5, "Need enough log entries for the test");

    // Take a snapshot so that shouldInstallSnapshot() can return it
    final long snapshotIndex = SimpleStateMachine4Testing.get(leader).takeSnapshot();
    LOG.info("Snapshot taken at index {}", snapshotIndex);
    Assertions.assertTrue(snapshotIndex > 0, "Snapshot should have been taken");

    final long purgeUpTo = lastLogIndex - 2;
    LOG.info("Purging leader log up to index {}", purgeUpTo);
    leaderLog.purge(purgeUpTo).get();

    final long startIndexAfterPurge = leaderLog.getStartIndex();
    LOG.info("Leader log after purge: startIndex={}", startIndexAfterPurge);
    Assertions.assertTrue(startIndexAfterPurge > 1,
        "Purge should have advanced startIndex, but got " + startIndexAfterPurge);

    return startIndexAfterPurge;
  }

  void runTestNewAppendEntriesRequestAfterPurge(RaftServer.Division leader,
      long targetNextIndex) throws Exception {
    final RaftLog leaderLog = leader.getRaftLog();
    final long startIndexAfterPurge = leaderLog.getStartIndex();

    final Stream<LogAppender> appenders = RaftServerTestUtil.getLogAppenders(leader);
    Assertions.assertNotNull(appenders, "Leader should have log appenders");
    final LogAppender appender = appenders.findFirst().orElseThrow(
        () -> new AssertionError("No log appender found"));

    Assertions.assertTrue(targetNextIndex > RaftLog.LEAST_VALID_LOG_INDEX,
        "targetNextIndex should be > LEAST_VALID_LOG_INDEX");
    appender.getFollower().setNextIndex(targetNextIndex);

    LOG.info("Set follower nextIndex={}, startIndexAfterPurge={}, snapshotIndex={}",
        targetNextIndex, startIndexAfterPurge, appender.getFollower().getSnapshotIndex());
    Assertions.assertEquals(0, appender.getFollower().getSnapshotIndex(),
        "Follower snapshotIndex should be 0 (default, never installed snapshot)");

    Assertions.assertNull(leaderLog.getTermIndex(targetNextIndex - 1),
        "Entry at previousIndex=" + (targetNextIndex - 1) + " should have been purged");

    // Should return null instead of throwing NPE
    Assertions.assertNull(appender.newAppendEntriesRequest(0, false),
        "newAppendEntriesRequest should return null when previous TermIndex is not found");

    Assertions.assertEquals(targetNextIndex, appender.getFollower().getNextIndex(),
        "Follower nextIndex should remain unchanged");

    Assertions.assertNotNull(appender.shouldInstallSnapshot(),
        "shouldInstallSnapshot should return non-null when followerNextIndex ("
            + targetNextIndex + ") and previous entry has been purged");
  }
}
