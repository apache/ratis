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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.PeerChanges;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.SnapshotSourceSelector;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.statemachine.RaftSnapshotBaseTest;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class InstallSnapshotNotificationTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static final Logger LOG = LoggerFactory.getLogger(InstallSnapshotNotificationTests.class);

  {
    Slf4jUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
  }

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        StateMachine4InstallSnapshotNotificationTests.class, StateMachine.class);
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(prop, false);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(
        prop, SNAPSHOT_TRIGGER_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, true);

    RaftServerConfigKeys.Log.setPurgeGap(prop, PURGE_GAP);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf(1024)); // 1k segment
    RaftServerConfigKeys.LeaderElection.setMemberMajorityAdd(prop, true);
  }

  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 64;
  private static final int PURGE_GAP = 8;
  private static final AtomicReference<SnapshotInfo> LEADER_SNAPSHOT_INFO_REF = new AtomicReference<>();

  private static final AtomicInteger numSnapshotRequests = new AtomicInteger();
  private static final AtomicInteger numNotifyInstallSnapshotFinished = new AtomicInteger();
  private static final AtomicInteger numFallbackToLeaderFromSourceFailure = new AtomicInteger();
  private static final AtomicReference<List<String>> LAST_SOURCE_PEER_IDS =
      new AtomicReference<>(Collections.emptyList());
  private static final AtomicLong LAST_MINIMUM_SNAPSHOT_INDEX = new AtomicLong(-1L);

  private enum SnapshotSourceFetchPolicy {
    LEADER_ONLY,
    FAIL_SOURCE_THEN_LEADER
  }

  private static final AtomicReference<SnapshotSourceFetchPolicy> SNAPSHOT_SOURCE_FETCH_POLICY =
      new AtomicReference<>(SnapshotSourceFetchPolicy.LEADER_ONLY);

  private static class StateMachine4InstallSnapshotNotificationTests extends SimpleStateMachine4Testing {

    private final Executor stateMachineExecutor = Executors.newSingleThreadExecutor();

    @Override
    public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
        RaftProtos.RoleInfoProto roleInfoProto,
        TermIndex termIndex) {
      return notifyInstallSnapshotFromLeader(roleInfoProto, termIndex, 0L, Collections.emptyList());
    }

    @Override
    public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
        RaftProtos.RoleInfoProto roleInfoProto, TermIndex termIndex,
        long minimumSnapshotIndex, List<RaftProtos.RaftPeerProto> sourcePeers) {
      if (!roleInfoProto.getFollowerInfo().hasLeaderInfo()) {
        return JavaUtils.completeExceptionally(new IOException("Failed " +
          "notifyInstallSnapshotFromLeader due to missing leader info"));
      }
      numSnapshotRequests.incrementAndGet();
      LAST_MINIMUM_SNAPSHOT_INDEX.set(minimumSnapshotIndex);
      LAST_SOURCE_PEER_IDS.set(sourcePeers.stream().map(p -> RaftPeerId.valueOf(p.getId()).toString())
          .collect(Collectors.toList()));

      final SingleFileSnapshotInfo leaderSnapshotInfo = (SingleFileSnapshotInfo) LEADER_SNAPSHOT_INFO_REF.get();
      LOG.info("{}: leaderSnapshotInfo = {}", getId(), leaderSnapshotInfo);
      if (leaderSnapshotInfo == null) {
        return super.notifyInstallSnapshotFromLeader(roleInfoProto, termIndex, minimumSnapshotIndex, sourcePeers);
      }

      Supplier<TermIndex> supplier = () -> {
        try {
          // Simulate a source failure before retrying with leader.
          // We still return success after falling back to leader so that the test can verify retry behavior.
          if (!sourcePeers.isEmpty()
              && SNAPSHOT_SOURCE_FETCH_POLICY.get() == SnapshotSourceFetchPolicy.FAIL_SOURCE_THEN_LEADER) {
            numFallbackToLeaderFromSourceFailure.incrementAndGet();
          }

          Path leaderSnapshotFile = leaderSnapshotInfo.getFile().getPath();
          final File followerSnapshotFilePath = new File(getStateMachineDir(),
              leaderSnapshotFile.getFileName().toString());
          // simulate the real situation such as snapshot transmission delay
          Thread.sleep(1000);
          if (followerSnapshotFilePath.exists()) {
            LOG.warn(followerSnapshotFilePath + " exists");
          } else {
            Files.copy(leaderSnapshotFile, followerSnapshotFilePath.toPath());
          }
        } catch (IOException | InterruptedException e) {
          LOG.error("Failed notifyInstallSnapshotFromLeader", e);
          return null;
        }
        return leaderSnapshotInfo.getTermIndex();
      };

      return CompletableFuture.supplyAsync(supplier, stateMachineExecutor);
    }

    @Override
    public void notifySnapshotInstalled(RaftProtos.InstallSnapshotResult result, long installIndex, RaftPeer peer) {
      if (result != RaftProtos.InstallSnapshotResult.SUCCESS &&
          result != RaftProtos.InstallSnapshotResult.SNAPSHOT_UNAVAILABLE) {
        return;
      }
      numNotifyInstallSnapshotFinished.incrementAndGet();
      final SingleFileSnapshotInfo leaderSnapshotInfo = (SingleFileSnapshotInfo) LEADER_SNAPSHOT_INFO_REF.get();
      File leaderSnapshotFile = leaderSnapshotInfo.getFile().getPath().toFile();
      synchronized (this) {
        try {
          if (getServer().get().getDivision(this.getGroupId()).getInfo().isLeader()) {
            LOG.info("Receive the notification to clean up snapshot as leader for {}, result: {}", peer, result);
            if (leaderSnapshotFile.exists()) {
              // For test purpose, we do not delete the leader's snapshot actually, which could be
              // sent to more than one peer during the test
              LOG.info("leader snapshot {} existed", leaderSnapshotFile);
            }
          } else {
            LOG.info("Receive the notification to clean up snapshot as follower for {}, result: {}", peer, result);
            final File followerSnapshotFile = new File(getStateMachineDir(), leaderSnapshotFile.getName());
            if (followerSnapshotFile.exists()) {
              FileUtils.deleteFile(followerSnapshotFile);
              LOG.info("follower snapshot {} deleted", followerSnapshotFile);
            }
          }
        } catch (Exception ex) {
          LOG.error("Failed to notify installSnapshot Finished", ex);
        }
      }
    }

  }

  private static class TestFollowerInfo implements FollowerInfo {
    private final String name;
    private final RaftPeer peer;
    private final long matchIndex;
    private final long commitIndex;
    private final Timestamp lastRpcResponseTime;
    private final Timestamp lastRespondedAppendEntriesSendTime;

    TestFollowerInfo(String peerId, long matchIndex, long commitIndex, long appendResponseAgeMs) {
      this.name = "test-" + peerId;
      this.peer = RaftPeer.newBuilder().setId(peerId).build();
      this.matchIndex = matchIndex;
      this.commitIndex = commitIndex;
      final Timestamp now = Timestamp.currentTime();
      this.lastRpcResponseTime = now;
      this.lastRespondedAppendEntriesSendTime = now.addTimeMs(-appendResponseAgeMs);
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public RaftPeerId getId() {
      return peer.getId();
    }

    @Override
    public RaftPeer getPeer() {
      return peer;
    }

    @Override
    public long getMatchIndex() {
      return matchIndex;
    }

    @Override
    public boolean updateMatchIndex(long newMatchIndex) {
      return false;
    }

    @Override
    public long getCommitIndex() {
      return commitIndex;
    }

    @Override
    public boolean updateCommitIndex(long newCommitIndex) {
      return false;
    }

    @Override
    public long getSnapshotIndex() {
      return 0L;
    }

    @Override
    public void setSnapshotIndex(long newSnapshotIndex) {
    }

    @Override
    public void setAttemptedToInstallSnapshot() {
    }

    @Override
    public boolean hasAttemptedToInstallSnapshot() {
      return true;
    }

    @Override
    public long getNextIndex() {
      return 0L;
    }

    @Override
    public void increaseNextIndex(long newNextIndex) {
    }

    @Override
    public void decreaseNextIndex(long newNextIndex) {
    }

    @Override
    public void setNextIndex(long newNextIndex) {
    }

    @Override
    public void updateNextIndex(long newNextIndex) {
    }

    @Override
    public void computeNextIndex(LongUnaryOperator op) {
    }

    @Override
    public Timestamp getLastRpcResponseTime() {
      return lastRpcResponseTime;
    }

    @Override
    public Timestamp getLastRpcSendTime() {
      return lastRpcResponseTime;
    }

    @Override
    public void updateLastRpcResponseTime() {
    }

    @Override
    public void updateLastRpcSendTime(boolean isHeartbeat) {
    }

    @Override
    public Timestamp getLastRpcTime() {
      return Timestamp.latest(getLastRpcResponseTime(), getLastRpcSendTime());
    }

    @Override
    public Timestamp getLastHeartbeatSendTime() {
      return lastRpcResponseTime;
    }

    @Override
    public Timestamp getLastRespondedAppendEntriesSendTime() {
      return lastRespondedAppendEntriesSendTime;
    }

    @Override
    public void updateLastRespondedAppendEntriesSendTime(Timestamp sendTime) {
    }
  }

  @Test
  public void testExactMatchSourceSelection() {
    final List<FollowerInfo> ranked = SnapshotSourceSelector.selectSourceFollowers(
        Arrays.asList(
            new TestFollowerInfo("exact", 200, 180, 10),
            new TestFollowerInfo("near", 199, 190, 20),
            new TestFollowerInfo("target", 205, 205, 5)),
        RaftPeerId.valueOf("target"),
        150,
        TermIndex.valueOf(5, 200));

    final List<String> ids = ranked.stream()
        .map(FollowerInfo::getId)
        .map(RaftPeerId::toString)
        .collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("exact", "near"), ids);
  }

  @Test
  public void testClosestFollowerSelectionByMatchIndexAndCommitIndex() {
    final List<FollowerInfo> ranked = SnapshotSourceSelector.selectSourceFollowers(
        Arrays.asList(
            new TestFollowerInfo("higherCommit", 320, 300, 20),
            new TestFollowerInfo("lowerCommit", 320, 200, 10),
            new TestFollowerInfo("lowerMatch", 319, 400, 5),
            new TestFollowerInfo("belowThreshold", 198, 198, 5)),
        RaftPeerId.valueOf("target"),
        200, // minimum acceptable match index is 199
        TermIndex.valueOf(8, 1000));

    final List<String> ids = ranked.stream()
        .map(FollowerInfo::getId)
        .map(RaftPeerId::toString)
        .collect(Collectors.toList());
    Assertions.assertEquals(Arrays.asList("higherCommit", "lowerCommit", "lowerMatch"), ids);
  }

  @Test
  public void testFollowerSourceFailureRetryWithLeader() throws Exception {
    final RaftProperties prop = getProperties();
    final boolean previousInstallSnapshotEnabled = RaftServerConfigKeys.Log.Appender.installSnapshotEnabled(prop);
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(prop, true);
    try {
      runWithNewCluster(2, this::testFollowerSourceFailureRetryWithLeader);
    } finally {
      SNAPSHOT_SOURCE_FETCH_POLICY.set(SnapshotSourceFetchPolicy.LEADER_ONLY);
      RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(prop, previousInstallSnapshotEnabled);
    }
  }

  private void testFollowerSourceFailureRetryWithLeader(CLUSTER cluster) throws Exception {
    LEADER_SNAPSHOT_INFO_REF.set(null);
    numSnapshotRequests.set(0);
    numFallbackToLeaderFromSourceFailure.set(0);
    LAST_SOURCE_PEER_IDS.set(Collections.emptyList());
    LAST_MINIMUM_SNAPSHOT_INDEX.set(-1L);
    SNAPSHOT_SOURCE_FETCH_POLICY.set(SnapshotSourceFetchPolicy.FAIL_SOURCE_THEN_LEADER);

    int i = 0;
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();

    try (final RaftClient client = cluster.createClient(leaderId)) {
      for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assertions.assertTrue(reply.isSuccess());
      }
    }

    final long nextIndex = leader.getRaftLog().getNextIndex();
    final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
        nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
    JavaUtils.attemptRepeatedly(() -> {
      Assertions.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
      return null;
    }, 10, ONE_SECOND, "snapshotFile.exist", LOG);

    final SnapshotInfo leaderSnapshotInfo = leader.getStateMachine().getLatestSnapshot();
    Assertions.assertNotNull(leaderSnapshotInfo);
    Assertions.assertTrue(LEADER_SNAPSHOT_INFO_REF.compareAndSet(null, leaderSnapshotInfo));

    final PeerChanges change = cluster.addNewPeers(1, true);
    RaftServerTestUtil.runWithMinorityPeers(cluster, change.getPeersInNewConf(), cluster::setConfiguration);
    RaftServerTestUtil.waitAndCheckNewConf(cluster, change.getPeersInNewConf(), 0, null);

    final RaftPeerId targetFollowerId = change.getAddedPeers().get(0).getId();
    final RaftServer.Division targetFollower = cluster.getDivision(targetFollowerId);
    JavaUtils.attempt(() -> {
      Assertions.assertEquals(leaderSnapshotInfo.getIndex(),
          RaftServerTestUtil.getLatestInstalledSnapshotIndex(targetFollower));
    }, 10, ONE_SECOND, "targetFollowerInstalledSnapshot", LOG);

    // The notification contained follower sources and the state machine retried with leader after source failure.
    JavaUtils.attempt(() -> {
      Assertions.assertTrue(numSnapshotRequests.get() >= 1);
      Assertions.assertTrue(numFallbackToLeaderFromSourceFailure.get() >= 1);
      Assertions.assertFalse(LAST_SOURCE_PEER_IDS.get().isEmpty());
      Assertions.assertTrue(LAST_MINIMUM_SNAPSHOT_INDEX.get() >= 0);
    }, 10, ONE_SECOND, "sourceFailureRetryWithLeader", LOG);
  }

  @Test
  public void testNoCandidateFallbackToLeader() throws Exception {
    final RaftProperties prop = getProperties();
    final boolean previousInstallSnapshotEnabled = RaftServerConfigKeys.Log.Appender.installSnapshotEnabled(prop);
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(prop, true);
    try {
      runWithNewCluster(1, this::testNoCandidateFallbackToLeader);
    } finally {
      SNAPSHOT_SOURCE_FETCH_POLICY.set(SnapshotSourceFetchPolicy.LEADER_ONLY);
      RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(prop, previousInstallSnapshotEnabled);
    }
  }

  private void testNoCandidateFallbackToLeader(CLUSTER cluster) throws Exception {
    LEADER_SNAPSHOT_INFO_REF.set(null);
    numSnapshotRequests.set(0);
    numFallbackToLeaderFromSourceFailure.set(0);
    LAST_SOURCE_PEER_IDS.set(Collections.emptyList());
    LAST_MINIMUM_SNAPSHOT_INDEX.set(-1L);
    SNAPSHOT_SOURCE_FETCH_POLICY.set(SnapshotSourceFetchPolicy.FAIL_SOURCE_THEN_LEADER);

    int i = 0;
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();

    try (final RaftClient client = cluster.createClient(leaderId)) {
      for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assertions.assertTrue(reply.isSuccess());
      }
    }

    final long nextIndex = leader.getRaftLog().getNextIndex();
    final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
        nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
    JavaUtils.attemptRepeatedly(() -> {
      Assertions.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
      return null;
    }, 10, ONE_SECOND, "snapshotFile.exist", LOG);

    final SnapshotInfo leaderSnapshotInfo = leader.getStateMachine().getLatestSnapshot();
    Assertions.assertNotNull(leaderSnapshotInfo);
    Assertions.assertTrue(LEADER_SNAPSHOT_INFO_REF.compareAndSet(null, leaderSnapshotInfo));

    final PeerChanges change = cluster.addNewPeers(1, true);
    RaftServerTestUtil.runWithMinorityPeers(cluster, change.getPeersInNewConf(), cluster::setConfiguration);
    RaftServerTestUtil.waitAndCheckNewConf(cluster, change.getPeersInNewConf(), 0, null);

    final RaftPeerId targetFollowerId = change.getAddedPeers().get(0).getId();
    final RaftServer.Division targetFollower = cluster.getDivision(targetFollowerId);
    JavaUtils.attempt(() -> {
      Assertions.assertEquals(leaderSnapshotInfo.getIndex(),
          RaftServerTestUtil.getLatestInstalledSnapshotIndex(targetFollower));
    }, 10, ONE_SECOND, "leaderFallbackInstalledSnapshot", LOG);

    // There is no follower source candidate in a single-leader cluster. The leader should directly install snapshot.
    Assertions.assertEquals(0, numSnapshotRequests.get());
    Assertions.assertEquals(0, numFallbackToLeaderFromSourceFailure.get());
    Assertions.assertEquals(Collections.emptyList(), LAST_SOURCE_PEER_IDS.get());
  }

  /**
   * Basic test for install snapshot notification: start a one node cluster
   * (disable install snapshot option) and let it generate a snapshot. Then
   * delete the log and restart the node, and add more nodes as followers.
   * The new follower nodes should get a install snapshot notification.
   */
  @Test
  public void testAddNewFollowers() throws Exception {
    final int numRequests = SNAPSHOT_TRIGGER_THRESHOLD*2 - 1; // trigger a snapshot
    runWithNewCluster(1, c -> testAddNewFollowers(c, numRequests));
  }

  @Test
  public void testAddNewFollowersNoSnapshot() throws Exception {
    final int numRequests = SNAPSHOT_TRIGGER_THRESHOLD/8;  // do not trigger a snapshot;
    runWithNewCluster(1, c -> testAddNewFollowers(c, numRequests));
  }

  private void testAddNewFollowers(CLUSTER cluster, int numRequests) throws Exception {
    final boolean shouldInstallSnapshot = numRequests >= SNAPSHOT_TRIGGER_THRESHOLD;
    LEADER_SNAPSHOT_INFO_REF.set(null);
    final List<LogSegmentPath> logs;
    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < numRequests; i++) {
          final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assertions.assertTrue(reply.isSuccess());
        }
      }

      if (shouldInstallSnapshot) {
        // wait for the snapshot to be done
        final RaftServer.Division leader = cluster.getLeader();
        final long nextIndex = leader.getRaftLog().getNextIndex();
        LOG.info("nextIndex = {}", nextIndex);
        final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
            nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
        JavaUtils.attemptRepeatedly(() -> {
          Assertions.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
          return null;
        }, 10, ONE_SECOND, "snapshotFile.exist", LOG);
        logs = LogSegmentPath.getLogSegmentPaths(leader.getRaftStorage());
      } else {
        logs = Collections.emptyList();
      }
    } finally {
      cluster.shutdown();
    }

    // delete the log segments from the leader
    LOG.info("Delete logs {}", logs);
    for (LogSegmentPath path : logs) {
      FileUtils.deleteFully(path.getPath()); // the log may be already puged
    }

    // restart the peer
    LOG.info("Restarting the cluster");
    cluster.restart(false);
    try {
      RaftSnapshotBaseTest.assertLeaderContent(cluster);

      // generate some more traffic
      try(final RaftClient client = cluster.createClient(cluster.getLeader().getId())) {
        Assertions.assertTrue(client.io().send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
      }

      final SnapshotInfo leaderSnapshotInfo = cluster.getLeader().getStateMachine().getLatestSnapshot();
      LOG.info("LeaderSnapshotInfo: {}", leaderSnapshotInfo.getTermIndex());
      final boolean set = LEADER_SNAPSHOT_INFO_REF.compareAndSet(null, leaderSnapshotInfo);
      Assertions.assertTrue(set);

      // Add new peer(s)
      final PeerChanges change = cluster.addNewPeers(1, true);
      // trigger setConfiguration
      RaftServerTestUtil.runWithMinorityPeers(cluster, change.getPeersInNewConf(), cluster::setConfiguration);

      RaftServerTestUtil.waitAndCheckNewConf(cluster, change.getPeersInNewConf(), 0, null);

      // Check the installed snapshot index on each Follower matches with the
      // leader snapshot.
      for (RaftServer.Division follower : cluster.getFollowers()) {
        final long expected = leaderSnapshotInfo.getIndex();
        Assertions.assertEquals(expected, RaftServerTestUtil.getLatestInstalledSnapshotIndex(follower));
        RaftSnapshotBaseTest.assertLogContent(follower, false);
      }

      // restart the peer and check if it can correctly handle conf change
      cluster.restartServer(cluster.getLeader().getId(), false);
      RaftSnapshotBaseTest.assertLeaderContent(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testRestartFollower() throws Exception {
    runWithNewCluster(3, this::testRestartFollower);
  }

  private void testRestartFollower(CLUSTER cluster) throws Exception {
    LEADER_SNAPSHOT_INFO_REF.set(null);
    int i = 0;
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();

    try (final RaftClient client = cluster.createClient(leaderId)) {
      for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assertions.assertTrue(reply.isSuccess());
      }
    }

    // wait for the snapshot to be done
    final long oldLeaderNextIndex = leader.getRaftLog().getNextIndex();
    {
      LOG.info("{}: oldLeaderNextIndex = {}", leaderId, oldLeaderNextIndex);
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          oldLeaderNextIndex - SNAPSHOT_TRIGGER_THRESHOLD, oldLeaderNextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assertions.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
        return null;
      }, 10, ONE_SECOND, "snapshotFile.exist", LOG);
    }

    final RaftPeerId followerId = cluster.getFollowers().get(0).getId();
    cluster.killServer(followerId);

    // generate some more traffic
    try (final RaftClient client = cluster.createClient(leader.getId())) {
      Assertions.assertTrue(client.io().send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
    }

    FIVE_SECONDS.sleep();
    cluster.restartServer(followerId, false);
    final RaftServer.Division follower = cluster.getDivision(followerId);
    JavaUtils.attempt(() -> {
      final long newLeaderNextIndex = leader.getRaftLog().getNextIndex();
      LOG.info("{}: newLeaderNextIndex = {}", leaderId, newLeaderNextIndex);
      Assertions.assertTrue(newLeaderNextIndex > oldLeaderNextIndex);
      Assertions.assertEquals(newLeaderNextIndex, follower.getRaftLog().getNextIndex());
    }, 10, ONE_SECOND, "followerNextIndex", LOG);
  }

  @Test
  public void testInstallSnapshotNotificationCount() throws Exception {
    runWithNewCluster(3, this::testInstallSnapshotNotificationCount);
  }


  private void testInstallSnapshotNotificationCount(CLUSTER cluster) throws Exception {
    LEADER_SNAPSHOT_INFO_REF.set(null);
    numSnapshotRequests.set(0);

    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      // Let a few heartbeats pass.
      ONE_SECOND.sleep();
      Assertions.assertEquals(0, numSnapshotRequests.get());

      // Generate data.
      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < 10; i++) {
          RaftClientReply
              reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assertions.assertTrue(reply.isSuccess());
        }
      }

      // Wait until index has been updated
      RaftTestUtil.waitFor(
              () -> cluster.getLeader().getStateMachine().getLastAppliedTermIndex().getIndex() == 20,
               300, 15000);

      // Take snapshot and check result.
      long snapshotIndex = cluster.getLeader().getStateMachine().takeSnapshot();
      Assertions.assertEquals(20, snapshotIndex);
      final SnapshotInfo leaderSnapshotInfo = cluster.getLeader().getStateMachine().getLatestSnapshot();
      Assertions.assertEquals(20, leaderSnapshotInfo.getIndex());
      final boolean set = LEADER_SNAPSHOT_INFO_REF.compareAndSet(null, leaderSnapshotInfo);
      Assertions.assertTrue(set);

      // Wait for the snapshot to be done.
      final RaftServer.Division leader = cluster.getLeader();
      final long nextIndex = leader.getRaftLog().getNextIndex();
      Assertions.assertEquals(21, nextIndex);
      // End index is exclusive.
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          0, nextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assertions.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
        return null;
      }, 10, ONE_SECOND, "snapshotFile.exist", LOG);

      // Clear all log files and reset cached log start index.
      long snapshotInstallIndex =
          leader.getRaftLog().onSnapshotInstalled(leader.getRaftLog().getLastCommittedIndex()).get();
      Assertions.assertEquals(20, snapshotInstallIndex);

      // Check that logs are gone.
      Assertions.assertEquals(0,
          LogSegmentPath.getLogSegmentPaths(leader.getRaftStorage()).size());
      Assertions.assertEquals(RaftLog.INVALID_LOG_INDEX, leader.getRaftLog().getStartIndex());

      // Allow some heartbeats to go through, then make sure none of them had
      // snapshot requests.
      ONE_SECOND.sleep();
      Assertions.assertEquals(0, numSnapshotRequests.get());

      // Make sure leader and followers are still up to date.
      for (RaftServer.Division follower : cluster.getFollowers()) {
        Assertions.assertEquals(
            leader.getRaftLog().getNextIndex(),
            follower.getRaftLog().getNextIndex());
      }

      // Add new peer(s) who will need snapshots from the leader.
      final int numNewPeers = 1;
      final PeerChanges change = cluster.addNewPeers(numNewPeers, true);
      // trigger setConfiguration
      RaftServerTestUtil.runWithMinorityPeers(cluster, change.getPeersInNewConf(), cluster::setConfiguration);
      RaftServerTestUtil.waitAndCheckNewConf(cluster, change.getPeersInNewConf(), 0, null);

      // Generate more data.
      try (final RaftClient client = cluster.createClient(leader.getId())) {
        Assertions.assertTrue(client.io().send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
      }

      // Make sure leader and followers are still up to date.
      for (RaftServer.Division follower : cluster.getFollowers()) {
        // Give follower slightly time to catch up
        RaftTestUtil.waitFor(
                () -> leader.getRaftLog().getNextIndex() == follower.getRaftLog().getNextIndex(),
                300, 15000);
      }

      // Make sure each new peer got one snapshot notification.
      Assertions.assertEquals(numNewPeers, numSnapshotRequests.get());

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testInstallSnapshotInstalledEvent() throws Exception{
    runWithNewCluster(1, this::testInstallSnapshotInstalledEvent);
  }

  private void testInstallSnapshotInstalledEvent(CLUSTER cluster) throws Exception{
    LEADER_SNAPSHOT_INFO_REF.set(null);
    numNotifyInstallSnapshotFinished.set(0);
    final List<LogSegmentPath> logs;
    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply
              reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assertions.assertTrue(reply.isSuccess());
        }
      }

      // wait for the snapshot to be done
      final RaftServer.Division leader = cluster.getLeader();
      final long nextIndex = leader.getRaftLog().getNextIndex();
      LOG.info("nextIndex = {}", nextIndex);
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assertions.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
        return null;
      }, 10, ONE_SECOND, "snapshotFile.exist", LOG);
      logs = LogSegmentPath.getLogSegmentPaths(leader.getRaftStorage());
    } finally {
      cluster.shutdown();
    }

    // delete the log segments from the leader
    LOG.info("Delete logs {}", logs);
    for (LogSegmentPath path : logs) {
      FileUtils.deleteFully(path.getPath()); // the log may be already puged
    }

    // restart the peer
    LOG.info("Restarting the cluster");
    cluster.restart(false);
    try {
      RaftSnapshotBaseTest.assertLeaderContent(cluster);

      // generate some more traffic
      try(final RaftClient client = cluster.createClient(cluster.getLeader().getId())) {
        Assertions.assertTrue(client.io().send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
      }

      final SnapshotInfo leaderSnapshotInfo = cluster.getLeader().getStateMachine().getLatestSnapshot();
      LOG.info("LeaderSnapshotInfo: {}", leaderSnapshotInfo.getTermIndex());
      final boolean set = LEADER_SNAPSHOT_INFO_REF.compareAndSet(null, leaderSnapshotInfo);
      Assertions.assertTrue(set);

      // add one new peer
      final PeerChanges change = cluster.addNewPeers(1, true);
      // trigger setConfiguration
      RaftServerTestUtil.runWithMinorityPeers(cluster, change.getPeersInNewConf(), cluster::setConfiguration);
      RaftServerTestUtil.waitAndCheckNewConf(cluster, change.getPeersInNewConf(), 0, null);

      // Check the installed snapshot index on each Follower matches with the
      // leader snapshot.
      for (RaftServer.Division follower : cluster.getFollowers()) {
        Assertions.assertEquals(leaderSnapshotInfo.getIndex(),
            RaftServerTestUtil.getLatestInstalledSnapshotIndex(follower));
      }

      // notification should be sent to both the leader and the follower
      File leaderSnapshotFile = leaderSnapshotInfo.getFiles().get(0).getPath().toFile();
      SimpleStateMachine4Testing followerStateMachine =
          (SimpleStateMachine4Testing) cluster.getFollowers().get(0).getStateMachine();
      final File followerSnapshotFile = new File(followerStateMachine.getStateMachineDir(),
          leaderSnapshotFile.getName());
      Assertions.assertEquals(numNotifyInstallSnapshotFinished.get(), 2);
      Assertions.assertTrue(leaderSnapshotFile.exists());
      Assertions.assertFalse(followerSnapshotFile.exists());

      // restart the peer and check if it can correctly handle conf change
      cluster.restartServer(cluster.getLeader().getId(), false);
      RaftSnapshotBaseTest.assertLeaderContent(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test for install snapshot during a peer bootstrap: start a one node cluster
   * (disable install snapshot option) and let it generate a snapshot. Add
   * another node and verify that the new node receives a install snapshot
   * notification.
   */
  @Test
  public void testInstallSnapshotDuringBootstrap() throws Exception {
    runWithNewCluster(1, this::testInstallSnapshotDuringBootstrap);
  }

  private void testInstallSnapshotDuringBootstrap(CLUSTER cluster) throws Exception {
    LEADER_SNAPSHOT_INFO_REF.set(null);
    numSnapshotRequests.set(0);
    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply
              reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assertions.assertTrue(reply.isSuccess());
        }
      }

      // wait for the snapshot to be done
      final RaftServer.Division leader = cluster.getLeader();
      final long nextIndex = leader.getRaftLog().getNextIndex();
      LOG.info("nextIndex = {}", nextIndex);
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assertions.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
        return null;
      }, 10, ONE_SECOND, "snapshotFile.exist", LOG);

      RaftSnapshotBaseTest.assertLeaderContent(cluster);

      final SnapshotInfo leaderSnapshotInfo = cluster.getLeader().getStateMachine().getLatestSnapshot();
      final boolean set = LEADER_SNAPSHOT_INFO_REF.compareAndSet(null, leaderSnapshotInfo);
      Assertions.assertTrue(set);

      // Add new peer(s)
      final int numNewPeers = 1;
      final PeerChanges change = cluster.addNewPeers(numNewPeers, true);
      // trigger setConfiguration
      RaftServerTestUtil.runWithMinorityPeers(cluster, change.getPeersInNewConf(), cluster::setConfiguration);

      RaftServerTestUtil.waitAndCheckNewConf(cluster, change.getPeersInNewConf(), 0, null);

      // Check the installed snapshot index on each Follower matches with the
      // leader snapshot.
      for (RaftServer.Division follower : cluster.getFollowers()) {
        Assertions.assertEquals(leaderSnapshotInfo.getIndex(),
            RaftServerTestUtil.getLatestInstalledSnapshotIndex(follower));
      }

      // Make sure each new peer got at least one snapshot notification.
      Assertions.assertTrue(numNewPeers <= numSnapshotRequests.get());
    } finally {
      cluster.shutdown();
    }
  }
}
