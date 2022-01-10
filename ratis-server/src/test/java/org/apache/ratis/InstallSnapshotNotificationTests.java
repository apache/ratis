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

import org.apache.log4j.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.statemachine.RaftSnapshotBaseTest;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public abstract class InstallSnapshotNotificationTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static final Logger LOG = LoggerFactory.getLogger(InstallSnapshotNotificationTests.class);

  {
    Log4jUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
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
  }

  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 64;
  private static final int PURGE_GAP = 8;
  private static final AtomicReference<SnapshotInfo> leaderSnapshotInfoRef = new AtomicReference<>();

  private static final AtomicInteger numSnapshotRequests = new AtomicInteger();

  private static class StateMachine4InstallSnapshotNotificationTests extends SimpleStateMachine4Testing {
    @Override
    public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
        RaftProtos.RoleInfoProto roleInfoProto,
        TermIndex termIndex) {
      if (!roleInfoProto.getFollowerInfo().hasLeaderInfo()) {
        return JavaUtils.completeExceptionally(new IOException("Failed " +
          "notifyInstallSnapshotFromLeader due to missing leader info"));
      }
      numSnapshotRequests.incrementAndGet();

      final SingleFileSnapshotInfo leaderSnapshotInfo = (SingleFileSnapshotInfo) leaderSnapshotInfoRef.get();
      LOG.info("{}: leaderSnapshotInfo = {}", getId(), leaderSnapshotInfo);
      if (leaderSnapshotInfo == null) {
        return super.notifyInstallSnapshotFromLeader(roleInfoProto, termIndex);
      }

      Supplier<TermIndex> supplier = () -> {
        try {
          Path leaderSnapshotFile = leaderSnapshotInfo.getFile().getPath();
          File followerSnapshotFilePath = new File(getSMdir(),
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

      return CompletableFuture.supplyAsync(supplier);
    }
  }

  /**
   * Basic test for install snapshot notification: start a one node cluster
   * (disable install snapshot option) and let it generate a snapshot. Then
   * delete the log and restart the node, and add more nodes as followers.
   * The new follower nodes should get a install snapshot notification.
   */
  @Test
  public void testAddNewFollowers() throws Exception {
    runWithNewCluster(1, this::testAddNewFollowers);
  }

  private void testAddNewFollowers(CLUSTER cluster) throws Exception {
    leaderSnapshotInfoRef.set(null);
    final List<LogSegmentPath> logs;
    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply
              reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }
      }

      // wait for the snapshot to be done
      final RaftServer.Division leader = cluster.getLeader();
      final long nextIndex = leader.getRaftLog().getNextIndex();
      LOG.info("nextIndex = {}", nextIndex);
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
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
        Assert.assertTrue(client.io().send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
      }

      final SnapshotInfo leaderSnapshotInfo = cluster.getLeader().getStateMachine().getLatestSnapshot();
      final boolean set = leaderSnapshotInfoRef.compareAndSet(null, leaderSnapshotInfo);
      Assert.assertTrue(set);

      // add two more peers
      final MiniRaftCluster.PeerChanges change = cluster.addNewPeers(2, true,
          true);
      // trigger setConfiguration
      cluster.setConfiguration(change.allPeersInNewConf);

      RaftServerTestUtil
          .waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);

      // Check the installed snapshot index on each Follower matches with the
      // leader snapshot.
      for (RaftServer.Division follower : cluster.getFollowers()) {
        Assert.assertEquals(leaderSnapshotInfo.getIndex(),
            RaftServerTestUtil.getLatestInstalledSnapshotIndex(follower));
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
    leaderSnapshotInfoRef.set(null);
    int i = 0;
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();

    try (final RaftClient client = cluster.createClient(leaderId)) {
      for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
    }

    // wait for the snapshot to be done
    final long oldLeaderNextIndex = leader.getRaftLog().getNextIndex();
    {
      LOG.info("{}: oldLeaderNextIndex = {}", leaderId, oldLeaderNextIndex);
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          oldLeaderNextIndex - SNAPSHOT_TRIGGER_THRESHOLD, oldLeaderNextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
        return null;
      }, 10, ONE_SECOND, "snapshotFile.exist", LOG);
    }

    final RaftPeerId followerId = cluster.getFollowers().get(0).getId();
    cluster.killServer(followerId);

    // generate some more traffic
    try (final RaftClient client = cluster.createClient(leader.getId())) {
      Assert.assertTrue(client.io().send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
    }

    FIVE_SECONDS.sleep();
    cluster.restartServer(followerId, false);
    final RaftServer.Division follower = cluster.getDivision(followerId);
    JavaUtils.attempt(() -> {
      final long newLeaderNextIndex = leader.getRaftLog().getNextIndex();
      LOG.info("{}: newLeaderNextIndex = {}", leaderId, newLeaderNextIndex);
      Assert.assertTrue(newLeaderNextIndex > oldLeaderNextIndex);
      Assert.assertEquals(newLeaderNextIndex, follower.getRaftLog().getNextIndex());
    }, 10, ONE_SECOND, "followerNextIndex", LOG);
  }

  @Test
  public void testInstallSnapshotNotificationCount() throws Exception {
    runWithNewCluster(3, this::testInstallSnapshotNotificationCount);
  }


  private void testInstallSnapshotNotificationCount(CLUSTER cluster) throws Exception {
    leaderSnapshotInfoRef.set(null);
    numSnapshotRequests.set(0);

    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      // Let a few heartbeats pass.
      ONE_SECOND.sleep();
      Assert.assertEquals(0, numSnapshotRequests.get());

      // Generate data.
      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < 10; i++) {
          RaftClientReply
              reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }
      }

      // Wait until index has been updated
      RaftTestUtil.waitFor(
              () -> cluster.getLeader().getStateMachine().getLastAppliedTermIndex().getIndex() == 20,
               300, 15000);

      // Take snapshot and check result.
      long snapshotIndex = cluster.getLeader().getStateMachine().takeSnapshot();
      Assert.assertEquals(20, snapshotIndex);
      final SnapshotInfo leaderSnapshotInfo = cluster.getLeader().getStateMachine().getLatestSnapshot();
      Assert.assertEquals(20, leaderSnapshotInfo.getIndex());
      final boolean set = leaderSnapshotInfoRef.compareAndSet(null, leaderSnapshotInfo);
      Assert.assertTrue(set);

      // Wait for the snapshot to be done.
      final RaftServer.Division leader = cluster.getLeader();
      final long nextIndex = leader.getRaftLog().getNextIndex();
      Assert.assertEquals(21, nextIndex);
      // End index is exclusive.
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          0, nextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
        return null;
      }, 10, ONE_SECOND, "snapshotFile.exist", LOG);

      // Clear all log files and reset cached log start index.
      long snapshotInstallIndex =
          leader.getRaftLog().onSnapshotInstalled(leader.getRaftLog().getLastCommittedIndex()).get();
      Assert.assertEquals(20, snapshotInstallIndex);

      // Check that logs are gone.
      Assert.assertEquals(0,
          LogSegmentPath.getLogSegmentPaths(leader.getRaftStorage()).size());
      Assert.assertEquals(RaftLog.INVALID_LOG_INDEX, leader.getRaftLog().getStartIndex());

      // Allow some heartbeats to go through, then make sure none of them had
      // snapshot requests.
      ONE_SECOND.sleep();
      Assert.assertEquals(0, numSnapshotRequests.get());

      // Make sure leader and followers are still up to date.
      for (RaftServer.Division follower : cluster.getFollowers()) {
        Assert.assertEquals(
            leader.getRaftLog().getNextIndex(),
            follower.getRaftLog().getNextIndex());
      }

      // Add two more peers who will need snapshots from the leader.
      final MiniRaftCluster.PeerChanges change = cluster.addNewPeers(2, true,
          true);
      // trigger setConfiguration
      cluster.setConfiguration(change.allPeersInNewConf);
      RaftServerTestUtil
          .waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);

      // Generate more data.
      try (final RaftClient client = cluster.createClient(leader.getId())) {
        Assert.assertTrue(client.io().send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
      }

      // Make sure leader and followers are still up to date.
      for (RaftServer.Division follower : cluster.getFollowers()) {
        // Give follower slightly time to catch up
        RaftTestUtil.waitFor(
                () -> leader.getRaftLog().getNextIndex() == follower.getRaftLog().getNextIndex(),
                300, 15000);
      }

      // Make sure each new peer got one snapshot notification.
      Assert.assertEquals(2, numSnapshotRequests.get());

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
    leaderSnapshotInfoRef.set(null);
    numSnapshotRequests.set(0);
    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply
              reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }
      }

      // wait for the snapshot to be done
      final RaftServer.Division leader = cluster.getLeader();
      final long nextIndex = leader.getRaftLog().getNextIndex();
      LOG.info("nextIndex = {}", nextIndex);
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
        return null;
      }, 10, ONE_SECOND, "snapshotFile.exist", LOG);

      RaftSnapshotBaseTest.assertLeaderContent(cluster);

      final SnapshotInfo leaderSnapshotInfo = cluster.getLeader().getStateMachine().getLatestSnapshot();
      final boolean set = leaderSnapshotInfoRef.compareAndSet(null, leaderSnapshotInfo);
      Assert.assertTrue(set);

      // add two more peers
      final MiniRaftCluster.PeerChanges change = cluster.addNewPeers(2, true,
          true);
      // trigger setConfiguration
      cluster.setConfiguration(change.allPeersInNewConf);

      RaftServerTestUtil.waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);

      // Check the installed snapshot index on each Follower matches with the
      // leader snapshot.
      for (RaftServer.Division follower : cluster.getFollowers()) {
        Assert.assertEquals(leaderSnapshotInfo.getIndex(),
            RaftServerTestUtil.getLatestInstalledSnapshotIndex(follower));
      }

      // Make sure each new peer got one snapshot notification.
      Assert.assertEquals(2, numSnapshotRequests.get());
    } finally {
      cluster.shutdown();
    }
  }
}
