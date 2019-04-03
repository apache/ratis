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
package org.apache.ratis.grpc;

import org.apache.log4j.Level;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.statemachine.RaftSnapshotBaseTest;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.ratis.BaseTest.ONE_SECOND;

public class TestInstallSnapshotWithGrpc {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final Logger LOG = LoggerFactory.getLogger(TestInstallSnapshotWithGrpc.class);
  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 10;
  private static SingleFileSnapshotInfo leaderSnapshotInfo;

  private MiniRaftCluster cluster;

  private MiniRaftCluster.Factory<?> getFactory() {
    return MiniRaftClusterWithGrpc.FACTORY;
  }

  @Before
  public void setup() throws IOException {
    final RaftProperties prop = new RaftProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        TestInstallSnapshotWithGrpc.StateMachineForGRpcTest.class, StateMachine.class);
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(prop, false);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(
        prop, SNAPSHOT_TRIGGER_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, true);
    this.cluster = getFactory().newCluster(1, prop);
    cluster.start();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static class StateMachineForGRpcTest extends
      SimpleStateMachine4Testing {
    @Override
    public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(TermIndex termIndex) {
      try {
        Path leaderSnapshotFile = leaderSnapshotInfo.getFile().getPath();
        File followerSnapshotFilePath = new File(getSMdir(),
            leaderSnapshotFile.getFileName().toString());
        Files.copy(leaderSnapshotFile, followerSnapshotFilePath.toPath());
      } catch (IOException e) {
        e.printStackTrace();
      }
      return CompletableFuture.completedFuture(leaderSnapshotInfo.getTermIndex());
    }
  }

  /**
   * Basic test for install snapshot notification: start a one node cluster
   * (disable install snapshot option) and let it generate a snapshot. Then
   * delete the log and restart the node, and add more nodes as followers.
   * The new follower nodes should get a install snapshot notification.
   */
  @Test
  public void testInstallSnapshotNotification() throws Exception {
    final List<RaftStorageDirectory.LogPathAndIndex> logs;
    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply
              reply = client.send(new RaftTestUtil.SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }
      }

      // wait for the snapshot to be done
      RaftStorageDirectory storageDirectory = cluster.getLeader().getState()
          .getStorage().getStorageDir();

      final long nextIndex = cluster.getLeader().getState().getLog().getNextIndex();
      LOG.info("nextIndex = {}", nextIndex);
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
      JavaUtils.attempt(() -> snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists),
          10, ONE_SECOND, "snapshotFile.exist", LOG);
      logs = storageDirectory.getLogSegmentFiles();
    } finally {
      cluster.shutdown();
    }

    // delete the log segments from the leader
    for (RaftStorageDirectory.LogPathAndIndex path : logs) {
      FileUtils.delete(path.getPath());
    }

    // restart the peer
    LOG.info("Restarting the cluster");
    cluster.restart(false);
    try {
      RaftSnapshotBaseTest.assertLeaderContent(cluster);

      // generate some more traffic
      try(final RaftClient client = cluster.createClient(cluster.getLeader().getId())) {
        Assert.assertTrue(client.send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
      }

      leaderSnapshotInfo = (SingleFileSnapshotInfo) cluster.getLeader().getStateMachine().getLatestSnapshot();

      // add two more peers
      MiniRaftCluster.PeerChanges change = cluster.addNewPeers(
          new String[]{"s3", "s4"}, true);
      // trigger setConfiguration
      cluster.setConfiguration(change.allPeersInNewConf);

      RaftServerTestUtil
          .waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);

      // Check the installed snapshot index on each Follower matches with the
      // leader snapshot.
      for (RaftServerImpl follower : cluster.getFollowers()) {
        follower.getState().getStorage().getStorageDir().getStateMachineDir();
        Assert.assertEquals(leaderSnapshotInfo.getIndex(),
            follower.getState().getLatestInstalledSnapshot().getIndex());
      }

      // restart the peer and check if it can correctly handle conf change
      cluster.restartServer(cluster.getLeader().getId(), false);
      RaftSnapshotBaseTest.assertLeaderContent(cluster);
    } finally {
      cluster.shutdown();
    }
  }
}
