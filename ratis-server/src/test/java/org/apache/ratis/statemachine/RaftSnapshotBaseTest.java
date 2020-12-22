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
package org.apache.ratis.statemachine;

import static org.apache.ratis.server.impl.StateMachineMetrics.RATIS_STATEMACHINE_METRICS;
import static org.apache.ratis.server.impl.StateMachineMetrics.RATIS_STATEMACHINE_METRICS_DESC;
import static org.apache.ratis.server.impl.StateMachineMetrics.STATEMACHINE_TAKE_SNAPSHOT_TIMER;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RATIS_SERVER_INSTALL_SNAPSHOT_COUNT;
import static org.apache.ratis.metrics.RatisMetrics.RATIS_APPLICATION_NAME_METRICS;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Log4jUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

public abstract class RaftSnapshotBaseTest extends BaseTest {
  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final Logger LOG = LoggerFactory.getLogger(RaftSnapshotBaseTest.class);
  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 10;

  public static List<File> getSnapshotFiles(MiniRaftCluster cluster, long startIndex, long endIndex) {
    final RaftServer.Division leader = cluster.getLeader();
    final SimpleStateMachineStorage storage = SimpleStateMachine4Testing.get(leader).getStateMachineStorage();
    final long term = leader.getInfo().getCurrentTerm();
    return LongStream.range(startIndex, endIndex)
        .mapToObj(i -> storage.getSnapshotFile(term, i))
        .collect(Collectors.toList());
  }


  public static void assertLeaderContent(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftLog leaderLog = leader.getRaftLog();
    final long lastIndex = leaderLog.getLastEntryTermIndex().getIndex();
    final LogEntryProto e = leaderLog.get(lastIndex);
    Assert.assertTrue(e.hasMetadataEntry());

    JavaUtils.attemptRepeatedly(() -> {
      Assert.assertEquals(leaderLog.getLastCommittedIndex() - 1, e.getMetadataEntry().getCommitIndex());
      return null;
    }, 50, BaseTest.HUNDRED_MILLIS, "CheckMetadataEntry", LOG);

    SimpleStateMachine4Testing simpleStateMachine = SimpleStateMachine4Testing.get(leader);
    Assert.assertTrue("Is not notified as a leader", simpleStateMachine.isNotifiedAsLeader());
    final LogEntryProto[] entries = simpleStateMachine.getContent();
    long message = 0;
    for (int i = 0; i < entries.length; i++) {
      LOG.info("{}) {} {}", i, message, entries[i]);
      if (entries[i].hasStateMachineLogEntry()) {
        final SimpleMessage m = new SimpleMessage("m" + message++);
        Assert.assertArrayEquals(m.getContent().toByteArray(),
            entries[i].getStateMachineLogEntry().getLogData().toByteArray());
      }
    }
  }

  private MiniRaftCluster cluster;

  public abstract MiniRaftCluster.Factory<?> getFactory();

  @Before
  public void setup() throws IOException {
    final RaftProperties prop = new RaftProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
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

  /**
   * Keep generating writing traffic and make sure snapshots are taken.
   * We then restart the whole raft peer and check if it can correctly load
   * snapshots + raft log.
   */
  @Test
  public void testRestartPeer() throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();
    int i = 0;
    try(final RaftClient client = cluster.createClient(leaderId)) {
      for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
        RaftClientReply reply = client.io().send(new SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
    }

    final long nextIndex = cluster.getLeader().getRaftLog().getNextIndex();
    LOG.info("nextIndex = {}", nextIndex);
    // wait for the snapshot to be done
    final List<File> snapshotFiles = getSnapshotFiles(cluster, nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
    JavaUtils.attemptRepeatedly(() -> {
      Assert.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
      return null;
    }, 10, ONE_SECOND, "snapshotFile.exist", LOG);

    // restart the peer and check if it can correctly load snapshot
    cluster.restart(false);
    try {
      // 200 messages + two leader elections --> last committed = 201
      assertLeaderContent(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  public static boolean exists(File f) {
    if (f.exists()) {
      LOG.info("File exists: " + f);
      return true;
    }
    return false;
  }

  /**
   * Basic test for install snapshot: start a one node cluster and let it
   * generate a snapshot. Then delete the log and restart the node, and add more
   * nodes as followers.
   */
  @Test
  public void testBasicInstallSnapshot() throws Exception {
    final List<LogSegmentPath> logs;
    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply reply = client.io().send(new SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }
      }

      // wait for the snapshot to be done
      final long nextIndex = cluster.getLeader().getRaftLog().getNextIndex();
      LOG.info("nextIndex = {}", nextIndex);
      final List<File> snapshotFiles = getSnapshotFiles(cluster, nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
        return null;
      }, 10, ONE_SECOND, "snapshotFile.exist", LOG);
      verifyTakeSnapshotMetric(cluster.getLeader());
      logs = LogSegmentPath.getLogSegmentPaths(cluster.getLeader().getRaftStorage());
    } finally {
      cluster.shutdown();
    }

    // delete the log segments from the leader
    for (LogSegmentPath path : logs) {
      FileUtils.delete(path.getPath());
    }

    // restart the peer
    LOG.info("Restarting the cluster");
    cluster.restart(false);
    try {
      assertLeaderContent(cluster);

      // generate some more traffic
      try(final RaftClient client = cluster.createClient(cluster.getLeader().getId())) {
        Assert.assertTrue(client.io().send(new SimpleMessage("m" + i)).isSuccess());
      }

      // add two more peers
      String[] newPeers = new String[]{"s3", "s4"};
      MiniRaftCluster.PeerChanges change = cluster.addNewPeers(
          newPeers, true);
      // trigger setConfiguration
      cluster.setConfiguration(change.allPeersInNewConf);

      for (String newPeer : newPeers) {
        final RaftServer.Division s = cluster.getDivision(RaftPeerId.valueOf(newPeer));
        SimpleStateMachine4Testing simpleStateMachine = SimpleStateMachine4Testing.get(s);
        Assert.assertSame(LifeCycle.State.RUNNING, simpleStateMachine.getLifeCycleState());
      }

      // Verify installSnapshot counter on leader before restart.
      verifyInstallSnapshotMetric(cluster.getLeader());
      RaftServerTestUtil.waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);

      Timer timer = getTakeSnapshotTimer(cluster.getLeader());
      long count = timer.getCount();

      // restart the peer and check if it can correctly handle conf change
      cluster.restartServer(cluster.getLeader().getId(), false);
      assertLeaderContent(cluster);

      // verify that snapshot was taken when stopping the server
      Assert.assertTrue(count < timer.getCount());
    } finally {
      cluster.shutdown();
    }
  }

  protected void verifyInstallSnapshotMetric(RaftServer.Division leader) {
    final Counter installSnapshotCounter = ((RaftServerMetricsImpl)leader.getRaftServerMetrics())
        .getCounter(RATIS_SERVER_INSTALL_SNAPSHOT_COUNT);
    Assert.assertNotNull(installSnapshotCounter);
    Assert.assertTrue(installSnapshotCounter.getCount() >= 1);
  }

  private static void verifyTakeSnapshotMetric(RaftServer.Division leader) {
    Timer timer = getTakeSnapshotTimer(leader);
    Assert.assertTrue(timer.getCount() > 0);
  }

  private static Timer getTakeSnapshotTimer(RaftServer.Division leader) {
    MetricRegistryInfo info = new MetricRegistryInfo(leader.getMemberId().toString(),
        RATIS_APPLICATION_NAME_METRICS,
        RATIS_STATEMACHINE_METRICS, RATIS_STATEMACHINE_METRICS_DESC);
    Optional<RatisMetricRegistry> opt = MetricRegistries.global().get(info);
    Assert.assertTrue(opt.isPresent());
    RatisMetricRegistry metricRegistry = opt.get();
    Assert.assertNotNull(metricRegistry);
    return metricRegistry.timer(STATEMACHINE_TAKE_SNAPSHOT_TIMER);
  }
}
