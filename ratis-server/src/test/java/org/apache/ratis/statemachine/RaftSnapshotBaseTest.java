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
package org.apache.ratis.statemachine;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.server.storage.RaftStorageDirectory.LogPathAndIndex;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public abstract class RaftSnapshotBaseTest extends BaseTest {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final Logger LOG = LoggerFactory.getLogger(RaftSnapshotBaseTest.class);
  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 10;

  static List<File> getSnapshotFiles(MiniRaftCluster cluster, long startIndex, long endIndex) {
    final RaftServerImpl leader = cluster.getLeader();
    final SimpleStateMachineStorage storage = SimpleStateMachine4Testing.get(leader).getStateMachineStorage();
    final long term = leader.getState().getCurrentTerm();
    return LongStream.range(startIndex, endIndex)
        .mapToObj(i -> storage.getSnapshotFile(term, i))
        .collect(Collectors.toList());
  }


  static void assertLeaderContent(MiniRaftCluster cluster) throws Exception {
    final RaftServerImpl leader = RaftTestUtil.waitForLeader(cluster);
    final RaftLog leaderLog = leader.getState().getLog();
    final long lastIndex = leaderLog.getLastEntryTermIndex().getIndex();
    final LogEntryProto e = leaderLog.get(lastIndex);
    Assert.assertTrue(e.hasMetadataEntry());
    Assert.assertEquals(leaderLog.getLastCommittedIndex() - 1, e.getMetadataEntry().getCommitIndex());

    final LogEntryProto[] entries = SimpleStateMachine4Testing.get(leader).getContent();
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
        RaftClientReply reply = client.send(new SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
    }

    final long nextIndex = cluster.getLeader().getState().getLog().getNextIndex();
    LOG.info("nextIndex = {}", nextIndex);
    // wait for the snapshot to be done
    final List<File> snapshotFiles = getSnapshotFiles(cluster, nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
    JavaUtils.attempt(() -> snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists),
        10, 1000, "snapshotFile.exist", LOG);

    // restart the peer and check if it can correctly load snapshot
    cluster.restart(false);
    try {
      // 200 messages + two leader elections --> last committed = 201
      assertLeaderContent(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  static boolean exists(File f) {
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
    final List<LogPathAndIndex> logs;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      int i = 0;
      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply reply = client.send(new SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }
      }

      // wait for the snapshot to be done
      RaftStorageDirectory storageDirectory = cluster.getLeader().getState()
          .getStorage().getStorageDir();

      final long nextIndex = cluster.getLeader().getState().getLog().getNextIndex();
      LOG.info("nextIndex = {}", nextIndex);
      final List<File> snapshotFiles = getSnapshotFiles(cluster, nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
      JavaUtils.attempt(() -> snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists),
          10, 1000, "snapshotFile.exist", LOG);
      logs = storageDirectory.getLogSegmentFiles();
    } finally {
      cluster.shutdown();
    }

    // delete the log segments from the leader
    for (LogPathAndIndex path : logs) {
      FileUtils.delete(path.getPath());
    }

    // restart the peer
    LOG.info("Restarting the cluster");
    cluster.restart(false);
    try {
      assertLeaderContent(cluster);

      // generate some more traffic
      try(final RaftClient client = cluster.createClient(cluster.getLeader().getId())) {
        Assert.assertTrue(client.send(new SimpleMessage("test")).isSuccess());
      }

      // add two more peers
      MiniRaftCluster.PeerChanges change = cluster.addNewPeers(
          new String[]{"s3", "s4"}, true);
      // trigger setConfiguration
      cluster.setConfiguration(change.allPeersInNewConf);

      RaftServerTestUtil.waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);
    } finally {
      cluster.shutdown();
    }
  }
}
