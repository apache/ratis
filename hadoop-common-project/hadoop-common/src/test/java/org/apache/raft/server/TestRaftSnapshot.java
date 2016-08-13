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
package org.apache.raft.server;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.RaftTestUtil.SimpleMessage;
import org.apache.raft.client.RaftClient;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.storage.RaftStorageDirectory.LogPathAndIndex;
import org.apache.raft.util.ProtoUtils;
import org.apache.raft.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.raft.server.simulation.RequestHandler;
import org.apache.raft.server.storage.MemoryRaftLog;
import org.apache.raft.server.storage.RaftStorageDirectory;
import org.apache.raft.util.RaftUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.raft.RaftTestUtil.waitAndCheckNewConf;

public class TestRaftSnapshot {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(MemoryRaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final Logger LOG = LoggerFactory.getLogger(TestRaftSnapshot.class);
  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 10;

  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private RaftProperties prop;

  @Before
  public void setup() {
    prop = new RaftProperties();
    prop.setClass(RaftServerConfigKeys.RAFT_SERVER_STATEMACHINE_CLASS_KEY,
        SimpleStateMachine.class, StateMachine.class);
    prop.setLong(
        RaftServerConfigKeys.RAFT_SERVER_SNAPSHOT_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_TRIGGER_THRESHOLD);
  }

  /**
   * Keep generating writing traffic and make sure snapshots are taken.
   * We then restart the whole raft peer and check if it can correctly load
   * snapshots + raft log.
   */
  @Test
  public void testRestartPeer() throws Exception {
    MiniRaftClusterWithSimulatedRpc cluster =
        new MiniRaftClusterWithSimulatedRpc(1, prop, true);
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      int i = 0;
      for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2; i++) {
        RaftClientReply reply = client.send(new SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }

      // wait for the snapshot to be done
      RaftStorageDirectory storageDirectory = cluster.getLeader().getState()
          .getStorage().getStorageDir();
      File snapshotFile = storageDirectory.getSnapshotFile(
          cluster.getLeader().getState().getCurrentTerm(), i - 1);

      int retries = 0;
      while (!snapshotFile.exists() && retries++ < 10) {
        Thread.sleep(1000);
      }

      Assert.assertTrue(snapshotFile + " does not exist", snapshotFile.exists());
    } finally {
      cluster.shutdown();
    }

    // restart the peer and check if it can correctly load snapshot
    cluster = new MiniRaftClusterWithSimulatedRpc(1, prop, false);
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);

      // 200 messages + two leader elections --> last committed = 201
      Assert.assertEquals(SNAPSHOT_TRIGGER_THRESHOLD * 2 + 1,
          cluster.getLeader().getState().getLog().getLastCommittedIndex());
      StateMachine sm = cluster.getLeader().getState().getStateMachine();
      LogEntryProto[] entries = ((SimpleStateMachine) sm).getContent();
      for (int i = 1; i <= SNAPSHOT_TRIGGER_THRESHOLD * 2; i++) {
        Assert.assertEquals(i, entries[i].getIndex());
        Assert.assertEquals(
            ProtoUtils.toClientMessageEntryProto(new SimpleMessage("m" + (i-1))),
            entries[i].getClientMessageEntry());
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Basic test for install snapshot: start a one node cluster and let it
   * generate a snapshot. Then delete the log and restart the node, and add more
   * nodes as followers.
   */
  @Test
  public void testBasicInstallSnapshot() throws Exception {
    MiniRaftClusterWithSimulatedRpc cluster =
        new MiniRaftClusterWithSimulatedRpc(1, prop, true);
    List<LogPathAndIndex> logs = new ArrayList<>();
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      int i = 0;
      for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
        RaftClientReply reply = client.send(new SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }

      // wait for the snapshot to be done
      RaftStorageDirectory storageDirectory = cluster.getLeader().getState()
          .getStorage().getStorageDir();
      File snapshotFile = storageDirectory.getSnapshotFile(
          cluster.getLeader().getState().getCurrentTerm(), i);
      logs = storageDirectory.getLogSegmentFiles();

      int retries = 0;
      while (!snapshotFile.exists() && retries++ < 10) {
        Thread.sleep(1000);
      }

      Assert.assertTrue(snapshotFile + " does not exist", snapshotFile.exists());
    } finally {
      Thread.sleep(1000);
      cluster.shutdown();
    }

    // delete the log segments from the leader
    for (LogPathAndIndex path : logs) {
      RaftUtils.deleteFile(path.path.toFile());
    }

    // restart the peer
    cluster = new MiniRaftClusterWithSimulatedRpc(1, prop, false);
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);

      Assert.assertEquals(SNAPSHOT_TRIGGER_THRESHOLD * 2,
          cluster.getLeader().getState().getLog().getLastCommittedIndex());
      StateMachine sm = cluster.getLeader().getState().getStateMachine();
      LogEntryProto[] entries = ((SimpleStateMachine) sm).getContent();
      for (int i = 1; i < SNAPSHOT_TRIGGER_THRESHOLD * 2; i++) {
        Assert.assertEquals(i, entries[i].getIndex());
        Assert.assertEquals(
            ProtoUtils.toClientMessageEntryProto(new SimpleMessage("m" + (i-1))),
            entries[i].getClientMessageEntry());
      }

      // generate some more traffic
      final RaftClient client = cluster.createClient("client",
          cluster.getLeader().getId());
      Assert.assertTrue(client.send(new SimpleMessage("test")).isSuccess());

      // add two more peers
      MiniRaftCluster.PeerChanges change = cluster.addNewPeers(2, true);
      // trigger setConfiguration
      SetConfigurationRequest request = new SetConfigurationRequest("client",
          cluster.getLeader().getId(), change.allPeersInNewConf);
      LOG.info("Start changing the configuration: {}", request);
      cluster.getLeader().setConfiguration(request);

      waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);
    } finally {
      cluster.shutdown();
    }
  }
}
