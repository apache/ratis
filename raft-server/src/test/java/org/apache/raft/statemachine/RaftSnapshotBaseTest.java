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
package org.apache.raft.statemachine;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.RaftTestUtil.SimpleMessage;
import org.apache.raft.client.RaftClient;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.simulation.RequestHandler;
import org.apache.raft.server.storage.RaftLog;
import org.apache.raft.server.storage.RaftStorageDirectory;
import org.apache.raft.server.storage.RaftStorageDirectory.LogPathAndIndex;
import org.apache.raft.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.raft.util.RaftUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.raft.RaftTestUtil.waitAndCheckNewConf;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_AUTO_SNAPSHOT_ENABLED_KEY;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_SNAPSHOT_TRIGGER_THRESHOLD_KEY;
import static org.apache.raft.server.RaftServerConstants.DEFAULT_SEQNUM;

public abstract class RaftSnapshotBaseTest {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final Logger LOG = LoggerFactory.getLogger(RaftSnapshotBaseTest.class);
  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 10;

  private MiniRaftCluster cluster;

  public abstract MiniRaftCluster initCluster(int numServer, RaftProperties prop)
      throws IOException;

  @Before
  public void setup() throws IOException {
    final RaftProperties prop = new RaftProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine.class, StateMachine.class);
    prop.setLong(RAFT_SERVER_SNAPSHOT_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_TRIGGER_THRESHOLD);
    prop.setBoolean(RAFT_SERVER_AUTO_SNAPSHOT_ENABLED_KEY, true);
    this.cluster = initCluster(1, prop);
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
    final String leaderId = cluster.getLeader().getId();
    int i = 0;
    try(final RaftClient client = cluster.createClient("client", leaderId)) {
      for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
        RaftClientReply reply = client.send(new SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
    }

    // wait for the snapshot to be done
    StateMachine sm = cluster.getLeader().getStateMachine();
    File snapshotFile = ((SimpleStateMachineStorage)sm.getStateMachineStorage())
        .getSnapshotFile(cluster.getLeader().getState().getCurrentTerm(), i);

    int retries = 0;
    do {
      Thread.sleep(1000);
    } while (!snapshotFile.exists() && retries++ < 10);

    Assert.assertTrue(snapshotFile + " does not exist", snapshotFile.exists());

    // restart the peer and check if it can correctly load snapshot
    cluster.restart(false);
    try {
      RaftTestUtil.waitForLeader(cluster);

      // 200 messages + two leader elections --> last committed = 201
      Assert.assertEquals(SNAPSHOT_TRIGGER_THRESHOLD * 2,
          cluster.getLeader().getState().getLog().getLastCommittedIndex());
      sm = cluster.getLeader().getStateMachine();
      LogEntryProto[] entries = ((SimpleStateMachine) sm).getContent();
      for (i = 1; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
        Assert.assertEquals(i+1, entries[i].getIndex());
        Assert.assertArrayEquals(
            new SimpleMessage("m" + i).getContent().toByteArray(),
            entries[i].getSmLogEntry().getData().toByteArray());
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
    List<LogPathAndIndex> logs = new ArrayList<>();
    try {
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();

      int i = 0;
      try(final RaftClient client = cluster.createClient("client", leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply reply = client.send(new SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }
      }

      // wait for the snapshot to be done
      RaftStorageDirectory storageDirectory = cluster.getLeader().getState()
          .getStorage().getStorageDir();
      StateMachine sm = cluster.getLeader().getStateMachine();
      File snapshotFile = ((SimpleStateMachineStorage) sm.getStateMachineStorage())
          .getSnapshotFile(cluster.getLeader().getState().getCurrentTerm(), i);
      logs = storageDirectory.getLogSegmentFiles();

      int retries = 0;
      do {
        Thread.sleep(1000);
      } while (!snapshotFile.exists() && retries++ < 10);

      Assert.assertTrue(snapshotFile + " does not exist", snapshotFile.exists());
    } finally {
      cluster.shutdown();
    }

    // delete the log segments from the leader
    for (LogPathAndIndex path : logs) {
      RaftUtils.deleteFile(path.path.toFile());
    }

    // restart the peer
    LOG.info("Restarting the cluster");
    cluster.restart(false);
    try {
      RaftTestUtil.waitForLeader(cluster);

      Assert.assertEquals(SNAPSHOT_TRIGGER_THRESHOLD * 2,
          cluster.getLeader().getState().getLog().getLastCommittedIndex());
      StateMachine sm = cluster.getLeader().getStateMachine();
      LogEntryProto[] entries = ((SimpleStateMachine) sm).getContent();
      for (int i = 1; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
        Assert.assertEquals(i+1, entries[i].getIndex());
        Assert.assertArrayEquals(
            new SimpleMessage("m" + i).getContent().toByteArray(),
            entries[i].getSmLogEntry().getData().toByteArray());
      }

      // generate some more traffic
      try(final RaftClient client = cluster.createClient("client",
          cluster.getLeader().getId())) {
        Assert.assertTrue(client.send(new SimpleMessage("test")).isSuccess());
      }

      // add two more peers
      MiniRaftCluster.PeerChanges change = cluster.addNewPeers(
          new String[]{"s3", "s4"}, true);
      // trigger setConfiguration
      SetConfigurationRequest request = new SetConfigurationRequest("client",
          cluster.getLeader().getId(), DEFAULT_SEQNUM, change.allPeersInNewConf);
      LOG.info("Start changing the configuration: {}", request);
      cluster.getLeader().setConfiguration(request);

      waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);
    } finally {
      cluster.shutdown();
    }
  }
}
