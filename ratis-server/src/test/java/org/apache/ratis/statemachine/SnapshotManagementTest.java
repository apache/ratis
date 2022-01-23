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

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.Log4jUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public abstract class SnapshotManagementTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftLog.LOG, Level.INFO);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.INFO);
  }

  static final Logger LOG = LoggerFactory.getLogger(SnapshotManagementTest.class);

  @Before
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Snapshot.setCreationGap(p,20L);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(p, false);
  }

  @Test
  public void testTakeSnapshot() throws Exception {
    runWithNewCluster(1, this::runTestTakeSnapshot);
    runWithNewCluster(1,this::runTestTakeSnapshotWithConfigurableGap);
    runWithNewCluster(3,this::runTestTakeSnapshotOnSpecificServer);
  }

  void runTestTakeSnapshot(CLUSTER cluster) throws Exception {
    final RaftClientReply snapshotReply;
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    try (final RaftClient client = cluster.createClient(leaderId)) {
      for (int i = 0; i < RaftServerConfigKeys.Snapshot.creationGap(getProperties()); i++) {
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
      snapshotReply = client.getSnapshotManagementApi().create(3000);
    }

    Assert.assertTrue(snapshotReply.isSuccess());
    final long snapshotIndex = snapshotReply.getLogIndex();
    LOG.info("snapshotIndex = {}", snapshotIndex);

    final File snapshotFile = SimpleStateMachine4Testing.get(leader)
        .getStateMachineStorage().getSnapshotFile(leader.getInfo().getCurrentTerm(), snapshotIndex);
    Assert.assertTrue(snapshotFile.exists());
  }

  void runTestTakeSnapshotWithConfigurableGap(CLUSTER cluster) throws Exception {
    RaftClientReply snapshotReply;
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    try (final RaftClient client = cluster.createClient(leaderId)) {
      for (int i = 0; i < RaftServerConfigKeys.Snapshot.creationGap(getProperties())/2-1; i++) {
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
      Assert.assertTrue(leader.getStateMachine().getLastAppliedTermIndex().getIndex()
            < RaftServerConfigKeys.Snapshot.creationGap(getProperties()));
      snapshotReply = client.getSnapshotManagementApi(leaderId).create(3000);
      Assert.assertTrue(snapshotReply.isSuccess());
      Assert.assertEquals(0,snapshotReply.getLogIndex());
      for (int i = 0; i < RaftServerConfigKeys.Snapshot.creationGap(getProperties())/2-1; i++) {
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
      final SnapshotManagementRequest r1 = SnapshotManagementRequest.newCreate(client.getId(),
          leaderId, cluster.getGroupId(), CallId.getAndIncrement(), 3000);
      snapshotReply = client.getSnapshotManagementApi(leaderId).create(3000);
    }
    Assert.assertTrue(snapshotReply.isSuccess());
    final long snapshotIndex = snapshotReply.getLogIndex();
    LOG.info("snapshotIndex = {}", snapshotIndex);

    final File snapshotFile = SimpleStateMachine4Testing.get(leader)
        .getStateMachineStorage()
        .getSnapshotFile(leader.getInfo().getCurrentTerm(), snapshotIndex);
    Assert.assertTrue(snapshotFile.exists());
  }

  void runTestTakeSnapshotOnSpecificServer(CLUSTER cluster) throws Exception {
    final RaftClientReply snapshotReply;
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftServer.Division follower = cluster.getFollowers().get(0);
    final RaftPeerId followerId = follower.getId();
    Assert.assertTrue(follower.getInfo().isFollower());
    try (final RaftClient client = cluster.createClient(followerId)) {
      for (int i = 0; i < RaftServerConfigKeys.Snapshot.creationGap(getProperties()); i++) {
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
      snapshotReply = client.getSnapshotManagementApi(followerId).create(3000);
    }

    Assert.assertTrue(snapshotReply.isSuccess());
    final long snapshotIndex = snapshotReply.getLogIndex();
    LOG.info("snapshotIndex = {} on {} server {}",
        snapshotIndex, follower.getInfo().getCurrentRole(), follower.getId());

    final File snapshotFile = SimpleStateMachine4Testing.get(follower)
        .getStateMachineStorage().getSnapshotFile(follower.getInfo().getCurrentTerm(), snapshotIndex);
    Assert.assertTrue(snapshotFile.exists());
  }
}
