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
package org.apache.ratis.shell.cli.sh;

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.File;

public abstract class SnapshotCommandIntegrationTest <CLUSTER extends MiniRaftCluster>
    extends AbstractCommandIntegrationTestWithGrpc
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.WARN);
    Slf4jUtils.setLogLevel(RaftLog.LOG, Level.WARN);
    Slf4jUtils.setLogLevel(RaftClient.LOG, Level.WARN);
  }

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, false);
    RaftServerConfigKeys.Snapshot.setCreationGap(prop, 20);
  }

  @Test
  public void testSnapshotCreateCommand() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestSnapshotCreateCommand);
    runWithNewCluster(NUM_SERVERS, this::runTestSnapshotCreateCommandOnSpecificServer);
  }

  void runTestSnapshotCreateCommand(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    try (final RaftClient client = cluster.createClient(leaderId)) {
      for (int i = 0; i < RaftServerConfigKeys.Snapshot.creationGap(getProperties()); i++) {
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
    }
    final String address = getClusterAddress(cluster);
    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());
    int ret = shell.run("snapshot", "create", "-peers", address, "-peerId",
        leader.getPeer().getId().toString());
    Assert.assertEquals(0, ret);
    String[] str = out.toString().trim().split(" ");
    int snapshotIndex = Integer.parseInt(str[str.length-1]);
    LOG.info("snapshotIndex = {}", snapshotIndex);

    final File snapshotFile = SimpleStateMachine4Testing.get(leader)
        .getStateMachineStorage().getSnapshotFile(leader.getInfo().getCurrentTerm(), snapshotIndex);
    Assert.assertTrue(snapshotFile.exists());
  }

  void runTestSnapshotCreateCommandOnSpecificServer(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    try (final RaftClient client = cluster.createClient(leaderId)) {
      for (int i = 0; i < RaftServerConfigKeys.Snapshot.creationGap(getProperties()); i++) {
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
    }
    final String address = getClusterAddress(cluster);
    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());
    Assert.assertEquals(2, cluster.getFollowers().size());
    int ret = shell.run("snapshot", "create", "-peers", address, "-peerId",
        cluster.getFollowers().get(0).getId().toString());
    Assert.assertEquals(0, ret);
    String[] str = out.toString().trim().split(" ");
    int snapshotIndex = Integer.parseInt(str[str.length-1]);
    LOG.info("snapshotIndex = {}", snapshotIndex);

    final File snapshotFile = SimpleStateMachine4Testing.get(cluster.getFollowers().get(0))
        .getStateMachineStorage()
        .getSnapshotFile(cluster.getFollowers().get(0).getInfo().getCurrentTerm(), snapshotIndex);
    Assert.assertTrue(snapshotFile.exists());
  }

}
