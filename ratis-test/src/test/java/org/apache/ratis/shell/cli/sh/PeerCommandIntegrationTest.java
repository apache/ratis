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

import org.apache.log4j.Level;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.common.collect.ObjectArrays;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public abstract class PeerCommandIntegrationTest <CLUSTER extends MiniRaftCluster>
    extends AbstractCommandIntegrationTestWithGrpc implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.WARN);
    Log4jUtils.setLogLevel(RaftLog.LOG, Level.WARN);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.WARN);
  }

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
  }

  @Test
  public void testPeerAddRemoveCommand() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestPeerRemoveCommand);
    runWithNewCluster(NUM_SERVERS, this::runTestPeerAddCommand);
  }

  void runTestPeerRemoveCommand(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final String address = getClusterAddress(cluster);
    RaftServer.Division toRemove = cluster.getFollowers().get(0);
    RaftPeer[] peers = new RaftPeer[]{cluster.getFollowers().get(1).getPeer(), leader.getPeer()};
    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());
    Assert.assertTrue(cluster.getFollowers().contains(toRemove));
    int ret = shell.run("peer", "remove", "-peers", address, "-peerId",
        toRemove.getPeer().getId().toString());

    Assert.assertEquals(0, ret);
    RaftServerTestUtil.waitAndCheckNewConf(cluster, peers,1, null);
  }

  void runTestPeerAddCommand(MiniRaftCluster cluster) throws Exception {
    LOG.info("Start testMultiGroup" + cluster.printServers());

    RaftTestUtil.waitForLeader(cluster);
    RaftPeer[] peers = cluster.getPeers().toArray(new RaftPeer[0]);
    RaftPeer[] newPeers = cluster.addNewPeers(1, true, true).newPeers;

    RaftServerTestUtil.waitAndCheckNewConf(cluster, peers, 0, null);
    StringBuilder sb = new StringBuilder();
    for (RaftPeer peer : peers) {
      sb.append(peer.getAdminAddress());
      sb.append(",");
    }
    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());

    int ret = shell.run("peer", "add", "-peers", sb.toString(), "-address",
        newPeers[0].getAdminAddress(), "-peerId", newPeers[0].getId().toString());

    Assert.assertEquals(0, ret);
    RaftServerTestUtil.waitAndCheckNewConf(cluster, ObjectArrays.concat(peers, newPeers[0]), 0, null);

  }

  @Test
  public void testPeerSetPriorityCommand() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestPeerSetPriorityCommand);
  }

  void runTestPeerSetPriorityCommand(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final String address = getClusterAddress(cluster);

    RaftServer.Division newLeader = cluster.getFollowers().get(0);
    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());
    Assert.assertTrue(cluster.getFollowers().contains(newLeader));
    int ret = shell.run("peer", "setPriority", "-peers", address, "-addressPriority",
        newLeader.getPeer().getAddress()+ "|" + 2);
    Assert.assertEquals(0, ret);
    JavaUtils.attempt(() -> {
      Assert.assertEquals(cluster.getLeader().getId(), newLeader.getId());
    }, 10, TimeDuration.valueOf(1, TimeUnit.SECONDS), "testPeerSetPriorityCommand", LOG);
  }

}
