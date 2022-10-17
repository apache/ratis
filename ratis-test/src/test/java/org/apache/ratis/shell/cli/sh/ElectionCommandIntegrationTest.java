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
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public abstract class ElectionCommandIntegrationTest <CLUSTER extends MiniRaftCluster>
    extends AbstractCommandIntegrationTestWithGrpc implements MiniRaftCluster.Factory.Get<CLUSTER>{

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
  public void testElectionTransferCommand() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestElectionTransferCommand);
  }

  void runTestElectionTransferCommand(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    String address = getClusterAddress(cluster);
    RaftServer.Division newLeader = cluster.getFollowers().get(0);
    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());
    Assert.assertNotEquals(cluster.getLeader().getId(), newLeader.getId());
    int ret = shell.run("election", "transfer", "-peers", address, "-address",
        newLeader.getPeer().getAddress());

    Assert.assertEquals(0, ret);
    JavaUtils.attempt(() -> {
      Assert.assertEquals(cluster.getLeader().getId(), newLeader.getId());
    }, 10, TimeDuration.valueOf(1, TimeUnit.SECONDS), "testElectionTransferCommand", LOG);
  }

  @Test
  public void testElectionPauseResumeCommand() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestElectionPauseResumeCommand);
  }

  void runTestElectionPauseResumeCommand(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    String sb = getClusterAddress(cluster);
    RaftServer.Division newLeader = cluster.getFollowers().get(0);
    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());
    Assert.assertNotEquals(cluster.getLeader().getId(), newLeader.getId());
    int ret = shell.run("election", "pause", "-peers", sb.toString(), "-address",
        newLeader.getPeer().getAddress());

    Assert.assertEquals(0, ret);
    ret = shell.run("peer", "setPriority", "-peers", sb.toString(), "-addressPriority",
        newLeader.getPeer().getAddress() + "|" + 2);
    Assert.assertEquals(0, ret);

    JavaUtils.attempt(() -> {
      Assert.assertNotEquals(cluster.getLeader().getId(), newLeader.getId());
    }, 10, TimeDuration.valueOf(1, TimeUnit.SECONDS), "testElectionPauseResumeCommand", LOG);

    ret = shell.run("election", "resume", "-peers", sb.toString(), "-address",
        newLeader.getPeer().getAddress());
    Assert.assertEquals(0, ret);

    JavaUtils.attempt(() -> {
      Assert.assertEquals(cluster.getLeader().getId(), newLeader.getId());
    }, 10, TimeDuration.valueOf(1, TimeUnit.SECONDS), "testElectionPauseResumeCommand", LOG);
  }
}
