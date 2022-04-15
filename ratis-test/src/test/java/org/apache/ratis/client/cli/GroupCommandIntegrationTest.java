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
package org.apache.ratis.client.cli;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.shell.cli.sh.RatisShell;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.SizeInBytes;

import org.junit.Assert;
import org.junit.Test;


public abstract class GroupCommandIntegrationTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.WARN);
    Log4jUtils.setLogLevel(RaftLog.LOG, Level.WARN);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.WARN);
  }

  static final int NUM_SERVERS = 3;

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
  }

  @Test
  public void testRestartFollower() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestGroupInfoCommand);
    runWithNewCluster(NUM_SERVERS, this::runTestGroupListCommand);
  }

  void runTestGroupListCommand(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    String address = cluster.getLeader().getPeer().getAdminAddress();
    StringBuffer sb = new StringBuffer();
    for (RaftServer.Division division : cluster.getFollowers()) {
      sb.append(division.getPeer().getAdminAddress());
      sb.append(",");
    }
    sb.append(address);
    RatisShell shell = new RatisShell();
    int ret = shell.run(new String[]{"group", "list", "-peers", sb.toString(), "-peerId", leader.getPeer().getId().toString()});
    Assert.assertEquals(0, ret);
  }

  void runTestGroupInfoCommand(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    String address = cluster.getLeader().getPeer().getAdminAddress();
    StringBuffer sb = new StringBuffer();
    for (RaftServer.Division division : cluster.getFollowers()) {
      sb.append(division.getPeer().getAdminAddress());
      sb.append(",");
    }
    sb.append(address);
    RatisShell shell = new RatisShell();
    int ret = shell.run(new String[]{"group", "info", "-peers", sb.toString()});
    Assert.assertEquals(0 , ret);
  }
}
