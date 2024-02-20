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
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.SizeInBytes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

public abstract class GroupCommandIntegrationTest<CLUSTER extends MiniRaftCluster>
    extends AbstractCommandIntegrationTestWithGrpc
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  static final String NEW_LINE = System.lineSeparator();

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
  }

  @Test
  public void testGroupListCommand() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestGroupListCommand);
  }

  void runTestGroupListCommand(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final String address = getClusterAddress(cluster);
    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());
    int ret = shell.run("group", "list", "-peers", address, "-peerId",
        leader.getPeer().getId().toString());
    Assertions.assertEquals(0, ret);
    String info = out.toString().trim();
    String expected = String.format("The peerId %s (server %s) is in 1 groups, and the groupIds is: [%s]",
        leader.getId(), leader.getPeer().getAddress(), leader.getGroup().getGroupId());
    Assertions.assertEquals(expected, info);
  }

  @Test
  public void testGroupInfoCommand() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestGroupInfoCommand);
  }

  void runTestGroupInfoCommand(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final String address = getClusterAddress(cluster);
    final StringPrintStream out = new StringPrintStream();
    RatisShell shell = new RatisShell(out.getPrintStream());
    int ret = shell.run("group", "info", "-peers", address);
    Assertions.assertEquals(0 , ret);
    String result = out.toString().trim();
    String hearder = String.format("group id: %s%sleader info: %s(%s)%s%s",
        cluster.getGroupId().getUuid(), NEW_LINE, leader.getId(),
        cluster.getLeader().getPeer().getAddress(), NEW_LINE, NEW_LINE);
    String info = result.substring(0, hearder.length());
    Assertions.assertEquals(hearder, info);
  }
}
