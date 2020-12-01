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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Test Raft Server Leader election timeout detection and notification to state machine.
 */
public class TestRaftServerNoLeaderTimeout extends BaseTest {
  static {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private static final int NUM_SERVERS = 3;

  private static final RaftProperties properties = new RaftProperties();

  private final MiniRaftClusterWithSimulatedRpc cluster = MiniRaftClusterWithSimulatedRpc
      .FACTORY.newCluster(NUM_SERVERS, getProperties());

  private static RaftProperties getProperties() {
    RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    return properties;
  }

  @Before
  public void setup() throws IOException {
    Assert.assertNull(cluster.getLeader());
    cluster.start();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testLeaderElectionDetection() throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final TimeDuration noLeaderTimeout = RaftServerConfigKeys.Notification.noLeaderTimeout(cluster.getProperties());

    final RaftServer.Division healthyFollower = cluster.getFollowers().get(1);
    final RaftServer.Division failedFollower = cluster.getFollowers().get(0);
    // fail the leader and one of the followers to that quorum is not present
    // for next leader election to succeed.
    cluster.killServer(failedFollower.getId());
    cluster.killServer(cluster.getLeader().getId());

    // Wait to ensure that leader election is triggered and also state machine callback is triggered
    noLeaderTimeout.sleep();
    noLeaderTimeout.sleep();

    RaftProtos.RoleInfoProto roleInfoProto =
        SimpleStateMachine4Testing.get(healthyFollower).getLeaderElectionTimeoutInfo();
    Assert.assertNotNull(roleInfoProto);

    Assert.assertEquals(roleInfoProto.getRole(), RaftProtos.RaftPeerRole.CANDIDATE);
    final long noLeaderTimeoutMs = noLeaderTimeout.toLong(TimeUnit.MILLISECONDS);
    Assert.assertTrue(roleInfoProto.getCandidateInfo().getLastLeaderElapsedTimeMs() > noLeaderTimeoutMs);
  }
}
