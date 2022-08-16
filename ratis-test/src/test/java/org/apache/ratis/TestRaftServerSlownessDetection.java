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
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;

/**
 * Test Raft Server Slowness detection and notification to Leader's statemachine.
 */
//TODO: fix StateMachine.notifySlowness(..); see RATIS-370
@Ignore
public class TestRaftServerSlownessDetection extends BaseTest {
  static {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  protected static final RaftProperties properties = new RaftProperties();

  private final MiniRaftClusterWithSimulatedRpc cluster = MiniRaftClusterWithSimulatedRpc
      .FACTORY.newCluster(NUM_SERVERS, getProperties());

  public RaftProperties getProperties() {
    RaftServerConfigKeys.Rpc
        .setSlownessTimeout(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
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
  public void testSlownessDetection() throws Exception {
    RaftServer.Division leaderServer = RaftTestUtil.waitForLeader(cluster);
    long slownessTimeout = RaftServerConfigKeys.Rpc
        .slownessTimeout(cluster.getProperties()).toIntExact(TimeUnit.MILLISECONDS);
    RaftServer.Division failedFollower = cluster.getFollowers().get(0);

    final RatisMetricRegistry ratisMetricRegistry
        = ((RaftServerMetricsImpl)leaderServer.getRaftServerMetrics()).getRegistry();
    SortedMap<String, Gauge> heartbeatElapsedTimeGauges =
        ratisMetricRegistry.getGauges((s, metric) ->
            s.contains("lastHeartbeatElapsedTime"));

    String followerId = failedFollower.getId().toString();
    Gauge metric = heartbeatElapsedTimeGauges.entrySet().parallelStream().filter(e -> e.getKey().contains(
        followerId)).iterator().next().getValue();

    long followerHeartBeatElapsedMetric = (long) metric.getValue();

    // fail the node and wait for the callback to be triggered
    cluster.killServer(failedFollower.getId());
    Thread.sleep( slownessTimeout * 2);

    long followerHeartBeatElapsedMetricNew = (long) metric.getValue();
    Assert.assertTrue(followerHeartBeatElapsedMetricNew > followerHeartBeatElapsedMetric);

    // Followers should not get any failed not notification
    for (RaftServer.Division followerServer : cluster.getFollowers()) {
      Assert.assertNull(SimpleStateMachine4Testing.get(followerServer).getSlownessInfo());
    }
    // the leader should get notification that the follower has failed now
    RaftProtos.RoleInfoProto roleInfoProto =
        SimpleStateMachine4Testing.get(cluster.getLeader()).getSlownessInfo();
    Assert.assertNotNull(roleInfoProto);

    List<RaftProtos.ServerRpcProto> followers =
        roleInfoProto.getLeaderInfo().getFollowerInfoList();
    //Assert that the node shutdown is lagging behind
    for (RaftProtos.ServerRpcProto serverProto : followers) {
      if (RaftPeerId.valueOf(serverProto.getId().getId()).equals(failedFollower.getId())) {
        Assert.assertTrue(serverProto.getLastRpcElapsedTimeMs() > slownessTimeout);
      }
    }
  }
}
