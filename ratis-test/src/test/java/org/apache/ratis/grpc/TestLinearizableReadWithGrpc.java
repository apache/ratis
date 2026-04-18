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
package org.apache.ratis.grpc;

import org.apache.ratis.LinearizableReadTests;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.impl.DefaultTimekeeperImpl;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys.Read.ReadIndex.Type;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.ratis.ReadOnlyRequestTests.INCREMENT;
import static org.apache.ratis.ReadOnlyRequestTests.QUERY;
import static org.apache.ratis.ReadOnlyRequestTests.assertReplyExact;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RAFT_CLIENT_READ_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLinearizableReadWithGrpc
  extends LinearizableReadTests<MiniRaftClusterWithGrpc>
  implements MiniRaftClusterWithGrpc.FactoryGet {

  @Override
  public boolean isLeaderLeaseEnabled() {
    return false;
  }

  @Override
  public Type readIndexType() {
    return Type.COMMIT_INDEX;
  }

  @Test
  public void testFollowerLinearizableReadStaysOnFollower() throws Exception {
    runWithNewCluster(TestLinearizableReadWithGrpc::runTestFollowerLinearizableReadStaysOnFollower);
  }

  static void runTestFollowerLinearizableReadStaysOnFollower(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final List<RaftServer.Division> followers = cluster.getFollowers();
    assertEquals(2, followers.size());
    final RaftServer.Division follower = followers.get(0);
    final RaftPeerId followerId = follower.getId();

    final RatisMetricRegistry leaderRegistry = TestRaftServerWithGrpc.getRaftServerMetrics(leader).getRegistry();
    final RatisMetricRegistry followerRegistry = TestRaftServerWithGrpc.getRaftServerMetrics(follower).getRegistry();
    final DefaultTimekeeperImpl leaderReadTimer =
        (DefaultTimekeeperImpl) leaderRegistry.timer(RAFT_CLIENT_READ_REQUEST);
    final DefaultTimekeeperImpl followerReadTimer =
        (DefaultTimekeeperImpl) followerRegistry.timer(RAFT_CLIENT_READ_REQUEST);

    final long leaderReadCountBefore = leaderReadTimer.getTimer().getCount();
    final long followerReadCountBefore = followerReadTimer.getTimer().getCount();
    final int numReads = 5;

    try (RaftClient client = cluster.createClient(leader.getId())) {
      assertReplyExact(1, client.io().send(INCREMENT));

      for (int i = 0; i < numReads; i++) {
        assertReplyExact(1, client.async().sendReadOnly(QUERY, followerId).get());
      }
    }

    JavaUtils.attempt(() -> {
      final long leaderReadCountAfter = leaderReadTimer.getTimer().getCount();
      final long followerReadCountAfter = followerReadTimer.getTimer().getCount();
      assertEquals(leaderReadCountBefore, leaderReadCountAfter,
          () -> "leader unexpectedly handled follower-directed reads");
      assertTrue(followerReadCountAfter >= followerReadCountBefore + numReads,
          () -> "follower did not record the follower-directed reads");
    }, 3, TimeDuration.ONE_SECOND, "follower read metrics", null);
  }
}
