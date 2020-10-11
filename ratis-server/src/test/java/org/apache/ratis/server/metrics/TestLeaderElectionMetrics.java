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

package org.apache.ratis.server.metrics;

import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LAST_LEADER_ELECTION_ELAPSED_TIME;
import static org.apache.ratis.server.metrics.LeaderElectionMetrics.LEADER_ELECTION_TIMEOUT_COUNT_METRIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerState;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for LeaderElectionMetrics.
 */
public class TestLeaderElectionMetrics {

  private static LeaderElectionMetrics leaderElectionMetrics;
  private static RatisMetricRegistry ratisMetricRegistry;

  @BeforeClass
  public static void setUp() throws Exception {
    RaftServerImpl raftServer = mock(RaftServerImpl.class);
    ServerState serverStateMock = mock(ServerState.class);
    when(raftServer.getState()).thenReturn(serverStateMock);
    when(serverStateMock.getLastLeaderElapsedTimeMs()).thenReturn(1000L);
    RaftGroupId raftGroupId = RaftGroupId.randomId();
    RaftPeerId raftPeerId = RaftPeerId.valueOf("TestId");
    RaftGroupMemberId raftGroupMemberId = RaftGroupMemberId.valueOf(raftPeerId, raftGroupId);
    when(raftServer.getMemberId()).thenReturn(raftGroupMemberId);
    leaderElectionMetrics = LeaderElectionMetrics.getLeaderElectionMetrics(raftServer);
    ratisMetricRegistry = leaderElectionMetrics.getRegistry();
  }

  @Test
  public void testOnLeaderElectionCompletion() throws Exception {
    leaderElectionMetrics.onNewLeaderElectionCompletion();
    Long leaderElectionLatency = (Long) ratisMetricRegistry.getGauges((s, metric) ->
        s.contains(LAST_LEADER_ELECTION_ELAPSED_TIME)).values().iterator().next().getValue();
    assertTrue(leaderElectionLatency > 0L);
  }

  @Test
  public void testOnLeaderElectionTimeout() throws Exception {
    long numLeaderElectionTimeouts = ratisMetricRegistry.counter(
        LEADER_ELECTION_TIMEOUT_COUNT_METRIC).getCount();
    assertTrue(numLeaderElectionTimeouts == 0);
    leaderElectionMetrics.onLeaderElectionTimeout();
    numLeaderElectionTimeouts = ratisMetricRegistry.counter(LEADER_ELECTION_TIMEOUT_COUNT_METRIC).getCount();
    assertEquals(1, numLeaderElectionTimeouts);
  }
}