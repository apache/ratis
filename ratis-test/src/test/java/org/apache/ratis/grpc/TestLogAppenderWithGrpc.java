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

import org.apache.ratis.LogAppenderTests;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.metrics.GrpcServerMetrics;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

@RunWith(Parameterized.class)
public class TestLogAppenderWithGrpc
    extends LogAppenderTests<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {
  {
    Slf4jUtils.setLogLevel(FollowerInfo.LOG, Level.DEBUG);
  }

  public TestLogAppenderWithGrpc(Boolean separateHeartbeat) {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
  }

  @Parameterized.Parameters
  public static Collection<Boolean[]> data() {
    return Arrays.asList((new Boolean[][] {{Boolean.FALSE}, {Boolean.TRUE}}));
  }

  @Test
  public void testPendingLimits() throws IOException, InterruptedException {
    int maxAppends = 10;
    RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    GrpcConfigKeys.Server.setLeaderOutstandingAppendsMax(properties, maxAppends);
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 1);
    MiniRaftClusterWithGrpc cluster = getFactory().newCluster(3, properties);
    cluster.start();

    // client and leader setup
    try (final RaftClient client = cluster.createClient(cluster.getGroup())) {
      final RaftServer.Division leader = waitForLeader(cluster);
      RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m"));
      client.io().watch(reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED);
      long initialNextIndex = RaftServerTestUtil.getNextIndex(leader);

      for (RaftServer.Division server : cluster.getFollowers()) {
        // block the appends in the follower
        SimpleStateMachine4Testing.get(server).blockWriteStateMachineData();
      }
      Collection<CompletableFuture<RaftClientReply>> futures = new ArrayList<>(maxAppends * 2);
      for (int i = 0; i < maxAppends * 2; i++) {
        futures.add(client.async().send(new RaftTestUtil.SimpleMessage("m")));
      }

      JavaUtils.attempt(() -> {
        for (long nextIndex : leader.getInfo().getFollowerNextIndices()) {
          // Verify nextIndex does not progress due to pendingRequests limit
          Assert.assertEquals(initialNextIndex + maxAppends, nextIndex);
        }
      }, 10, ONE_SECOND, "matching nextIndex", LOG);
      for (RaftServer.Division server : cluster.getFollowers()) {
        // unblock the appends in the follower
        SimpleStateMachine4Testing.get(server).unblockWriteStateMachineData();
      }

      JavaUtils.allOf(futures).join();
      cluster.shutdown();
    }
  }

  @Test
  public void testRestartLogAppender() throws Exception {
    runWithNewCluster(2, this::runTestRestartLogAppender);
  }

  private void runTestRestartLogAppender(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = waitForLeader(cluster);

    int messageCount = 0;
    // Send some messages
    try(RaftClient client = cluster.createClient(leader.getId())) {
      for(int i = 0; i < 10; i++) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + ++messageCount));
        Assert.assertTrue(reply.isSuccess());
      }
    }

    // assert INCONSISTENCY counter == 0
    final GrpcServerMetrics leaderMetrics = new GrpcServerMetrics(leader.getMemberId().toString());
    final String counter = String.format(GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY,
        cluster.getFollowers().iterator().next().getMemberId().getPeerId());
    Assert.assertEquals(0L, leaderMetrics.getRegistry().counter(counter).getCount());

    // restart LogAppender
    RaftServerTestUtil.restartLogAppenders(leader);

    // Send some more messages
    try(RaftClient client = cluster.createClient(leader.getId())) {
      for(int i = 0; i < 10; i++) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + ++messageCount));
        Assert.assertTrue(reply.isSuccess());
      }
    }

    final RaftServer.Division newLeader = waitForLeader(cluster);
    if (leader == newLeader) {
      final GrpcServerMetrics newleaderMetrics = new GrpcServerMetrics(leader.getMemberId().toString());

      // assert INCONSISTENCY counter >= 1
      // If old LogAppender die before new LogAppender start, INCONSISTENCY equal to 1,
      // else INCONSISTENCY greater than 1
      Assert.assertTrue(newleaderMetrics.getRegistry().counter(counter).getCount() >= 1L);
    }
  }
}
