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

import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftBasicTests;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public class TestRaftWithGrpc
    extends RaftBasicTests<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {

  {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  public static Collection<Boolean[]> data() {
    return Arrays.asList((new Boolean[][] {{Boolean.FALSE}, {Boolean.TRUE}}));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testWithLoad(Boolean separateHeartbeat) throws Exception {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
    super.testWithLoad();
    BlockRequestHandlingInjection.getInstance().unblockAll();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRequestTimeout(Boolean separateHeartbeat) throws Exception {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
    runWithNewCluster(NUM_SERVERS, cluster -> testRequestTimeout(false, cluster, LOG));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testUpdateViaHeartbeat(Boolean separateHeartbeat) throws Exception {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
    runWithNewCluster(NUM_SERVERS, this::runTestUpdateViaHeartbeat);
  }

  void runTestUpdateViaHeartbeat(MiniRaftClusterWithGrpc cluster) throws Exception {
    waitForLeader(cluster);
    try (final RaftClient client = cluster.createClient()) {
      // block append requests
      cluster.getServerAliveStream()
          .filter(impl -> !impl.getInfo().isLeader())
          .map(SimpleStateMachine4Testing::get)
          .forEach(SimpleStateMachine4Testing::blockWriteStateMachineData);

      CompletableFuture<RaftClientReply>
          replyFuture = client.async().send(new RaftTestUtil.SimpleMessage("abc"));
      TimeDuration.valueOf(5 , TimeUnit.SECONDS).sleep();
      // replyFuture should not be completed until append request is unblocked.
      Assertions.assertFalse(replyFuture.isDone());
      // unblock append request.
      cluster.getServerAliveStream()
          .filter(impl -> !impl.getInfo().isLeader())
          .map(SimpleStateMachine4Testing::get)
          .forEach(SimpleStateMachine4Testing::unblockWriteStateMachineData);

      final RaftLog leaderLog = cluster.getLeader().getRaftLog();
      // The entries have been appended in the followers
      // although the append entry timed out at the leader
      cluster.getServerAliveStream().filter(impl -> !impl.getInfo().isLeader()).forEach(raftServer ->
          JavaUtils.runAsUnchecked(() -> JavaUtils.attempt(() -> {
        final long leaderNextIndex = leaderLog.getNextIndex();
        final LogEntryHeader[] leaderEntries = leaderLog.getEntries(0, Long.MAX_VALUE);

        final RaftLog followerLog = raftServer.getRaftLog();
        Assertions.assertEquals(leaderNextIndex, followerLog.getNextIndex());
        final LogEntryHeader[] serverEntries = followerLog.getEntries(0, Long.MAX_VALUE);
        Assertions.assertArrayEquals(serverEntries, leaderEntries);
      }, 10, HUNDRED_MILLIS, "assertRaftLog-" + raftServer.getId(), LOG)));

      // Wait for heartbeats from leader to be received by followers
      Thread.sleep(500);
      RaftServerTestUtil.getLogAppenders(cluster.getLeader()).forEach(logAppender ->
          JavaUtils.runAsUnchecked(() -> JavaUtils.attempt(() -> {
        final long leaderNextIndex = leaderLog.getNextIndex();
        // FollowerInfo in the leader state should have updated next and match index.
        final long followerMatchIndex = logAppender.getFollower().getMatchIndex();
        Assertions.assertTrue(followerMatchIndex >= leaderNextIndex - 1);
        Assertions.assertEquals(followerMatchIndex + 1, logAppender.getFollower().getNextIndex());
      }, 10, HUNDRED_MILLIS, "assertRaftLog-" + logAppender.getFollower(), LOG)));
    }
  }
}
