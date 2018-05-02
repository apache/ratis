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
package org.apache.ratis.grpc;

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftBasicTests;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public class TestRaftWithGrpc extends RaftBasicTests {
  private final MiniRaftClusterWithGRpc cluster;

  public TestRaftWithGrpc() throws IOException {
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    cluster = MiniRaftClusterWithGRpc.FACTORY.newCluster(
        NUM_SERVERS, properties);
    Assert.assertNull(cluster.getLeader());
  }

  @Override
  public MiniRaftClusterWithGRpc getCluster() {
    return cluster;
  }

  @Override
  @Test
  public void testWithLoad() throws Exception {
    super.testWithLoad();
    BlockRequestHandlingInjection.getInstance().unblockAll();
  }

  @Test
  public void testRequestTimeout() throws Exception {
    testRequestTimeout(false, getCluster(), LOG);
  }

  @Test
  public void testUpdateViaHeartbeat()
      throws IOException, InterruptedException, ExecutionException {
    LOG.info("Running testUpdateViaHeartbeat");
    final MiniRaftClusterWithGRpc cluster = getCluster();
    waitForLeader(cluster);
    long waitTime = 5000;
    try (final RaftClient client = cluster.createClient()) {
      // block append requests
      cluster.getServerAliveStream().forEach(raftServer -> {
        try {
          if (!raftServer.isLeader()) {
            ((SimpleStateMachine4Testing) raftServer.getStateMachine()).setBlockAppend(true);
          }
        } catch (InterruptedException e) {
          LOG.error("Interrupted while blocking append", e);
        }
      });
      CompletableFuture<RaftClientReply>
          replyFuture = client.sendAsync(new RaftTestUtil.SimpleMessage("abc"));
      Thread.sleep(waitTime);
      // replyFuture should not be completed until append request is unblocked.
      Assert.assertTrue(!replyFuture.isDone());
      // unblock append request.
      cluster.getServerAliveStream().forEach(raftServer -> {
        try {
          ((SimpleStateMachine4Testing) raftServer.getStateMachine()).setBlockAppend(false);
        } catch (InterruptedException e) {
          LOG.error("Interrupted while unblocking append", e);
        }
      });
      long index = cluster.getLeader().getState().getLog().getNextIndex();
      TermIndex[] leaderEntries = cluster.getLeader().getState().getLog().getEntries(0, Integer.MAX_VALUE);
      // The entries have been appended in the followers
      // although the append entry timed out at the leader
      cluster.getServerAliveStream().forEach(raftServer -> {
        Assert.assertEquals(raftServer.getState().getLog().getNextIndex(), index);
        if (!raftServer.isLeader()) {
          TermIndex[] serverEntries = raftServer.getState().getLog().getEntries(0, Integer.MAX_VALUE);
          Arrays.equals(serverEntries, leaderEntries);
        }
      });

      // Wait for heartbeats from leader to be received by followers
      Thread.sleep(1000);
      RaftServerTestUtil.getLogAppenders(cluster.getLeader()).forEach(logAppender -> {
        // FollowerInfo in the leader state should have updated next and match index.
        Assert.assertEquals(logAppender.getFollower().getMatchIndex(), index - 1);
        Assert.assertEquals(logAppender.getFollower().getNextIndex(), index);
      });
    }
    cluster.shutdown();
  }
}
