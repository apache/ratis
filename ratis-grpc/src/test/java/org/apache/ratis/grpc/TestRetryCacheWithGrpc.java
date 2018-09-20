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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RetryCacheTests;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.shaded.proto.RaftProtos;
import org.apache.ratis.util.LogUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestRetryCacheWithGrpc extends RetryCacheTests {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
  }

  private final MiniRaftClusterWithGrpc cluster;

  public TestRetryCacheWithGrpc() throws IOException {
    cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(
        NUM_SERVERS, properties);
    Assert.assertNull(cluster.getLeader());
  }

  @Override
  public MiniRaftClusterWithGrpc getCluster() {
    return cluster;
  }

  @Test
  public void testAsyncRetryWithReplicatedAll() throws Exception {
    final MiniRaftCluster cluster = getCluster();
    RaftTestUtil.waitForLeader(cluster);

    final RaftPeerId leaderId = cluster.getLeaderAndSendFirstMessage().getId();
    long oldLastApplied = cluster.getLeader().getState().getLastAppliedIndex();

    // Kill a follower
    final RaftPeerId killedFollower = cluster.getFollowers().get(0).getId();
    cluster.killServer(killedFollower);

    final long callId = 999;
    final long seqNum = 111;
    final ClientId clientId = ClientId.randomId();

    // Retry with the same clientId and callId
    final List<CompletableFuture<RaftClient>> futures = new ArrayList<>();
    futures.addAll(sendRetry(clientId, leaderId, callId, seqNum, cluster));
    futures.addAll(sendRetry(clientId, leaderId, callId, seqNum, cluster));

    // restart the killed follower
    cluster.restartServer(killedFollower, false);
    for(CompletableFuture<RaftClient> f : futures) {
      f.join().close();
    }
    assertServer(cluster, clientId, callId, oldLastApplied);
  }

  List<CompletableFuture<RaftClient>> sendRetry(
      ClientId clientId, RaftPeerId leaderId, long callId, long seqNum, MiniRaftCluster cluster)
      throws Exception {
    List<CompletableFuture<RaftClient>> futures = new ArrayList<>();
    final int numRequest = 3;
    for (int i = 0; i < numRequest; i++) {
      final RaftClient client = cluster.createClient(leaderId, cluster.getGroup(), clientId);
      final RaftClientRpc rpc = client.getClientRpc();
      final RaftClientRequest request = cluster.newRaftClientRequest(client.getId(), leaderId,
          callId, seqNum, new RaftTestUtil.SimpleMessage("message"), RaftProtos.ReplicationLevel.ALL);

      LOG.info("{} sendRequestAsync {}", i, request);
      futures.add(rpc.sendRequestAsync(request)
          .thenApply(reply -> assertReply(reply, client, callId)));
    }

    for(CompletableFuture<RaftClient> f : futures) {
      try {
        f.get(200, TimeUnit.MILLISECONDS);
        Assert.fail("It should timeout for ReplicationLevel.ALL since a follower is down");
      } catch(TimeoutException te) {
        LOG.info("Expected " + te);
      }
    }
    return futures;
  }
}