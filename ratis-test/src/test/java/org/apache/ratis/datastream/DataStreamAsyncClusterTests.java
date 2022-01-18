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
package org.apache.ratis.datastream;

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.datastream.DataStreamTestUtils.MultiDataStreamStateMachine;
import org.apache.ratis.datastream.DataStreamTestUtils.SingleDataStream;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class DataStreamAsyncClusterTests<CLUSTER extends MiniRaftCluster>
    extends DataStreamClusterTests<CLUSTER> {
  final Executor executor = Executors.newFixedThreadPool(16);

  @Override
  public int getGlobalTimeoutSeconds() {
    return 300;
  }

  @Test
  public void testMultipleStreamsSingleServer() throws Exception {
    runWithNewCluster(1, this::runTestDataStream);
  }

  @Test
  public void testMultipleStreamsMultipleServers() throws Exception {
    // Avoid changing leader
    final TimeDuration min = RaftServerConfigKeys.Rpc.timeoutMin(getProperties());
    RaftServerConfigKeys.Rpc.setTimeoutMin(getProperties(), TimeDuration.valueOf(2, TimeUnit.SECONDS));
    final TimeDuration max = RaftServerConfigKeys.Rpc.timeoutMax(getProperties());
    RaftServerConfigKeys.Rpc.setTimeoutMax(getProperties(), TimeDuration.valueOf(3, TimeUnit.SECONDS));

    runWithNewCluster(3, this::runTestDataStream);

    // Reset
    RaftServerConfigKeys.Rpc.setTimeoutMin(getProperties(), min);
    RaftServerConfigKeys.Rpc.setTimeoutMax(getProperties(), max);
  }

  @Test
  public void testMultipleStreamsMultipleServersStepDownLeader() throws Exception {
    runWithNewCluster(3, this::runTestDataStreamStepDownLeader);
  }

  void runTestDataStreamStepDownLeader(CLUSTER cluster) throws Exception {
    runTestDataStream(cluster, true);
  }

  void runTestDataStream(CLUSTER cluster) throws Exception {
    runTestDataStream(cluster, false);
  }

  void runTestDataStream(CLUSTER cluster, boolean stepDownLeader) throws Exception {
    RaftTestUtil.waitForLeader(cluster);

    final List<CompletableFuture<Long>> futures = new ArrayList<>();
    futures.add(CompletableFuture.supplyAsync(() -> runTestDataStream(cluster, 5, 10, 1_000_000, 10, stepDownLeader), executor));
    futures.add(CompletableFuture.supplyAsync(() -> runTestDataStream(cluster, 2, 20, 1_000, 5_000, stepDownLeader), executor));
    final long maxIndex = futures.stream()
        .map(CompletableFuture::join)
        .max(Long::compareTo)
        .orElseThrow(IllegalStateException::new);

    if (stepDownLeader) {
      final RaftPeerId oldLeader = cluster.getLeader().getId();
      final CompletableFuture<RaftPeerId> changeLeader = futures.get(0).thenApplyAsync(dummy -> {
        try {
          return RaftTestUtil.changeLeader(cluster, oldLeader);
        } catch (Exception e) {
          throw new CompletionException("Failed to change leader from " + oldLeader, e);
        }
      });
      LOG.info("Changed leader from {} to {}", oldLeader, changeLeader.join());
    }

    // wait for all servers to catch up
    try (RaftClient client = cluster.createClient()) {
      RaftClientReply reply = client.async().watch(maxIndex, ReplicationLevel.ALL).join();
      Assert.assertTrue(reply.isSuccess());
    }
    // assert all streams are linked
    for (RaftServer proxy : cluster.getServers()) {
      final RaftServer.Division impl = proxy.getDivision(cluster.getGroupId());
      final MultiDataStreamStateMachine stateMachine = (MultiDataStreamStateMachine) impl.getStateMachine();
      for (SingleDataStream s : stateMachine.getStreams()) {
        Assert.assertFalse(s.getDataChannel().isOpen());
        DataStreamTestUtils.assertLogEntry(impl, s);
      }
    }
  }

  Long runTestDataStream(
      CLUSTER cluster, int numClients, int numStreams, int bufferSize, int bufferNum, boolean stepDownLeader) {
    final List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int j = 0; j < numClients; j++) {
      futures.add(CompletableFuture.supplyAsync(
          () -> runTestDataStream(cluster, numStreams, bufferSize, bufferNum, stepDownLeader), executor));
    }
    Assert.assertEquals(numClients, futures.size());
    return futures.stream()
        .map(CompletableFuture::join)
        .max(Long::compareTo)
        .orElseThrow(IllegalStateException::new);
  }

  ClientId getPrimaryClientId(CLUSTER cluster, RaftPeer primary) {
    return cluster.getDivision(primary.getId()).getRaftClient().getId();
  }

  long runTestDataStream(CLUSTER cluster, int numStreams, int bufferSize, int bufferNum, boolean stepDownLeader) {
    final Iterable<RaftServer> servers = CollectionUtils.as(cluster.getServers(), s -> s);
    final RaftPeerId leader = cluster.getLeader().getId();
    final List<CompletableFuture<RaftClientReply>> futures = new ArrayList<>();
    final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());
    try(RaftClient client = cluster.createClient(primaryServer)) {
      ClientId primaryClientId = getPrimaryClientId(cluster, primaryServer);
      for (int i = 0; i < numStreams; i++) {
        final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
            .stream(null, getRoutingTable(cluster.getGroup().getPeers(), primaryServer));
        futures.add(CompletableFuture.supplyAsync(() -> DataStreamTestUtils.writeAndCloseAndAssertReplies(
            servers, leader, out, bufferSize, bufferNum, primaryClientId, client.getId(), stepDownLeader).join(), executor));
      }
      Assert.assertEquals(numStreams, futures.size());
      return futures.stream()
          .map(CompletableFuture::join)
          .map(RaftClientReply::getLogIndex)
          .max(Long::compareTo)
          .orElseThrow(IllegalStateException::new);
    } catch (IOException e) {
      throw new CompletionException(e);
    }
  }
}
