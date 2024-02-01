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

import org.apache.ratis.netty.client.NettyClientStreamRpc;
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
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

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
  public void testSingleStreamsMultipleServers() throws Exception {
    Slf4jUtils.setLogLevel(NettyClientStreamRpc.LOG, Level.TRACE);
    try {
      runWithNewCluster(3,
          cluster -> runTestDataStream(cluster, false,
              (c, stepDownLeader) -> runTestDataStream(c, 1, 1, 1_000, 3, stepDownLeader)));
    } finally {
      Slf4jUtils.setLogLevel(NettyClientStreamRpc.LOG, Level.INFO);
    }
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
    runMultipleStreams(cluster, true);
  }

  void runTestDataStream(CLUSTER cluster) throws Exception {
    runTestDataStream(cluster, false, this::runMultipleStreams);
  }

  long runMultipleStreams(CLUSTER cluster, boolean stepDownLeader) {
    final List<CompletableFuture<Long>> futures = new ArrayList<>();
    futures.add(CompletableFuture.supplyAsync(() -> runTestDataStream(cluster, 5, 10, 100_000, 10, stepDownLeader), executor));
    futures.add(CompletableFuture.supplyAsync(() -> runTestDataStream(cluster, 2, 20, 1_000, 5_000, stepDownLeader), executor));
    return futures.stream()
        .map(CompletableFuture::join)
        .max(Long::compareTo)
        .orElseThrow(IllegalStateException::new);
  }

  void runTestDataStream(CLUSTER cluster, boolean stepDownLeader, CheckedBiFunction<CLUSTER, Boolean, Long, Exception> runMethod) throws Exception {
    RaftTestUtil.waitForLeader(cluster);

    final long maxIndex = runMethod.apply(cluster, stepDownLeader);

    if (stepDownLeader) {
      final RaftPeerId oldLeader = cluster.getLeader().getId();
      final RaftPeerId changed;
      try {
        changed = RaftTestUtil.changeLeader(cluster, oldLeader);
      } catch (Exception e) {
        throw new CompletionException("Failed to change leader from " + oldLeader, e);
      }
      LOG.info("Changed leader from {} to {}", oldLeader, changed);
    }

    // wait for all servers to catch up
    try (RaftClient client = cluster.createClient()) {
      RaftClientReply reply = client.async().watch(maxIndex, ReplicationLevel.ALL).join();
      Assertions.assertTrue(reply.isSuccess());
    }
    // assert all streams are linked
    for (RaftServer proxy : cluster.getServers()) {
      final RaftServer.Division impl = proxy.getDivision(cluster.getGroupId());
      final MultiDataStreamStateMachine stateMachine = (MultiDataStreamStateMachine) impl.getStateMachine();
      for (SingleDataStream s : stateMachine.getStreams()) {
        Assertions.assertFalse(s.getDataChannel().isOpen());
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
    Assertions.assertEquals(numClients, futures.size());
    return futures.stream()
        .map(CompletableFuture::join)
        .max(Long::compareTo)
        .orElseThrow(IllegalStateException::new);
  }

  long runTestDataStream(CLUSTER cluster, int numStreams, int bufferSize, int bufferNum, boolean stepDownLeader) {
    final Iterable<RaftServer> servers = CollectionUtils.as(cluster.getServers(), s -> s);
    final RaftPeerId leader;
    try {
      leader = RaftTestUtil.waitForLeader(cluster).getId();
    } catch (InterruptedException e) {
      throw new CompletionException(e);
    }
    final List<CompletableFuture<RaftClientReply>> futures = new ArrayList<>();
    final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());
    try(RaftClient client = cluster.createClient(primaryServer)) {
      for (int i = 0; i < numStreams; i++) {
        final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
            .stream(null, getRoutingTable(cluster.getGroup().getPeers(), primaryServer));
        futures.add(CompletableFuture.supplyAsync(() -> DataStreamTestUtils.writeAndCloseAndAssertReplies(
            servers, leader, out, bufferSize, bufferNum, client.getId(), stepDownLeader).join(), executor));
      }
      Assertions.assertEquals(numStreams, futures.size());
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
