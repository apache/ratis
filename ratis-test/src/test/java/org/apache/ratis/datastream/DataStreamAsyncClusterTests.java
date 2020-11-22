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

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public abstract class DataStreamAsyncClusterTests<CLUSTER extends MiniRaftCluster>
    extends DataStreamClusterTests<CLUSTER> {
  final Executor executor = Executors.newFixedThreadPool(16);

  @Test
  public void testMultipleStreamsSingleServer() throws Exception {
    runWithNewCluster(1, this::runTestDataStream);
  }

  @Test
  public void testMultipleStreamsMultipleServers() throws Exception {
    runWithNewCluster(3, this::runTestDataStream);
  }

  void runTestDataStream(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    futures.add(CompletableFuture.runAsync(() -> runTestDataStream(cluster, 5, 10, 1_000_000, 10), executor));
    futures.add(CompletableFuture.runAsync(() -> runTestDataStream(cluster, 2, 20, 1_000, 10_000), executor));
    futures.forEach(CompletableFuture::join);
  }

  void runTestDataStream(CLUSTER cluster, int numClients, int numStreams, int bufferSize, int bufferNum) {
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int j = 0; j < numClients; j++) {
      futures.add(CompletableFuture.runAsync(() -> runTestDataStream(cluster, numStreams, bufferSize, bufferNum), executor));
    }
    Assert.assertEquals(numClients, futures.size());
    futures.forEach(CompletableFuture::join);
  }

  void runTestDataStream(CLUSTER cluster, int numStreams, int bufferSize, int bufferNum) {
    final Iterable<RaftServer> servers = CollectionUtils.as(cluster.getServers(), s -> s);
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    try(RaftClient client = cluster.createClient()) {
      for (int i = 0; i < numStreams; i++) {
        final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi().stream();
        futures.add(CompletableFuture.runAsync(() -> DataStreamTestUtils.writeAndCloseAndAssertReplies(
            servers, out, bufferSize, bufferNum), executor));
      }
      Assert.assertEquals(numStreams, futures.size());
      futures.forEach(CompletableFuture::join);
    } catch (IOException e) {
      throw new CompletionException(e);
    }
  }
}
