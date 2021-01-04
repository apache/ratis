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

import org.apache.ratis.BaseTest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.datastream.DataStreamTestUtils.MultiDataStreamStateMachine;
import org.apache.ratis.datastream.DataStreamTestUtils.SingleDataStream;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.Timestamp;
import org.apache.ratis.util.function.CheckedConsumer;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public abstract class DataStreamClusterTests<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    setStateMachine(MultiDataStreamStateMachine.class);
  }

  public static final int NUM_SERVERS = 3;

  RoutingTable getRoutingTable(Collection<RaftPeer> peers, RaftPeer primary) {
    return DataStreamTestUtils.getRoutingTableChainTopology(peers, primary);
  }

  @Test
  public void testStreamWrites() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::testStreamWrites);
  }

  void testStreamWrites(CLUSTER cluster) throws Exception {
    waitForLeader(cluster);
    runTestDataStreamOutput(cluster);

    // create data file
    final int size = 10_000_000 + ThreadLocalRandom.current().nextInt(1_000_000);
    final File f = new File(getTestDir(), "a.txt");
    DataStreamTestUtils.createFile(f, size);

    for(int i = 0; i < 3; i++) {
      runTestWriteFile(cluster, i, writeAsyncDefaultFileRegion(f, size));
      runTestWriteFile(cluster, i, transferToWritableByteChannel(f, size));
    }
  }

  void runTestDataStreamOutput(CLUSTER cluster) throws Exception {
    final RaftClientRequest request;
    final CompletableFuture<RaftClientReply> reply;
    final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());
    try (RaftClient client = cluster.createClient(primaryServer)) {
      try(final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
          .stream(null, getRoutingTable(cluster.getGroup().getPeers(), primaryServer))) {
        request = out.getHeader();
        reply = out.getRaftClientReplyFuture();

        // write using DataStreamOutput
        DataStreamTestUtils.writeAndAssertReplies(out, 1000, 10);
      }
    }

    watchOrSleep(cluster, reply.join().getLogIndex());
    assertLogEntry(cluster, request);
  }

  void runTestWriteFile(CLUSTER cluster, int i,
      CheckedConsumer<DataStreamOutputImpl, Exception> testCase) throws Exception {
    final RaftClientRequest request;
    final CompletableFuture<RaftClientReply> reply;
    final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());
    try (RaftClient client = cluster.createClient(primaryServer)) {
      try(final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
          .stream(null, getRoutingTable(cluster.getGroup().getPeers(), primaryServer))) {
        request = out.getHeader();
        reply = out.getRaftClientReplyFuture();

        final Timestamp start = Timestamp.currentTime();
        testCase.accept(out);
        LOG.info("{}: {} elapsed {}ms", i, testCase, start.elapsedTimeMs());
      }
    }

    watchOrSleep(cluster, reply.join().getLogIndex());
    assertLogEntry(cluster, request);
  }

  static CheckedConsumer<DataStreamOutputImpl, Exception> transferToWritableByteChannel(File f, int size) {
    return new CheckedConsumer<DataStreamOutputImpl, Exception>() {
      @Override
      public void accept(DataStreamOutputImpl out) throws Exception {
        try (FileInputStream in = new FileInputStream(f)) {
          final long transferred = in.getChannel().transferTo(0, size, out.getWritableByteChannel());
          Assert.assertEquals(size, transferred);
        }
      }

      @Override
      public String toString() {
        return "transferToWritableByteChannel";
      }
    };
  }

  static CheckedConsumer<DataStreamOutputImpl, Exception> writeAsyncDefaultFileRegion(File f, int size) {
    return new CheckedConsumer<DataStreamOutputImpl, Exception>() {
      @Override
      public void accept(DataStreamOutputImpl out) {
        final DataStreamReply dataStreamReply = out.writeAsync(f).join();
        DataStreamTestUtils.assertSuccessReply(Type.STREAM_DATA, size, dataStreamReply);
      }

      @Override
      public String toString() {
        return "writeAsyncDefaultFileRegion";
      }
    };
  }

  void watchOrSleep(CLUSTER cluster, long index) throws Exception {
    try (RaftClient client = cluster.createClient()) {
      client.async().watch(index, ReplicationLevel.ALL).join();
    } catch (UnsupportedOperationException e) {
      // the cluster does not support watch
      ONE_SECOND.sleep();
    }
  }

  void assertLogEntry(CLUSTER cluster, RaftClientRequest request) throws Exception {
    for (RaftServer proxy : cluster.getServers()) {
      final RaftServer.Division impl = proxy.getDivision(cluster.getGroupId());
      final MultiDataStreamStateMachine stateMachine = (MultiDataStreamStateMachine) impl.getStateMachine();
      final SingleDataStream s = stateMachine.getSingleDataStream(request);
      Assert.assertFalse(s.getDataChannel().isOpen());
      DataStreamTestUtils.assertLogEntry(impl, s);
    }
  }
}
