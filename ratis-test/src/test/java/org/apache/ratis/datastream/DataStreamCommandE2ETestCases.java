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

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.datastream.DataStreamTestUtils.MultiDataStreamStateMachine;
import org.apache.ratis.datastream.DataStreamTestUtils.SingleDataStream;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.util.CollectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * End-to-end cluster tests for {@link Type#STREAM_COMMAND}.
 */
@Timeout(value = 300)
public interface DataStreamCommandE2ETestCases {

  MiniRaftCluster.Factory.Get<?> getClusterFactory();

  /** Routing table used by command e2e tests; override to match cluster topology. */
  default RoutingTable routingTable(Collection<RaftPeer> peers, RaftPeer primary) {
    return DataStreamTestUtils.getRoutingTableChainTopology(peers, primary);
  }

  @Test
  default void testStreamCommand() throws Exception {
    getClusterFactory().setStateMachine(MultiDataStreamStateMachine.class);
    getClusterFactory().runWithNewCluster(3, cluster -> {
      RaftTestUtil.waitForLeader(cluster);
      final Iterable<RaftServer> servers = CollectionUtils.as(cluster.getServers(), s -> s);
      final RaftPeerId leader = cluster.getLeader().getId();
      final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());
      final List<ByteBuffer> streamCommands = Arrays.asList(
          DataStreamTestUtils.recordCommand('c', 't', 'r', 'l', '1'),
          DataStreamTestUtils.recordCommand('c', 't', 'r', 'l', '2'));

      try (RaftClient client = cluster.createClient(primaryServer)) {
        final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
            .stream(null, routingTable(cluster.getGroup().getPeers(), primaryServer));
        final List<Long> commandOffsets = new ArrayList<>();
        DataStreamTestUtils.writeAndCloseAndAssertReplies(
            servers, leader, out, 1_000, 3, client.getId(), false, streamCommands, commandOffsets).join();

        DataStreamTestUtils.assertCommandsOnAllServers(
            servers, out.getHeader(), streamCommands, commandOffsets);
      }
    });
  }

  @Test
  default void testStreamCommandConsecutiveAtSameOffset() throws Exception {
    getClusterFactory().setStateMachine(MultiDataStreamStateMachine.class);
    getClusterFactory().runWithNewCluster(3, cluster -> {
      RaftTestUtil.waitForLeader(cluster);
      final Iterable<RaftServer> servers = CollectionUtils.as(cluster.getServers(), s -> s);
      final RaftPeerId leader = cluster.getLeader().getId();
      final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());
      final List<ByteBuffer> streamCommands = Arrays.asList(
          DataStreamTestUtils.recordCommand('a'),
          DataStreamTestUtils.recordCommand('b'),
          DataStreamTestUtils.recordCommand('c'));
      final List<Long> commandOffsets = Arrays.asList(512L, 512L, 512L);

      try (RaftClient client = cluster.createClient(primaryServer)) {
        final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
            .stream(null, routingTable(cluster.getGroup().getPeers(), primaryServer));
        out.getHeaderFuture().join();

        DataStreamTestUtils.assertSuccessReply(Type.STREAM_DATA, 512,
            out.writeAsync(DataStreamTestUtils.initBuffer(0, 512), StandardWriteOption.FLUSH).join());

        for (ByteBuffer command : streamCommands) {
          DataStreamTestUtils.assertSuccessReply(Type.STREAM_COMMAND, 0, out.commandAsync(command).join());
        }

        DataStreamTestUtils.assertSuccessReply(Type.STREAM_DATA, 256,
            out.writeAsync(DataStreamTestUtils.initBuffer(512, 256),
                StandardWriteOption.FLUSH, StandardWriteOption.SYNC).join());

        final int totalBytes = 768;
        DataStreamTestUtils.assertCommandsOnAllServers(
            servers, out.getHeader(), streamCommands, commandOffsets);
        final RaftClientReply raftReply = DataStreamTestUtils.closeStreamAndAssertReplies(
            out, servers, leader, client.getId(), totalBytes, false).join();
        Assertions.assertTrue(raftReply.isSuccess());
      }
    });
  }

  @Test
  default void testStreamCommandForceReplicated() throws Exception {
    getClusterFactory().setStateMachine(MultiDataStreamStateMachine.class);
    getClusterFactory().runWithNewCluster(3, cluster -> {
      RaftTestUtil.waitForLeader(cluster);
      final Iterable<RaftServer> servers = CollectionUtils.as(cluster.getServers(), s -> s);
      final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());
      final int dataSize = 2_048;

      try (RaftClient client = cluster.createClient(primaryServer)) {
        final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
            .stream(null, routingTable(cluster.getGroup().getPeers(), primaryServer));
        out.getHeaderFuture().join();

        DataStreamTestUtils.assertSuccessReply(Type.STREAM_DATA, dataSize,
            out.writeAsync(DataStreamTestUtils.initBuffer(0, dataSize), StandardWriteOption.FLUSH).join());
        DataStreamTestUtils.assertSuccessReply(Type.STREAM_COMMAND, 0,
            out.commandAsync(DataStreamTestUtils.forceCommand()).join());

        DataStreamTestUtils.assertSuccessReply(Type.STREAM_DATA, 512,
            out.writeAsync(DataStreamTestUtils.initBuffer(dataSize, 512), StandardWriteOption.FLUSH).join());

        DataStreamTestUtils.assertCommandsOnAllServers(servers, out.getHeader(),
            Arrays.asList(DataStreamTestUtils.forceCommand()), Arrays.asList((long) dataSize));
        final RaftPeerId leader = cluster.getLeader().getId();
        final int totalBytes = dataSize + 512;
        final RaftClientReply raftReply = DataStreamTestUtils.assertCloseReply(
            out, out.closeAsync().join(), totalBytes, leader, client.getId(), false).join();
        Assertions.assertTrue(raftReply.isSuccess());

        for (RaftServer server : servers) {
          final SingleDataStream stream = getStream(server, out);
          Assertions.assertEquals(1, stream.getDataChannel().getForceCount());
          Assertions.assertEquals(totalBytes, stream.getDataChannel().getBytesWritten());
          Assertions.assertEquals(dataSize, stream.getDataChannel().getForcedPosition());
        }
      }
    });
  }

  @Test
  default void testStreamCommandMultipleStreams() throws Exception {
    getClusterFactory().setStateMachine(MultiDataStreamStateMachine.class);
    getClusterFactory().runWithNewCluster(3, cluster -> {
      RaftTestUtil.waitForLeader(cluster);
      final Iterable<RaftServer> servers = CollectionUtils.as(cluster.getServers(), s -> s);
      final RaftPeerId leader = cluster.getLeader().getId();
      final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());
      final List<CompletableFuture<Void>> futures = new ArrayList<>();

      try (RaftClient client = cluster.createClient(primaryServer)) {
        for (int streamIndex = 0; streamIndex < 3; streamIndex++) {
          final int index = streamIndex;
          futures.add(CompletableFuture.runAsync(() -> {
            final List<ByteBuffer> streamCommands = Arrays.asList(
                DataStreamTestUtils.recordCommand('s', '0' + index));
            final List<Long> commandOffsets = new ArrayList<>();
            try {
              final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
                  .stream(null, routingTable(cluster.getGroup().getPeers(), primaryServer));
              DataStreamTestUtils.writeAndCloseAndAssertReplies(
                  servers, leader, out, 500, 2, client.getId(), false, streamCommands, commandOffsets).join();
              DataStreamTestUtils.assertCommandsOnAllServers(
                  servers, out.getHeader(), streamCommands, commandOffsets);
            } catch (Exception e) {
              throw new CompletionException(e);
            }
          }));
        }
        futures.forEach(CompletableFuture::join);
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    });
  }

  @Test
  default void testStreamCommandWithStepDownLeader() throws Exception {
    getClusterFactory().setStateMachine(MultiDataStreamStateMachine.class);
    getClusterFactory().runWithNewCluster(3, cluster -> {
      RaftTestUtil.waitForLeader(cluster);
      final Iterable<RaftServer> servers = CollectionUtils.as(cluster.getServers(), s -> s);
      final RaftPeerId leader = cluster.getLeader().getId();
      final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());
      final List<ByteBuffer> streamCommands = Arrays.asList(
          DataStreamTestUtils.recordCommand('l', 'e', 'a', 'd'));
      final List<Long> commandOffsets = new ArrayList<>();

      try (RaftClient client = cluster.createClient(primaryServer)) {
        final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
            .stream(null, routingTable(cluster.getGroup().getPeers(), primaryServer));
        final RaftClientReply reply = DataStreamTestUtils.writeAndCloseAndAssertReplies(
            servers, leader, out, 800, 2, client.getId(), true, streamCommands, commandOffsets).join();
        Assertions.assertTrue(reply.isSuccess());

        DataStreamTestUtils.assertCommandsOnAllServers(
            servers, out.getHeader(), streamCommands, commandOffsets);

        try (RaftClient watchClient = cluster.createClient()) {
          Assertions.assertTrue(watchClient.async()
              .watch(reply.getLogIndex(), ReplicationLevel.ALL).join().isSuccess());
        }
        for (RaftServer server : cluster.getServers()) {
          DataStreamTestUtils.assertLogEntry(
              server.getDivision(cluster.getGroupId()), getStream(server, out));
        }
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    });
  }

  @Test
  default void testStreamCommandAfterCloseFails() throws Exception {
    getClusterFactory().setStateMachine(MultiDataStreamStateMachine.class);
    getClusterFactory().runWithNewCluster(3, cluster -> {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeer primaryServer = CollectionUtils.random(cluster.getGroup().getPeers());

      try (RaftClient client = cluster.createClient(primaryServer)) {
        final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
            .stream(null, routingTable(cluster.getGroup().getPeers(), primaryServer));
        out.getHeaderFuture().join();
        out.writeAsync(DataStreamTestUtils.initBuffer(0, 64), StandardWriteOption.FLUSH).join();
        out.closeAsync().join();

        final ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
            () -> out.commandAsync(DataStreamTestUtils.recordCommand('x')).get());
        Assertions.assertInstanceOf(AlreadyClosedException.class, exception.getCause());
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    });
  }

  static SingleDataStream getStream(RaftServer server, DataStreamOutputImpl out) throws Exception {
    final MultiDataStreamStateMachine stateMachine = (MultiDataStreamStateMachine)
        server.getDivision(out.getHeader().getRaftGroupId()).getStateMachine();
    return stateMachine.getSingleDataStream(out.getHeader());
  }
}
