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
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.DataStreamServerImpl;
import org.apache.ratis.server.impl.ServerFactory;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.junit.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

abstract class DataStreamBaseTest extends BaseTest {
  static final int MODULUS = 23;

  static byte pos2byte(int pos) {
    return (byte) ('A' + pos%MODULUS);
  }

  private final Executor executor = Executors.newFixedThreadPool(16);

  static class MultiDataStreamStateMachine extends BaseStateMachine {
    final ConcurrentMap<Long, SingleDataStream> streams = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<DataStream> stream(RaftClientRequest request) {
      final SingleDataStream s = new SingleDataStream();
      streams.put(request.getCallId(), s);
      return s.stream(request);
    }

    SingleDataStream getSingleDataStream(long callId) {
      return streams.get(callId);
    }
  }

  static class SingleDataStream {
    private int byteWritten = 0;
    private RaftClientRequest writeRequest;

    final WritableByteChannel channel = new WritableByteChannel() {
      private volatile boolean open = true;

      @Override
      public int write(ByteBuffer src) {
        if (!open) {
          throw new IllegalStateException("Already closed");
        }
        final int remaining = src.remaining();
        for(; src.remaining() > 0; ) {
          Assert.assertEquals(pos2byte(byteWritten), src.get());
          byteWritten += 1;
        }
        return remaining;
      }

      @Override
      public boolean isOpen() {
        return open;
      }

      @Override
      public void close() {
        open = false;
      }
    };

    final DataStream stream = new DataStream() {
      @Override
      public WritableByteChannel getWritableByteChannel() {
        return channel;
      }

      @Override
      public CompletableFuture<?> cleanUp() {
        try {
          channel.close();
        } catch (Throwable t) {
          return JavaUtils.completeExceptionally(t);
        }
        return CompletableFuture.completedFuture(null);
      }
    };

    public CompletableFuture<DataStream> stream(RaftClientRequest request) {
      writeRequest = request;
      return CompletableFuture.completedFuture(stream);
    }

    public int getByteWritten() {
      return byteWritten;
    }

    public RaftClientRequest getWriteRequest() {
      return writeRequest;
    }
  }

  static class Server {
    private final RaftPeer peer;
    private final RaftServer raftServer;
    private final DataStreamServerImpl dataStreamServer;

    Server(RaftPeer peer, RaftServer raftServer) {
      this.peer = peer;
      this.raftServer = raftServer;
      this.dataStreamServer = new DataStreamServerImpl(raftServer, null);
    }

    RaftPeer getPeer() {
      return peer;
    }

    MultiDataStreamStateMachine getStateMachine(RaftGroupId groupId) throws IOException {
      return (MultiDataStreamStateMachine)raftServer.getStateMachine(groupId);
    }

    void start() {
      dataStreamServer.getServerRpc().start();
    }

    void addRaftPeers(Collection<RaftPeer> peers) {
      dataStreamServer.getServerRpc().addRaftPeers(peers);
    }

    void close() throws IOException {
      dataStreamServer.close();
    }
  }

  protected RaftProperties properties;

  private List<Server> servers;
  private RaftGroup raftGroup;

  Server getPrimaryServer() {
    return servers.get(0);
  }

  protected RaftServer newRaftServer(RaftPeer peer, RaftProperties properties) {
    return new RaftServer() {
      private final ConcurrentMap<RaftGroupId, MultiDataStreamStateMachine> stateMachines = new ConcurrentHashMap<>();

      @Override
      public RaftPeerId getId() {
        return peer.getId();
      }

      @Override
      public StateMachine getStateMachine(RaftGroupId groupId) {
        return stateMachines.computeIfAbsent(groupId, key -> new MultiDataStreamStateMachine());
      }

      @Override
      public RaftProperties getProperties() {
        return properties;
      }


      @Override
      public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) {
        return null;
      }

      @Override
      public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) {
        return null;
      }

      @Override
      public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) {
        return null;
      }

      @Override
      public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(AppendEntriesRequestProto request) {
        return null;
      }

      @Override
      public RpcType getRpcType() {
        return null;
      }

      @Override
      public RaftClientReply submitClientRequest(RaftClientRequest request) {
        return null;
      }

      @Override
      public RaftClientReply setConfiguration(SetConfigurationRequest request) {
        return null;
      }

      @Override
      public CompletableFuture<RaftClientReply> submitClientRequestAsync(RaftClientRequest request) {
        return null;
      }

      @Override
      public CompletableFuture<RaftClientReply> setConfigurationAsync(SetConfigurationRequest request) {
        return null;
      }

      @Override
      public GroupListReply getGroupList(GroupListRequest request) {
        return null;
      }

      @Override
      public GroupInfoReply getGroupInfo(GroupInfoRequest request) {
        return null;
      }

      @Override
      public RaftClientReply groupManagement(GroupManagementRequest request) {
        return null;
      }

      @Override
      public CompletableFuture<GroupListReply> getGroupListAsync(GroupListRequest request) {
        return null;
      }

      @Override
      public CompletableFuture<GroupInfoReply> getGroupInfoAsync(GroupInfoRequest request) {
        return null;
      }

      @Override
      public CompletableFuture<RaftClientReply> groupManagementAsync(GroupManagementRequest request) {
        return null;
      }

      @Override
      public void close() {
      }

      @Override
      public Iterable<RaftGroupId> getGroupIds() {
        return null;
      }

      @Override
      public Iterable<RaftGroup> getGroups() {
        return null;
      }

      @Override
      public ServerFactory getFactory() {
        return null;
      }

      @Override
      public void start() {
      }

      @Override
      public LifeCycle.State getLifeCycleState() {
        return null;
      }
    };
  }


  protected void setup(int numServers){
    final List<RaftPeer> peers = Arrays.stream(MiniRaftCluster.generateIds(numServers, 0))
        .map(RaftPeerId::valueOf)
        .map(id -> RaftPeer.newBuilder().setId(id).setDataStreamAddress(NetUtils.createLocalServerAddress()).build())
        .collect(Collectors.toList());

    List<RaftServer> raftServers = new ArrayList<>();
    peers.forEach(peer -> raftServers.add(newRaftServer(peer, properties)));
    setup(peers, raftServers);
  }


  void setup(List<RaftPeer> peers, List<RaftServer> raftServers) {
    raftGroup = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
    servers = new ArrayList<>(peers.size());
    // start stream servers on raft peers.
    for (int i = 0; i < peers.size(); i++) {
      final Server server = new Server(peers.get(i), raftServers.get(i));
      server.addRaftPeers(removePeerFromList(peers.get(i), peers));
      server.start();
      servers.add(server);
    }
  }

  private Collection<RaftPeer> removePeerFromList(RaftPeer peer, List<RaftPeer> peers) {
    List<RaftPeer> otherPeers = new ArrayList<>(peers);
    otherPeers.remove(peer);
    return otherPeers;
  }

  RaftClient newRaftClientForDataStream() {
    return RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setPrimaryDataStreamServer(getPrimaryServer().getPeer())
        .setProperties(properties)
        .build();
  }

  protected void shutdown() throws IOException {
    for (Server server : servers) {
      server.close();
    }
  }

  void runTestDataStream(int numServers) throws Exception {
    try {
      setup(numServers);
      final List<CompletableFuture<Void>> futures = new ArrayList<>();
      futures.add(CompletableFuture.runAsync(() -> runTestDataStream(5, 10, 1_000_000, 10), executor));
      futures.add(CompletableFuture.runAsync(() -> runTestDataStream(2, 20, 1_000, 10_000), executor));
      futures.forEach(CompletableFuture::join);
    } finally {
      shutdown();
    }
  }

  void runTestDataStream(int numClients, int numStreams, int bufferSize, int bufferNum) {
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int j = 0; j < numClients; j++) {
      futures.add(CompletableFuture.runAsync(() -> runTestDataStream(numStreams, bufferSize, bufferNum), executor));
    }
    Assert.assertEquals(numClients, futures.size());
    futures.forEach(CompletableFuture::join);
  }

  void runTestDataStream(int numStreams, int bufferSize, int bufferNum) {
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    try(RaftClient client = newRaftClientForDataStream()) {
      for (int i = 0; i < numStreams; i++) {
        futures.add(CompletableFuture.runAsync(() -> runTestDataStream(
            (DataStreamOutputImpl) client.getDataStreamApi().stream(), bufferSize, bufferNum), executor));
      }
      Assert.assertEquals(numStreams, futures.size());
      futures.forEach(CompletableFuture::join);
    } catch (IOException e) {
      throw new CompletionException(e);
    }
  }


  void runTestMockCluster(int bufferSize, int bufferNum, RaftClientReply expectedClientReply)
      throws IOException {
    try (final RaftClient client = newRaftClientForDataStream()) {
      final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi().stream();
      runTestDataStream(out, bufferSize, bufferNum);
      DataStreamReplyByteBuffer replyByteBuffer = (DataStreamReplyByteBuffer) out.closeAsync().join();

      final RaftClientReply clientReply = ClientProtoUtils.toRaftClientReply(
          RaftClientReplyProto.parseFrom(replyByteBuffer.slice()));
      Assert.assertEquals(clientReply.getCallId(), expectedClientReply.getCallId());
      Assert.assertEquals(clientReply.getClientId(), expectedClientReply.getClientId());
      Assert.assertEquals(clientReply.getLogIndex(), expectedClientReply.getLogIndex());
      if (expectedClientReply.getException() != null) {
        Assert.assertFalse(replyByteBuffer.isSuccess());
        Assert.assertTrue(clientReply.getException().getMessage().contains(
            expectedClientReply.getException().getMessage()));
      } else {
        Assert.assertTrue(replyByteBuffer.isSuccess());
      }
    }
  }

  private void runTestDataStream(DataStreamOutputImpl out, int bufferSize, int bufferNum) {
    LOG.info("start stream {}", out.getHeader().getCallId());
    final List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
    final List<Integer> sizes = new ArrayList<>();

    //send data
    final int halfBufferSize = bufferSize/2;
    int dataSize = 0;
    for(int i = 0; i < bufferNum; i++) {
      final int size = halfBufferSize + ThreadLocalRandom.current().nextInt(halfBufferSize);
      sizes.add(size);

      final ByteBuffer bf = initBuffer(dataSize, size);
      futures.add(out.writeAsync(bf));
      dataSize += size;
    }

    { // check header
      final DataStreamReply reply = out.getHeaderFuture().join();
      Assert.assertTrue(reply.isSuccess());
      Assert.assertEquals(0, reply.getBytesWritten());
      Assert.assertEquals(reply.getType(), Type.STREAM_HEADER);
    }

    // check writeAsync requests
    for(int i = 0; i < futures.size(); i++) {
      final DataStreamReply reply = futures.get(i).join();
      Assert.assertTrue(reply.isSuccess());
      Assert.assertEquals(sizes.get(i).longValue(), reply.getBytesWritten());
      Assert.assertEquals(reply.getType(), Type.STREAM_DATA);
    }
    try {
      for (Server s : servers) {
        assertHeader(s, out.getHeader(), dataSize);
      }
    } catch (Throwable e) {
      throw new CompletionException(e);
    }
  }

  void assertHeader(Server server, RaftClientRequest header, int dataSize) throws Exception {
    final MultiDataStreamStateMachine s = server.getStateMachine(header.getRaftGroupId());
    final SingleDataStream stream = s.getSingleDataStream(header.getCallId());
    Assert.assertEquals(raftGroup.getGroupId(), header.getRaftGroupId());
    Assert.assertEquals(dataSize, stream.getByteWritten());

    final RaftClientRequest writeRequest = stream.getWriteRequest();
    assertRaftClientRequest(header, writeRequest);
  }

  static void assertRaftClientRequest(RaftClientRequest expected, RaftClientRequest computed) {
    Assert.assertNotNull(computed);
    Assert.assertEquals(expected.getClientId(), computed.getClientId());
    Assert.assertEquals(expected.getServerId(), computed.getServerId());
    Assert.assertEquals(expected.getRaftGroupId(), computed.getRaftGroupId());
    Assert.assertEquals(expected.getCallId(), computed.getCallId());
  }

  static ByteBuffer initBuffer(int offset, int size) {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(size);
    final int length = buffer.capacity();
    buffer.position(0).limit(length);
    for (int j = 0; j < length; j++) {
      buffer.put(pos2byte(offset + j));
    }
    buffer.flip();
    Assert.assertEquals(length, buffer.remaining());
    return buffer;
  }
}
