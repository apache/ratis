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
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.DataStreamClientImpl;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.netty.NettyConfigKeys;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

abstract class DataStreamBaseTest extends BaseTest {
  static final int MODULUS = 23;

  static byte pos2byte(int pos) {
    return (byte) ('A' + pos%MODULUS);
  }

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

  class Server {
    private final RaftPeer peer;
    private final RaftServer raftServer;
    private final DataStreamServerImpl dataStreamServer;

    Server(RaftPeer peer) {
      this.peer = peer;
      this.raftServer = newRaftServer(peer, properties);
      this.dataStreamServer = new DataStreamServerImpl(raftServer, null);
    }

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
        .map(id -> new RaftPeer(id, NetUtils.createLocalServerAddress()))
        .collect(Collectors.toList());

    List<RaftServer> raftServers = new ArrayList<>();
    peers.forEach(peer -> raftServers.add(newRaftServer(peer, properties)));
    setup(peers, raftServers);
  }

  protected void setup(List<RaftServer> raftServers) {
    final List<RaftPeer> peers = new ArrayList<>();
    raftServers.forEach(raftServer ->
        peers.add(new RaftPeer(raftServer.getId(),
            NetUtils.createSocketAddrForHost("http://localhost",
                NettyConfigKeys.DataStream.port(raftServer.getProperties())))));

    setup(peers, raftServers);
  }

  private void setup(List<RaftPeer> peers, List<RaftServer> raftServers){
    raftGroup = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
    servers = new ArrayList<>(peers.size());
    // start stream servers on raft peers.
    for (int i = 0; i < peers.size(); i++) {
      final Server server = new Server(peers.get(i), raftServers.get(i));
      if (i == 0) {
        // only the first server routes requests to peers.
        List<RaftPeer> otherPeers = new ArrayList<>(peers);
        otherPeers.remove(peers.get(i));
        server.addRaftPeers(otherPeers);
      }
      server.start();
      servers.add(server);
    }
  }

  DataStreamClientImpl newDataStreamClientImpl() {
    return new DataStreamClientImpl(getPrimaryServer().getPeer(), properties, null);
  }

  protected void shutdown() throws IOException {
    for (Server server : servers) {
      server.close();
    }
  }

  protected void runTestDataStream(int numServers, int numClients, int numStreams, int bufferSize, int bufferNum)
      throws Exception {
    try {
      setup(numServers);
      runTestDataStream(numClients, numStreams, bufferSize, bufferNum);
    } finally {
      shutdown();
    }
  }

  protected void runTestCloseStream(List<RaftServer> raftServers, int bufferSize, int bufferNum,
      RaftClientReply expectedClientReply) throws Exception {
    try {
      setup(raftServers);
      runTestCloseStream(bufferSize, bufferNum, expectedClientReply);
    } finally {
      shutdown();
    }
  }

  private void runTestDataStream(int numClients, int numStreams, int bufferSize, int bufferNum) throws Exception {
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    final List<DataStreamClientImpl> clients = new ArrayList<>();
    try {
      for (int j = 0; j < numClients; j++) {
        final DataStreamClientImpl client = newDataStreamClientImpl();
        clients.add(client);
        for (int i = 0; i < numStreams; i++) {
          futures.add(CompletableFuture.runAsync(
              () -> runTestDataStream((DataStreamOutputImpl) client.stream(raftGroup.getGroupId()), bufferSize, bufferNum)));
        }
      }
      Assert.assertEquals(numClients*numStreams, futures.size());
      futures.forEach(CompletableFuture::join);
    } finally {
      for (int j = 0; j < numClients; j++) {
        clients.get(j).close();
      }
    }
  }

  private void runTestCloseStream(int bufferSize, int bufferNum, RaftClientReply expectedClientReply)
      throws IOException {
    try (final DataStreamClientImpl client = newDataStreamClientImpl()) {
      DataStreamOutputImpl out = (DataStreamOutputImpl) client.stream(raftGroup.getGroupId());
      runTestDataStream(out, bufferSize, bufferNum);
      DataStreamReplyByteBuffer replyByteBuffer = (DataStreamReplyByteBuffer) out.closeAsync().join();

      final RaftClientReply clientReply = ClientProtoUtils.toRaftClientReply(
          RaftClientReplyProto.parseFrom(replyByteBuffer.slice()));
      Assert.assertTrue(replyByteBuffer.isSuccess());
      Assert.assertEquals(clientReply.getCallId(), expectedClientReply.getCallId());
      Assert.assertTrue(clientReply.getClientId().equals(expectedClientReply.getClientId()));
      Assert.assertEquals(clientReply.getLogIndex(), expectedClientReply.getLogIndex());
    }
  }

  private void runTestDataStream(DataStreamOutputImpl out, int bufferSize, int bufferNum) {
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
      assertHeader(out.getHeader(), dataSize);
    } catch (Throwable e) {
      throw new CompletionException(e);
    }
  }

  void assertHeader(RaftClientRequest header, int dataSize) throws Exception {
    final Server server = getPrimaryServer();
    final MultiDataStreamStateMachine s = server.getStateMachine(header.getRaftGroupId());
    final SingleDataStream stream = s.getSingleDataStream(header.getCallId());
    final RaftClientRequest writeRequest = stream.getWriteRequest();
    Assert.assertEquals(writeRequest.getCallId(), header.getCallId());
    Assert.assertEquals(writeRequest.getRaftGroupId(), header.getRaftGroupId());
    Assert.assertEquals(raftGroup.getGroupId(), header.getRaftGroupId());
    Assert.assertEquals(writeRequest.getServerId(), header.getServerId());
    Assert.assertEquals(dataSize, stream.getByteWritten());
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
