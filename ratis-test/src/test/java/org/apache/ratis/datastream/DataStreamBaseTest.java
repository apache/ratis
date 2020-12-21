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
import org.apache.ratis.server.DataStreamServer;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.DivisionProperties;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.DataStreamTestUtils.MyDataChannel;
import org.apache.ratis.datastream.DataStreamTestUtils.MultiDataStreamStateMachine;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
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
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.server.DataStreamMap;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.ServerFactory;
import org.apache.ratis.server.metrics.RaftServerMetrics;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataChannel;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

abstract class DataStreamBaseTest extends BaseTest {
  class MyDivision implements RaftServer.Division {
    private final RaftServer server;
    private final MultiDataStreamStateMachine stateMachine = new MultiDataStreamStateMachine();
    private final DataStreamMap streamMap;
    private RaftClient client;

    MyDivision(RaftServer server) {
      this.server = server;
      this.streamMap = RaftServerTestUtil.newDataStreamMap(server.getId());
    }

    @Override
    public DivisionProperties properties() {
      return null;
    }

    @Override
    public RaftGroupMemberId getMemberId() {
      return null;
    }

    @Override
    public DivisionInfo getInfo() {
      return null;
    }

    @Override
    public RaftConfiguration getRaftConf() {
      final List<RaftPeer> peers = servers.stream().map(Server::getPeer).collect(Collectors.toList());
      return RaftServerTestUtil.newRaftConfiguration(peers);
    }

    @Override
    public RaftServer getRaftServer() {
      return server;
    }

    @Override
    public RaftServerMetrics getRaftServerMetrics() {
      return null;
    }

    @Override
    public MultiDataStreamStateMachine getStateMachine() {
      return stateMachine;
    }

    @Override
    public RaftLog getRaftLog() {
      return null;
    }

    @Override
    public RaftStorage getRaftStorage() {
      return null;
    }

    @Override
    public RetryCache getRetryCache() {
      return null;
    }

    @Override
    public DataStreamMap getDataStreamMap() {
      return streamMap;
    }

    public void setRaftClient(RaftClient client) {
      this.client = client;
    }

    @Override
    public RaftClient getRaftClient() {
      return this.client;
    }

    @Override
    public void close() {}
  }

  static class Server {
    private final RaftPeer peer;
    private final RaftServer raftServer;
    private final DataStreamServer dataStreamServer;

    Server(RaftPeer peer, RaftServer raftServer) {
      this.peer = peer;
      this.raftServer = raftServer;
      this.dataStreamServer = RaftServerTestUtil.newDataStreamServer(raftServer);
    }

    RaftPeer getPeer() {
      return peer;
    }

    RaftServer getRaftServer() {
      return raftServer;
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
  private List<RaftPeer> peers;
  private RaftGroup raftGroup;

  Server getPrimaryServer() {
    return servers.get(0);
  }

  protected MyRaftServer newRaftServer(RaftPeer peer, RaftProperties properties) {
    return new MyRaftServer(peer, properties);
  }

  class MyRaftServer implements RaftServer {
      private final RaftPeer peer;
      private final RaftProperties properties;
      private final ConcurrentMap<RaftGroupId, MyDivision> divisions = new ConcurrentHashMap<>();

      MyRaftServer(RaftPeer peer, RaftProperties properties) {
        this.peer = peer;
        this.properties = properties;
      }

      @Override
      public RaftPeerId getId() {
        return peer.getId();
      }

      @Override
      public RaftPeer getPeer() {
        return peer;
      }

      @Override
      public MyDivision getDivision(RaftGroupId groupId) {
        return divisions.computeIfAbsent(groupId, key -> new MyDivision(this));
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
      public RaftServerRpc getServerRpc() {
        return null;
      }

      @Override
      public DataStreamServerRpc getDataStreamServerRpc() {
        return null;
      }

      @Override
      public RaftClientReply submitClientRequest(RaftClientRequest request) {
        return submitClientRequestAsync(request).join();
      }

      @Override
      public RaftClientReply setConfiguration(SetConfigurationRequest request) {
        return null;
      }

      @Override
      public CompletableFuture<RaftClientReply> submitClientRequestAsync(RaftClientRequest request) {
        final MyDivision d = getDivision(request.getRaftGroupId());
        return d.getDataStreamMap()
            .remove(ClientInvocationId.valueOf(request))
            .thenApply(StateMachine.DataStream::getDataChannel)
            .thenApply(channel -> buildRaftClientReply(request, channel));
      }

      RaftClientReply buildRaftClientReply(RaftClientRequest request, DataChannel channel) {
        Assert.assertTrue(channel instanceof MyDataChannel);
        final MyDataChannel dataChannel = (MyDataChannel) channel;
        return RaftClientReply.newBuilder()
            .setRequest(request)
            .setSuccess()
            .setMessage(() -> DataStreamTestUtils.bytesWritten2ByteString(dataChannel.getBytesWritten()))
            .build();
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
  }


  protected void setup(int numServers){
    final List<RaftPeer> peers = Arrays.stream(MiniRaftCluster.generateIds(numServers, 0))
        .map(RaftPeerId::valueOf)
        .map(id -> RaftPeer.newBuilder().setId(id).setDataStreamAddress(NetUtils.createLocalServerAddress()).build())
        .collect(Collectors.toList());

    List<RaftServer> raftServers = new ArrayList<>();
    peers.forEach(peer -> raftServers.add(newRaftServer(peer, properties)));
    setup(RaftGroupId.randomId(), peers, raftServers);
  }


  void setup(RaftGroupId groupId, List<RaftPeer> peers, List<RaftServer> raftServers) {
    raftGroup = RaftGroup.valueOf(groupId, peers);
    this.peers = peers;
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

  RaftClient newRaftClientForDataStream(ClientId clientId) {
    return RaftClient.newBuilder()
        .setClientId(clientId)
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

  ClientId getPrimaryClientId() throws IOException {
    return getPrimaryServer().raftServer.getDivision(raftGroup.getGroupId()).getRaftClient().getId();
  }

  void runTestMockCluster(ClientId clientId, int bufferSize, int bufferNum,
      Exception expectedException, Exception headerException)
      throws IOException {
    try (final RaftClient client = newRaftClientForDataStream(clientId)) {
      final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi()
          .stream(null, getRoutingTable(peers, getPrimaryServer().getPeer()));
      if (headerException != null) {
        final DataStreamReply headerReply = out.getHeaderFuture().join();
        Assert.assertFalse(headerReply.isSuccess());
        final RaftClientReply clientReply = ClientProtoUtils.toRaftClientReply(
            ((DataStreamReplyByteBuffer)headerReply).slice());
        Assert.assertTrue(clientReply.getException().getMessage().contains(headerException.getMessage()));
        return;
      }

      final RaftClientReply clientReply = DataStreamTestUtils.writeAndCloseAndAssertReplies(
          CollectionUtils.as(servers, Server::getRaftServer), null, out, bufferSize, bufferNum,
          getPrimaryClientId(), null, false).join();
      if (expectedException != null) {
        Assert.assertFalse(clientReply.isSuccess());
        Assert.assertTrue(clientReply.getException().getMessage().contains(
            expectedException.getMessage()));
      } else {
        Assert.assertTrue(clientReply.isSuccess());
      }
    }
  }
}
