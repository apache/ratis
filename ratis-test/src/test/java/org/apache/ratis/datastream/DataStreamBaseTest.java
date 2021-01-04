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
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DataStreamServer;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.util.CollectionUtils;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

abstract class DataStreamBaseTest extends BaseTest {
  RaftConfiguration getRaftConf() {
      final List<RaftPeer> peers = servers.stream().map(Server::getPeer).collect(Collectors.toList());
      return RaftServerTestUtil.newRaftConfiguration(peers);
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
          .stream(null, DataStreamTestUtils.getRoutingTableChainTopology(peers, getPrimaryServer().getPeer()));
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
          getPrimaryClientId(), client.getId(), false).join();
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
