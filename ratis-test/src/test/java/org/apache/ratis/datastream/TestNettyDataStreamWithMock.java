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

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.NetUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNettyDataStreamWithMock extends DataStreamBaseTest {
  static RaftPeer newRaftPeer(RaftServer server) {
    final InetSocketAddress rpc = NetUtils.createLocalServerAddress();
    final int dataStreamPort = NettyConfigKeys.DataStream.port(server.getProperties());
    return RaftPeer.newBuilder()
        .setId(server.getId())
        .setAddress(rpc)
        .setDataStreamAddress(NetUtils.createSocketAddrForHost(rpc.getHostName(), dataStreamPort))
        .build();
  }

  @Before
  public void setup() {
    properties = new RaftProperties();
    RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.NETTY);
  }

  @Override
  protected MyRaftServer newRaftServer(RaftPeer peer, RaftProperties properties) {
    final RaftProperties p = new RaftProperties(properties);
    NettyConfigKeys.DataStream.setPort(p, NetUtils.createSocketAddr(peer.getDataStreamAddress()).getPort());
    return super.newRaftServer(peer, p);
  }

  private void testMockCluster(int leaderIndex, int numServers, RaftException leaderException,
      Exception submitException) throws Exception {
    testMockCluster(leaderIndex, numServers, leaderException, submitException, null);
  }

  private void testMockCluster(int leaderIndex, int numServers, RaftException leaderException,
      Exception submitException, IOException getStateMachineException) throws Exception {
    List<RaftServer> raftServers = new ArrayList<>();
    ClientId clientId = ClientId.randomId();
    RaftGroupId groupId = RaftGroupId.randomId();
    final RaftPeer suggestedLeader = RaftPeer.newBuilder().setId("s" + leaderIndex).build();

    for (int i = 0; i < numServers; i ++) {
      RaftServer raftServer = mock(RaftServer.class);
      RaftPeerId peerId = RaftPeerId.valueOf("s" + i);
      RaftProperties properties = new RaftProperties();
      NettyConfigKeys.DataStream.setPort(properties, NetUtils.createLocalServerAddress().getPort());
      RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.NETTY);

      if (submitException != null) {
        when(raftServer.submitClientRequestAsync(Mockito.any(RaftClientRequest.class)))
            .thenThrow(submitException);
      } else {
        final boolean isLeader = i == leaderIndex;
        when(raftServer.submitClientRequestAsync(Mockito.any(RaftClientRequest.class)))
            .thenAnswer((Answer<CompletableFuture<RaftClientReply>>) invocation -> {
              final RaftClientRequest r = (RaftClientRequest) invocation.getArguments()[0];
              final RaftClientReply reply;
              if (isLeader) {
                final RaftClientReply.Builder b = RaftClientReply.newBuilder().setRequest(r);
                reply = leaderException != null? b.setException(leaderException).build()
                    : b.setSuccess().setMessage(() -> DataStreamTestUtils.MOCK).build();
              } else {
                final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(peerId, groupId);
                final NotLeaderException notLeaderException = new NotLeaderException(memberId, suggestedLeader, null);
                reply = RaftClientReply.newBuilder().setRequest(r).setException(notLeaderException).build();
              }
              return CompletableFuture.completedFuture(reply);
            });
      }

      when(raftServer.getProperties()).thenReturn(properties);
      when(raftServer.getId()).thenReturn(peerId);
      if (getStateMachineException == null) {
        final ConcurrentMap<RaftGroupId, MyDivision> divisions = new ConcurrentHashMap<>();
        when(raftServer.getDivision(Mockito.any(RaftGroupId.class))).thenAnswer(
            invocation -> divisions.computeIfAbsent((RaftGroupId)invocation.getArguments()[0], MyDivision::new));
      } else {
        when(raftServer.getDivision(Mockito.any(RaftGroupId.class))).thenThrow(getStateMachineException);
      }

      raftServers.add(raftServer);
    }

    runTestMockCluster(groupId, raftServers, clientId, 1_000_000, 10,
        submitException != null ? submitException : leaderException, getStateMachineException);
  }

  void runTestMockCluster(RaftGroupId groupId, List<RaftServer> raftServers, ClientId clientId, int bufferSize, int bufferNum,
      Exception expectedException, Exception headerException) throws Exception {
    try {
      final List<RaftPeer> peers = raftServers.stream()
          .map(TestNettyDataStreamWithMock::newRaftPeer)
          .collect(Collectors.toList());
      setup(groupId, peers, raftServers);
      runTestMockCluster(clientId, bufferSize, bufferNum, expectedException, headerException);
    } finally {
      shutdown();
    }
  }

  @Test
  public void testCloseStreamPrimaryIsLeader() throws Exception {
    // primary is 0, leader is 0
    testMockCluster(0, 3, null, null);
  }

  @Test
  public void testCloseStreamPrimaryIsNotLeader() throws Exception {
    // primary is 0, leader is 1
    testMockCluster(1, 3, null, null);
  }

  @Test
  public void testCloseStreamOneServer() throws Exception {
    // primary is 0, leader is 0
    testMockCluster(0, 1, null, null);
  }

  @Test
  public void testStateMachineExceptionInReplyPrimaryIsLeader() throws Exception {
    // primary is 0, leader is 0
    StateMachineException stateMachineException = new StateMachineException("leader throw StateMachineException");
    testMockCluster(0, 3, stateMachineException, null);
  }

  @Test
  public void testStateMachineExceptionInReplyPrimaryIsNotLeader() throws Exception {
    // primary is 0, leader is 1
    StateMachineException stateMachineException = new StateMachineException("leader throw StateMachineException");
    testMockCluster(1, 3, stateMachineException, null);
  }

  @Test
  public void testDataStreamExceptionInReplyPrimaryIsLeader() throws Exception {
    // primary is 0, leader is 0
    IOException ioException = new IOException("leader throw IOException");
    testMockCluster(0, 3, null, ioException);
  }

  @Test
  public void testDataStreamExceptionInReplyPrimaryIsNotLeader() throws Exception {
    // primary is 0, leader is 1
    IOException ioException = new IOException("leader throw IOException");
    testMockCluster(1, 3, null, ioException);
  }

  @Test
  public void testDataStreamExceptionGetStateMachine() throws Exception {
    final IOException getStateMachineException = new IOException("Failed to get StateMachine");
    testMockCluster(1, 1, null, null, getStateMachineException);
  }
}
