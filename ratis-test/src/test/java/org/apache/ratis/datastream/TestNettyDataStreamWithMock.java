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
import org.apache.ratis.client.AsyncRpcApi;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.DataStreamTestUtils.MultiDataStreamStateMachine;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.DataStreamMap;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerTestUtil;
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

  RaftServer.Division mockDivision(RaftServer server, RaftGroupId groupId, RaftClient client) {
    final RaftServer.Division division = mock(RaftServer.Division.class);
    when(division.getRaftServer()).thenReturn(server);
    when(division.getRaftClient()).thenReturn(client);
    when(division.getRaftConf()).thenAnswer(i -> getRaftConf());

    final MultiDataStreamStateMachine stateMachine = new MultiDataStreamStateMachine();
    try {
      stateMachine.initialize(server, groupId, null);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    when(division.getStateMachine()).thenReturn(stateMachine);

    final DataStreamMap streamMap = RaftServerTestUtil.newDataStreamMap(server.getId());
    when(division.getDataStreamMap()).thenReturn(streamMap);

    return division;
  }

  private void testMockCluster(int numServers, RaftException leaderException,
      IllegalStateException submitException) throws Exception {
    testMockCluster(numServers, leaderException, submitException, null);
  }

  private void testMockCluster(int numServers, RaftException leaderException,
      IllegalStateException submitException, IOException getStateMachineException) throws Exception {
    List<RaftServer> raftServers = new ArrayList<>();
    ClientId clientId = ClientId.randomId();
    RaftGroupId groupId = RaftGroupId.randomId();

    for (int i = 0; i < numServers; i ++) {
      RaftServer raftServer = mock(RaftServer.class);
      RaftPeerId peerId = RaftPeerId.valueOf("s" + i);
      RaftProperties properties = new RaftProperties();
      NettyConfigKeys.DataStream.setPort(properties, NetUtils.createLocalServerAddress().getPort());
      RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.NETTY);

      when(raftServer.getProperties()).thenReturn(properties);
      when(raftServer.getId()).thenReturn(peerId);
      when(raftServer.getPeer()).thenReturn(RaftPeer.newBuilder().setId(peerId).build());
      if (getStateMachineException == null) {
        RaftClient client = Mockito.mock(RaftClient.class);
        when(client.getId()).thenReturn(clientId);
        AsyncRpcApi asyncRpcApi = Mockito.mock(AsyncRpcApi.class);
        when(client.async()).thenReturn(asyncRpcApi);

        final RaftServer.Division myDivision = mockDivision(raftServer, groupId, client);
        when(raftServer.getDivision(Mockito.any(RaftGroupId.class))).thenReturn(myDivision);

        if (submitException != null) {
          when(asyncRpcApi.sendForward(Mockito.any(RaftClientRequest.class))).thenThrow(submitException);
        } else if (i == 0) {
          // primary
          when(asyncRpcApi.sendForward(Mockito.any(RaftClientRequest.class)))
              .thenAnswer((Answer<CompletableFuture<RaftClientReply>>) invocation -> {
                final RaftClientRequest r = (RaftClientRequest) invocation.getArguments()[0];
                final RaftClientReply.Builder b = RaftClientReply.newBuilder().setRequest(r);
                final RaftClientReply reply = leaderException != null? b.setException(leaderException).build()
                      : b.setSuccess().setMessage(() -> DataStreamTestUtils.MOCK).build();
                return CompletableFuture.completedFuture(reply);
              });
        }
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
  public void testCloseStreamPrimary() throws Exception {
    testMockCluster(3, null, null);
  }

  @Test
  public void testCloseStreamOneServer() throws Exception {
    testMockCluster(1, null, null);
  }

  @Test
  public void testStateMachineExceptionInReply() throws Exception {
    StateMachineException stateMachineException = new StateMachineException("leader throw StateMachineException");
    testMockCluster(3, stateMachineException, null);
  }

  @Test
  public void testDataStreamExceptionInReply() throws Exception {
    IllegalStateException submitException = new IllegalStateException("primary throw IllegalStateException");
    testMockCluster(3, null, submitException);
  }

  @Test
  public void testDataStreamExceptionGetStateMachine() throws Exception {
    final IOException getStateMachineException = new IOException("Failed to get StateMachine");
    testMockCluster(1, null, null, getStateMachineException);
  }
}
