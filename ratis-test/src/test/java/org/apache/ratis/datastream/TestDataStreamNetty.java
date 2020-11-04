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
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.NetUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDataStreamNetty extends DataStreamBaseTest {
  @Before
  public void setup() {
    properties = new RaftProperties();
    RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.NETTY);
  }

  @Override
  protected RaftServer newRaftServer(RaftPeer peer, RaftProperties properties) {
    final RaftProperties p = new RaftProperties(properties);
    NettyConfigKeys.DataStream.setPort(p,  NetUtils.createSocketAddr(peer.getAddress()).getPort());
    return super.newRaftServer(peer, p);
  }

  @Test
  public void testDataStreamSingleServer() throws Exception {
    runTestDataStream(1, 2, 20, 1_000_000, 10);
    runTestDataStream(1, 2, 20, 1_000, 10_000);
  }

  @Test
  public void testDataStreamMultipleServer() throws Exception {
    runTestDataStream(3, 2, 20, 1_000_000, 100);
    runTestDataStream(3, 2, 20, 1_000, 10_000);
  }

  private void testCloseStream(int leaderIndex, int numServers) throws Exception {
    List<RaftServer> raftServers = new ArrayList<>();
    ClientId clientId = ClientId.randomId();
    RaftGroupId groupId = RaftGroupId.randomId();
    long callId = 100;
    long longIndex = 200;
    RaftPeer suggestedLeader = new RaftPeer(RaftPeerId.valueOf("s" + leaderIndex));
    RaftClientReply expectedClientReply = new RaftClientReply(clientId, suggestedLeader.getId(),
        groupId, callId, true, null, null, longIndex, null);

    for (int i = 0; i < 3; i ++) {
      RaftServer raftServer = mock(RaftServer.class);
      RaftClientReply raftClientReply;
      RaftPeerId peerId = RaftPeerId.valueOf("s" + i);
      RaftProperties properties = new RaftProperties();
      NettyConfigKeys.DataStream.setPort(properties, NetUtils.createLocalServerAddress().getPort());

      if (i == leaderIndex) {
        raftClientReply = expectedClientReply;
      } else {
        RaftGroupMemberId raftGroupMemberId = RaftGroupMemberId.valueOf(peerId, groupId);
        NotLeaderException notLeaderException = new NotLeaderException(raftGroupMemberId, suggestedLeader, null);
        raftClientReply = new RaftClientReply(clientId, peerId,
            groupId, callId, false, null, notLeaderException, longIndex, null);
      }

      when(raftServer.submitClientRequestAsync(Mockito.any(RaftClientRequest.class)))
          .thenReturn(CompletableFuture.completedFuture(raftClientReply));
      when(raftServer.getProperties()).thenReturn(properties);
      when(raftServer.getId()).thenReturn(peerId);
      when(raftServer.getStateMachine(Mockito.any(RaftGroupId.class))).thenReturn(new MultiDataStreamStateMachine());

      raftServers.add(raftServer);
    }

    runTestCloseStream(raftServers, 1_000_000, 10, expectedClientReply);
  }

  @Test
  public void testCloseStreamPrimaryIsLeader() throws Exception {
    // primary is 0, leader is 0
    testCloseStream(0, 3);
  }

  @Test
  public void testCloseStreamPrimaryIsNotLeader() throws Exception {
    // primary is 0, leader is 1
    testCloseStream(1, 3);
  }

  @Test
  public void testCloseStreamOneServer() throws Exception {
    // primary is 0, leader is 0
    testCloseStream(0, 1);
  }
}
