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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ratis.server;

import java.util.Arrays;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestRaftServerDivision {
  private static final RaftPeerId LOCAL_ID = RaftPeerId.valueOf("local");
  private static final RaftPeerId REMOTE_ID = RaftPeerId.valueOf("remote");
  private static final RaftGroupMemberId MEMBER_ID =
      RaftGroupMemberId.valueOf(LOCAL_ID, RaftGroupId.emptyGroupId());
  private static final RaftPeer REMOTE_PEER = RaftPeer.newBuilder().setId(REMOTE_ID).build();

  @Test
  void testCheckLeaderReady() {
    final RaftServer.Division division = mock(RaftServer.Division.class, CALLS_REAL_METHODS);
    final DivisionInfo info = mock(DivisionInfo.class);
    final RaftConfiguration conf = mock(RaftConfiguration.class);
    when(division.getMemberId()).thenReturn(MEMBER_ID);
    when(division.getInfo()).thenReturn(info);
    when(division.getRaftConf()).thenReturn(conf);
    when(info.getLifeCycleState()).thenReturn(LifeCycle.State.RUNNING);
    when(conf.getAllPeers()).thenReturn(Arrays.asList(RaftPeer.newBuilder().setId(LOCAL_ID).build(), REMOTE_PEER));

    when(info.isLeader()).thenReturn(true);
    when(info.isLeaderReady()).thenReturn(true);
    Assertions.assertNull(division.checkLeaderReady());

    when(info.isLeaderReady()).thenReturn(false);
    Assertions.assertInstanceOf(LeaderNotReadyException.class, division.checkLeaderReady());

    when(info.isLeader()).thenReturn(false);
    when(info.getLeaderId()).thenReturn(LOCAL_ID);
    final RaftException staleLeader = division.checkLeaderReady();
    Assertions.assertInstanceOf(NotLeaderException.class, staleLeader);
    Assertions.assertNull(((NotLeaderException) staleLeader).getSuggestedLeader());

    when(info.getLeaderId()).thenReturn(REMOTE_ID);
    when(conf.getPeer(REMOTE_ID)).thenReturn(REMOTE_PEER);
    final RaftException knownLeader = division.checkLeaderReady();
    Assertions.assertInstanceOf(NotLeaderException.class, knownLeader);
    Assertions.assertEquals(REMOTE_PEER, ((NotLeaderException) knownLeader).getSuggestedLeader());
  }
}
