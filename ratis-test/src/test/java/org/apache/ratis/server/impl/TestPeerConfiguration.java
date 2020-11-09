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
package org.apache.ratis.server.impl;

import org.apache.ratis.BaseTest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPeerConfiguration extends BaseTest {
  @Test
  public void testPeerConfiguration() {
    final RaftPeer[] peers = {
        RaftPeer.newBuilder().setId("s0").build(),
        RaftPeer.newBuilder().setId("s1").build(),
        RaftPeer.newBuilder().setId("s0").build(),
    };
    testFailureCase("Duplicated peers", () -> {
      new PeerConfiguration(Arrays.asList(peers));
    }, IllegalArgumentException.class);
  }

  @Test
  public void testOddNodesQuorum() {
    String localId = "0";
    String node1 = "1";
    String node2 = "2";
    PeerConfiguration conf = new PeerConfiguration(raftPeers(localId, node1, node2));
    // in odd quorum, half + self is majority
    assertTrue(conf.hasMajority(raftPeerIds(node1), RaftPeerId.valueOf(localId)));
    // in odd quorum, majority is impossible after half rejected
    assertFalse(conf.majorityRejectVotes(raftPeerIds(node1)));
  }

  @Test
  public void testEvenNodeQuorum() {
    String localId = "0";
    String node1 = "1";
    String node2 = "2";
    String node3 = "3";
    PeerConfiguration conf = new PeerConfiguration(raftPeers(localId, node1, node2, node3));
    // in even quorum, half + self is majority
    assertFalse(conf.hasMajority(raftPeerIds(node1), RaftPeerId.valueOf(localId)));
    assertTrue(conf.hasMajority(raftPeerIds(node1, node2), RaftPeerId.valueOf(localId)));
    // in even quorum, majority is impossible after half rejected
    assertFalse(conf.majorityRejectVotes(raftPeerIds(node1)));
    assertTrue(conf.majorityRejectVotes(raftPeerIds(node1, node2)));
  }

  private Collection<RaftPeer> raftPeers(String... voters) {
    return Arrays.stream(voters)
        .map(id -> RaftPeer.newBuilder().setId(id).build())
        .collect(Collectors.toSet());
  }

  private Collection<RaftPeerId> raftPeerIds(String... voters) {
    return Arrays.stream(voters).map(RaftPeerId::valueOf).collect(Collectors.toSet());
  }

}
