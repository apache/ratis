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
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftConfiguration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestRaftConfiguration extends BaseTest {
  @Test
  public void testIsHighestPriority() {
    Integer node1 = 0;
    Integer node2 = 1;
    Integer node3 = 2;
    PeerConfiguration peerConfig = new PeerConfiguration(raftPeersWithPriority(node1, node2, node3));
    RaftConfiguration config = RaftConfigurationImpl.newBuilder().setConf(peerConfig).build();
    RaftPeer[] allRaftPeers = peerConfig.getPeers(RaftProtos.RaftPeerRole.FOLLOWER).toArray(new RaftPeer[peerConfig.getPeers(
        RaftProtos.RaftPeerRole.FOLLOWER).size()]);

    // First member should not have highest priority
    assertFalse(RaftServerTestUtil.isHighestPriority(config,
        allRaftPeers[0].getId()));

    // Last member should have highest priority
    assertTrue(RaftServerTestUtil.isHighestPriority(config,
        allRaftPeers[allRaftPeers.length - 1].getId()));

    // Should return false for non existent peer id
    assertFalse(RaftServerTestUtil.isHighestPriority(config, RaftPeerId.valueOf("123456789")));
  }

  private Collection<RaftPeer> raftPeersWithPriority(Integer... voters) {
    return Arrays.stream(voters)
        .map(id -> RaftPeer.newBuilder().setPriority(id).setId(id.toString()).build())
        .collect(Collectors.toSet());
  }

  @Test
  public void testSingleMode() {
    RaftConfigurationImpl config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(1)))
        .build();
    assertTrue("Peer is in single mode.", config.isSingleMode(RaftPeerId.valueOf("1")));

    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(0, 1)))
        .setOldConf(new PeerConfiguration(raftPeersWithPriority(0)))
        .build();
    assertTrue("Peer is in single mode.", config.isSingleMode(RaftPeerId.valueOf("0")));
    assertFalse("Peer is a new peer.", config.isSingleMode(RaftPeerId.valueOf("1")));

    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(0, 1)))
        .build();
    assertFalse("Peer is in ha mode.", config.isSingleMode(RaftPeerId.valueOf("0")));
    assertFalse("Peer is in ha mode.", config.isSingleMode(RaftPeerId.valueOf("1")));

    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(0, 1)))
        .setOldConf(new PeerConfiguration(raftPeersWithPriority(2, 3)))
        .build();
    assertFalse("Peer is in ha mode.", config.isSingleMode(RaftPeerId.valueOf("0")));
    assertFalse("Peer is in ha mode.", config.isSingleMode(RaftPeerId.valueOf("1")));
    assertFalse("Peer is in ha mode.", config.isSingleMode(RaftPeerId.valueOf("3")));
    assertFalse("Peer is in ha mode.", config.isSingleMode(RaftPeerId.valueOf("4")));
  }

  @Test
  public void testChangeMajority() {
    // Case 1: {1} --> {1, 2}.
    RaftConfigurationImpl config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(1)))
        .build();
    assertFalse("Change from single mode to ha mode is not considered as changing majority.",
        config.changeMajority(raftPeersWithPriority(1, 2)));

    // Case 2: {1} --> {2}.
    assertTrue(config.changeMajority(raftPeersWithPriority(2)));

    // Case 3: {1, 2, 3} --> {1, 2, 4, 5}.
    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(1, 2, 3)))
        .build();
    assertTrue(config.changeMajority(raftPeersWithPriority(1, 2, 4, 5)));

    // Case 4: {1, 2, 3} --> {1, 4, 5}.
    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(1, 2, 3)))
        .build();
    assertTrue(config.changeMajority(raftPeersWithPriority(1, 4, 5)));

    // Case 5: {1, 2, 3} --> {1, 2, 3, 4, 5}.
    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(1, 2, 3)))
        .build();
    assertFalse(config.changeMajority(raftPeersWithPriority(1, 2, 3, 4, 5)));

    // Case 6: {1, 2, 3, 4, 5} --> {1, 2}.
    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(1, 2, 3, 4, 5)))
        .build();
    assertFalse(config.changeMajority(raftPeersWithPriority(1, 2)));

    // Case 7: {1, 2, 3, 4, 5} --> {1, 2, 3}.
    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(1, 2, 3, 4, 5)))
        .build();
    assertFalse(config.changeMajority(raftPeersWithPriority(1, 2, 3)));

    // Case 8: {1, 2, 3} --> {1, 2, 3, 4}.
    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(1, 2, 3)))
        .build();
    assertFalse(config.changeMajority(raftPeersWithPriority(1, 2, 3, 4)));

    // Case 9: {1, 2, 3, 4, 5} --> {1, 2, 3, 4, 6, 7}.
    config = RaftConfigurationImpl.newBuilder()
        .setConf(new PeerConfiguration(raftPeersWithPriority(1, 2, 3, 4, 5)))
        .build();
    assertFalse(config.changeMajority(raftPeersWithPriority(1, 2, 3, 4, 6, 7)));
  }
}