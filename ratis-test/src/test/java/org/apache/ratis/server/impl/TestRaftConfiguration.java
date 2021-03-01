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
import org.apache.ratis.server.RaftConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;


public class TestRaftConfiguration extends BaseTest {
  @Test
  public void testIsHighestPriority() {
    Integer node1 = 0;
    Integer node2 = 1;
    Integer node3 = 2;
    PeerConfiguration peerConfig = new PeerConfiguration(raftPeersWithPriority(node1, node2, node3));
    RaftConfiguration config = RaftConfigurationImpl.newBuilder().setConf(peerConfig).build();
    RaftPeer[] allRaftPeers = peerConfig.getPeers().toArray(new RaftPeer[peerConfig.getPeers().size()]);

    // First member should not have highest priority
    Assert.assertFalse(RaftConfigurationTestUtil.isHighestPriority(config,
            allRaftPeers[0].getId()));

    // Last member should have highest priority
    Assert.assertTrue(RaftConfigurationTestUtil.isHighestPriority(config,
            allRaftPeers[allRaftPeers.length -1].getId()));

    // Should return false for non existent peer id
    Assert.assertFalse(RaftConfigurationTestUtil.isHighestPriority(config, RaftPeerId.valueOf("123456789")));
  }

  private Collection<RaftPeer> raftPeersWithPriority(Integer... voters) {
    return Arrays.stream(voters)
            .map(id -> RaftPeer.newBuilder().setPriority(id).setId(id.toString()).build())
            .collect(Collectors.toSet());
  }
}
