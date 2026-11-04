/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.grpc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

/** Several groups on the same servers replicate over more than one appendEntries connection per peer. */
public class TestGrpcLogAppenderConnections extends BaseTest {
  private static final int CONNECTIONS = 3;
  private static final int NEW_GROUPS = 5;

  @Test
  public void testGroupsOnSameServersShareLogAppenderConnections() throws Exception {
    final RaftProperties properties = new RaftProperties();
    GrpcConfigKeys.Server.setLogAppenderConnections(properties, CONNECTIONS);
    final MiniRaftCluster cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(3, properties);
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);
      final Collection<RaftPeer> peers = cluster.getGroup().getPeers();

      // add more groups on the same three servers
      final List<RaftGroup> groups = new ArrayList<>();
      groups.add(cluster.getGroup());
      for (int i = 0; i < NEW_GROUPS; i++) {
        final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
        for (RaftPeer peer : peers) {
          try (RaftClient client = cluster.createClient(peer.getId(), group)) {
            assertTrue(client.getGroupManagementApi(peer.getId()).add(group).isSuccess());
          }
        }
        groups.add(group);
      }

      for (RaftGroup group : groups) {
        RaftTestUtil.waitForLeader(cluster, group.getGroupId());
        try (RaftClient client = cluster.createClient(group)) {
          for (int i = 0; i < 10; i++) {
            assertTrue(client.io().send(new SimpleMessage(group.getGroupId() + "-" + i)).isSuccess());
          }
        }
      }
      JavaUtils.attempt(() -> assertConnections(cluster, groups), 10, TimeDuration.ONE_SECOND,
          "assertConnections", LOG);

      // a removed group releases its connections
      final List<RaftGroup> removed = groups.subList(1, groups.size());
      for (RaftGroup group : removed) {
        for (RaftPeer peer : peers) {
          try (RaftClient client = cluster.createClient(peer.getId(), group)) {
            assertTrue(client.getGroupManagementApi(peer.getId()).remove(group.getGroupId(), true, false)
                .isSuccess());
          }
        }
      }
      final List<RaftGroup> remaining = groups.subList(0, 1);
      JavaUtils.attempt(() -> assertConnections(cluster, remaining), 10, TimeDuration.ONE_SECOND,
          "assertConnections after remove", LOG);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * On each server, the groups it leads use the appendEntries connections to each follower,
   * spread evenly over them; no other group uses them.
   */
  private static void assertConnections(MiniRaftCluster cluster, List<RaftGroup> groups) throws Exception {
    for (RaftServer server : cluster.getServers()) {
      final Set<RaftGroupId> led = new HashSet<>();
      for (RaftGroup group : groups) {
        if (cluster.getDivision(server.getId(), group.getGroupId()).getInfo().isLeader()) {
          led.add(group.getGroupId());
        }
      }
      final GrpcServicesImpl rpc = (GrpcServicesImpl) server.getServerRpc();
      for (RaftPeer follower : groups.get(0).getPeers()) {
        if (follower.getId().equals(server.getId())) {
          continue;
        }
        final GrpcServerProtocolClient proxy = rpc.getProxies().getProxy(follower.getId());
        assertEquals(CONNECTIONS, proxy.getAppendEntriesChannels().size());
        final Map<RaftGroupId, Integer> indices = proxy.getAppendEntriesIndices();
        assertEquals(led, indices.keySet(), server.getId() + " -> " + follower.getId());

        final int[] groupCounts = new int[CONNECTIONS];
        indices.values().forEach(i -> groupCounts[i]++);
        for (int count : groupCounts) {
          assertTrue(Math.abs(count * CONNECTIONS - led.size()) < CONNECTIONS,
              () -> server.getId() + " -> " + follower.getId() + ": " + indices);
        }
      }
    }
  }
}
