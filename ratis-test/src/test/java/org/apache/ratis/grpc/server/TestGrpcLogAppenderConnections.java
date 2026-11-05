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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

/** Several groups on the same servers replicate over more than one appendEntries connection per peer. */
public class TestGrpcLogAppenderConnections extends BaseTest {
  private static final int CONNECTIONS = 3;
  private static final int NEW_GROUPS = 2 * CONNECTIONS;

  @Test
  public void testGroupsOnSameServersShareLogAppenderConnections() throws Exception {
    final RaftProperties properties = new RaftProperties();
    GrpcConfigKeys.Server.setLogAppenderConnections(properties, CONNECTIONS);
    final MiniRaftCluster cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(3, properties);
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);
      // the first server has the highest priority in the new groups, so that it becomes the leader of all of them
      final List<RaftPeer> peers = new ArrayList<>(cluster.getGroup().getPeers());
      final RaftPeerId leaderId = peers.get(0).getId();
      final List<RaftPeer> prioritized = new ArrayList<>();
      for (RaftPeer peer : peers) {
        prioritized.add(RaftPeer.newBuilder(peer).setPriority(peer.getId().equals(leaderId) ? 1 : 0).build());
      }

      // add more groups on the same three servers
      final List<RaftGroup> groups = new ArrayList<>();
      for (int i = 0; i < NEW_GROUPS; i++) {
        final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), prioritized);
        for (RaftPeer peer : prioritized) {
          try (RaftClient client = cluster.createClient(peer.getId(), group)) {
            assertTrue(client.getGroupManagementApi(peer.getId()).add(group).isSuccess());
          }
        }
        groups.add(group);
      }

      for (RaftGroup group : groups) {
        JavaUtils.attempt(() -> assertTrue(cluster.getDivision(leaderId, group.getGroupId()).getInfo().isLeader()),
            30, TimeDuration.ONE_SECOND, leaderId + " leads " + group.getGroupId(), LOG);
        try (RaftClient client = cluster.createClient(group)) {
          for (int i = 0; i < 10; i++) {
            assertTrue(client.io().send(new SimpleMessage(group.getGroupId() + "-" + i)).isSuccess());
          }
        }
      }
      assertConnections(cluster, leaderId, groups);
    } finally {
      cluster.shutdown();
    }
  }

  /** The leader's appenders of all the groups use every appendEntries connection to each follower. */
  private static void assertConnections(MiniRaftCluster cluster, RaftPeerId leaderId, List<RaftGroup> groups)
      throws Exception {
    final RaftServer leader = cluster.getServer(leaderId);
    final GrpcServicesImpl rpc = (GrpcServicesImpl) leader.getServerRpc();
    final Map<RaftPeerId, Set<Integer>> indices = new HashMap<>();
    for (RaftGroup group : groups) {
      final List<LogAppender> appenders = RaftServerTestUtil.getLogAppenders(
          cluster.getDivision(leaderId, group.getGroupId())).collect(Collectors.toList());
      assertEquals(group.getPeers().size() - 1, appenders.size());
      for (LogAppender appender : appenders) {
        final RaftPeerId follower = appender.getFollowerId();
        assertEquals(CONNECTIONS, rpc.getProxies().getProxy(follower).getAppendEntriesChannels().size());
        final int index = ((GrpcLogAppender) appender).getAppendEntriesIndex();
        indices.computeIfAbsent(follower, k -> new TreeSet<>()).add(index);
      }
    }
    final Set<Integer> all = IntStream.range(0, CONNECTIONS).boxed().collect(Collectors.toCollection(TreeSet::new));
    assertEquals(2, indices.size());
    indices.forEach((follower, used) -> assertEquals(all, used, leaderId + " -> " + follower));
  }
}
