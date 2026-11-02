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

import org.apache.ratis.BaseTest;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.GroupManagementBaseTest;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.junit.jupiter.api.Test;

/** Several groups on the same servers replicate over more than one appendEntries connection per peer. */
public class TestGrpcLogAppenderConnections extends BaseTest {
  private static final int CONNECTIONS = 3;

  @Test
  public void testMultiGroupWithLogAppenderConnections() throws Exception {
    final RaftProperties properties = new RaftProperties();
    GrpcConfigKeys.Server.setLogAppenderConnections(properties, CONNECTIONS);
    final MiniRaftCluster cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(0, properties);

    final CheckedBiConsumer<MiniRaftCluster, RaftGroup, Exception> checker = (c, group) -> {
      for (RaftPeer peer : group.getPeers()) {
        final RaftServer server = c.getServer(peer.getId());
        final GrpcServicesImpl rpc = (GrpcServicesImpl) server.getServerRpc();
        for (RaftPeer other : group.getPeers()) {
          if (!other.getId().equals(peer.getId())) {
            assertEquals(CONNECTIONS, rpc.getProxies().getProxy(other.getId()).getAppendEntriesChannels().size());
          }
        }
      }
      try (RaftClient client = c.createClient(group)) {
        for (int i = 0; i < 10; i++) {
          assertTrue(client.io().send(new SimpleMessage(group.getGroupId() + "-" + i)).isSuccess());
        }
      }
    };
    GroupManagementBaseTest.runMultiGroupTest(cluster, new int[] {3, 6, 9}, -1, checker);
  }
}
