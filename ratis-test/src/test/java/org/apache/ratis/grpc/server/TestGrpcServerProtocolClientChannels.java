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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.BaseTest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

/** Tests the appendEntries channels of {@link GrpcServerProtocolClient}; channels connect lazily, no server needed. */
public class TestGrpcServerProtocolClientChannels extends BaseTest {
  private static final RaftPeer PEER = RaftPeer.newBuilder().setId("s1").setAddress("localhost:1").build();

  private static GrpcServerProtocolClient newClient(int appendEntriesConnections) {
    return new GrpcServerProtocolClient(PEER, 1, appendEntriesConnections, 1 << 20,
        TimeDuration.valueOf(3, TimeUnit.SECONDS), null, true, null);
  }

  @Test
  public void testOneConnectionUsesMainChannel() {
    final GrpcServerProtocolClient client = newClient(1);
    try {
      final List<ManagedChannel> channels = client.getAppendEntriesChannels();
      assertEquals(1, channels.size());
      assertSame(client.getMainChannel(), channels.get(0));
      for (int i = 0; i < 10; i++) {
        assertEquals(0, client.getAppendEntriesIndex(RaftGroupId.randomId()));
      }
      assertTrue(client.getAppendEntriesIndices().isEmpty());
    } finally {
      client.close();
    }
  }

  @Test
  public void testGroupsSpreadOverConnections() {
    final int connections = 4;
    final GrpcServerProtocolClient client = newClient(connections);
    final List<ManagedChannel> channels = client.getAppendEntriesChannels();
    try {
      assertEquals(connections, channels.size());
      assertSame(client.getMainChannel(), channels.get(0));
      assertEquals(connections, new HashSet<>(channels).size());

      final int[] groupCounts = new int[connections];
      for (int i = 0; i < 100; i++) {
        final RaftGroupId groupId = RaftGroupId.randomId();
        final int index = client.getAppendEntriesIndex(groupId);
        assertTrue(index >= 0 && index < connections, "index " + index);
        // a group always uses the same connection, which keeps its appendEntries in order
        assertEquals(index, client.getAppendEntriesIndex(RaftGroupId.valueOf(groupId.getUuid())));
        groupCounts[index]++;
      }
      // each new group goes to the connection with the fewest groups
      for (int count : groupCounts) {
        assertEquals(100 / connections, count);
      }
    } finally {
      client.close();
    }
    channels.forEach(c -> assertTrue(c.isShutdown(), c + " is not shut down"));
  }

  @Test
  public void testReleasedConnectionIsReused() {
    final int connections = 3;
    final GrpcServerProtocolClient client = newClient(connections);
    try {
      final RaftGroupId[] groups = new RaftGroupId[2 * connections];
      for (int i = 0; i < groups.length; i++) {
        groups[i] = RaftGroupId.randomId();
        assertEquals(i % connections, client.getAppendEntriesIndex(groups[i]));
      }

      // the connection of a released group goes to the next new group
      client.releaseAppendEntriesIndex(groups[4]);
      assertEquals(groups.length - 1, client.getAppendEntriesIndices().size());
      assertEquals(1, client.getAppendEntriesIndex(RaftGroupId.randomId()));

      // releasing a group again, or a group without a connection, changes nothing
      client.releaseAppendEntriesIndex(groups[4]);
      client.releaseAppendEntriesIndex(RaftGroupId.randomId());
      assertEquals(0, client.getAppendEntriesIndex(RaftGroupId.randomId()));
      assertEquals(2, client.getAppendEntriesIndex(groups[2]));
    } finally {
      client.close();
    }
  }
}
