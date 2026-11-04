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
        assertEquals(0, client.nextAppendEntriesIndex());
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testAppendersTakeConnectionsInTurn() {
    final int connections = 4;
    final GrpcServerProtocolClient client = newClient(connections);
    final List<ManagedChannel> channels = client.getAppendEntriesChannels();
    try {
      assertEquals(connections, channels.size());
      assertSame(client.getMainChannel(), channels.get(0));
      assertEquals(connections, new HashSet<>(channels).size());

      for (int i = 0; i < 100; i++) {
        assertEquals(i % connections, client.nextAppendEntriesIndex());
      }
    } finally {
      client.close();
    }
    channels.forEach(c -> assertTrue(c.isShutdown(), c + " is not shut down"));
  }
}
