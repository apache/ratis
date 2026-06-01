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
package org.apache.ratis.grpc.client;

import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.util.concurrent.EventExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TestGrpcClientEventLoops extends BaseTest {
  @Test
  public void testClientWorkerEventLoopGroupSharedByPeerProxies() throws Exception {
    final RaftProperties properties = new RaftProperties();
    GrpcConfigKeys.Client.setWorkerGroupSize(properties, 1);
    GrpcConfigKeys.setUseEpoll(properties, false);

    final RaftPeer p0 = newPeer("s0", 15000);
    final RaftPeer p1 = newPeer("s1", 15001);
    EventLoopGroup clientWorkers = null;
    try (GrpcClientRpc rpc = GrpcClientRpc.create(ClientId.randomId(), properties, null, null)) {
      rpc.addRaftPeers(Arrays.asList(p0, p1));
      final GrpcClientProtocolClient c0 = rpc.getProxies().getProxy(p0.getId());
      final GrpcClientProtocolClient c1 = rpc.getProxies().getProxy(p1.getId());

      clientWorkers = c0.getClientWorkersForTesting();
      Assertions.assertSame(clientWorkers, c1.getClientWorkersForTesting());
      Assertions.assertEquals(1, countEventExecutors(clientWorkers));
      Assertions.assertFalse(clientWorkers.isShuttingDown());
    }

    Assertions.assertNotNull(clientWorkers);
    Assertions.assertTrue(clientWorkers.isShuttingDown() || clientWorkers.isShutdown() || clientWorkers.isTerminated());
  }

  private static RaftPeer newPeer(String id, int port) {
    return RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:" + port).build();
  }

  private static int countEventExecutors(EventLoopGroup group) {
    int count = 0;
    for (EventExecutor ignored : group) {
      count++;
    }
    return count;
  }
}
