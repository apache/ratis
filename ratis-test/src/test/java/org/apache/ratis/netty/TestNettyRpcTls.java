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
package org.apache.ratis.netty;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.security.SecurityTestUtils;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test TLS on the Netty RPC transport, covering both server-to-server RPC (election/replication)
 * and client-to-server RPC.
 */
public class TestNettyRpcTls extends BaseTest {
  @Test
  public void testRpcWithMutualTls() throws Exception {
    final RaftProperties properties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);

    // A single Parameters object carries both the server and client TLS configuration. It is applied
    // to every server (its inbound endpoint and its outbound peer connections) and to the client
    // created by the cluster, so all Netty RPC traffic is mutually authenticated over TLS.
    final Parameters parameters = new Parameters();
    NettyConfigKeys.Server.setTlsConf(parameters, SecurityTestUtils.newServerTlsConfig(true));
    NettyConfigKeys.Client.setTlsConf(parameters, SecurityTestUtils.newClientTlsConfig(true));

    final MiniRaftClusterWithNetty cluster = new MiniRaftClusterWithNetty(
        new String[]{"s0", "s1", "s2"}, properties, parameters);
    try {
      cluster.start();

      // A successful election proves server-to-server RPC (RequestVote/AppendEntries) works over TLS.
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      Assertions.assertNotNull(leader, "A leader should be elected over the TLS-secured Netty RPC");

      // A successful client write proves client-to-server RPC works over TLS.
      try (RaftClient client = cluster.createClient(leader.getId())) {
        final RaftClientReply reply = client.io().send(new SimpleMessage("hello-tls"));
        Assertions.assertTrue(reply.isSuccess());
      }
    } finally {
      cluster.shutdown();
    }
  }
}
