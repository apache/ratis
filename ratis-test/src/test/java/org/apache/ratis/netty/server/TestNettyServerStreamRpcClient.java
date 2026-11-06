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
package org.apache.ratis.netty.server;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestNettyServerStreamRpcClient {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testPropertiesReachClientBuilder(boolean skipSendForward) throws Exception {
    final RaftProperties properties = new RaftProperties();
    RaftConfigKeys.DataStream.setSkipSendForward(properties, skipSendForward);
    final RaftPeer peer = RaftPeer.newBuilder().setId("s0").build();
    try (DataStreamClient client = NettyServerStreamRpc.newClient(peer, properties, new Parameters())) {
      Assertions.assertNotNull(client.getClientRpc());
    }
  }
}
