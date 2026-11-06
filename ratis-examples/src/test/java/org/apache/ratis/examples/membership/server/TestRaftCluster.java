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
package org.apache.ratis.examples.membership.server;

import org.apache.ratis.examples.counter.CounterCommand;
import org.apache.ratis.netty.NettyFactory;
import org.apache.ratis.netty.client.NettyClientRpc;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class TestRaftCluster {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 128, 65536, -1})
  @ResourceLock(Resources.SYSTEM_OUT)
  public void testQueryCounterPrintsInteger(int count) throws Exception {
    final RaftPeer peer = RaftPeer.newBuilder().setId("p12345").setAddress("localhost:12345").build();
    final NettyClientRpc rpc = Mockito.mock(NettyClientRpc.class);
    Mockito.when(rpc.sendRequest(Mockito.any(RaftClientRequest.class))).thenAnswer(invocation -> {
      final RaftClientRequest request = invocation.getArgument(0);
      Assertions.assertEquals(CounterCommand.GET.getMessage(), request.getMessage());
      return RaftClientReply.newBuilder()
          .setClientId(request.getClientId())
          .setServerId(request.getServerId())
          .setGroupId(request.getRaftGroupId())
          .setCallId(request.getCallId())
          .setSuccess(true)
          .setMessage(Message.valueOf(ByteString.copyFrom(ByteBuffer.allocate(Integer.BYTES).putInt(count).array())))
          .build();
    });

    final PrintStream originalOut = System.out;
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final RaftCluster cluster = new RaftCluster();
    try (MockedConstruction<CServer> servers = Mockito.mockConstruction(CServer.class,
        (server, context) -> Mockito.when(server.getPeer()).thenReturn(peer));
         MockedConstruction<NettyFactory> factories = Mockito.mockConstruction(NettyFactory.class,
             (factory, context) -> Mockito.when(factory.newRaftClientRpc(Mockito.any(), Mockito.any()))
                 .thenReturn(rpc));
         PrintStream capturedOut = new PrintStream(output, true, StandardCharsets.UTF_8.name())) {
      cluster.init(Collections.singletonList(12345));
      System.setOut(capturedOut);
      try {
        cluster.queryCounter();
      } finally {
        System.setOut(originalOut);
        cluster.close();
      }
    }

    Assertions.assertEquals("Current counter value: " + count + System.lineSeparator(),
        output.toString(StandardCharsets.UTF_8.name()));
    Mockito.verify(rpc).close();
  }
}
