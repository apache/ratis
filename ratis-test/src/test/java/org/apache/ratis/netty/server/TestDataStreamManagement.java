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

import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuf;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.netty.metrics.NettyServerStreamRpcMetrics;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelId;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ratis.thirdparty.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestDataStreamManagement {
  @Test
  void readCleansChannelMapOnEarlyException() throws Exception {
    // Scenario: STREAM_DATA arrives without prior STREAM_HEADER, so readImpl fails early.
    // Expectation: read(...) catch path must still remove channelId->invocationId mapping
    // to avoid leaks when the channel remains active.
    final RaftPeerId serverId = RaftPeerId.valueOf("s1");
    final RaftProperties properties = new RaftProperties();
    final RaftServer server = newRaftServer(serverId, properties);

    final NettyServerStreamRpcMetrics metrics = new NettyServerStreamRpcMetrics("s1");
    final DataStreamManagement management = new DataStreamManagement(server, metrics);

    // Use a real Netty pipeline to obtain a concrete ChannelHandlerContext.
    final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
    final ChannelHandlerContext ctx = embeddedChannel.pipeline().firstContext();
    assertNotNull(ctx, "ChannelHandlerContext should be initialized");
    final ChannelId channelId = embeddedChannel.id();

    final DataStreamRequestByteBuf request = new DataStreamRequestByteBuf(
        ClientId.randomId(),
        Type.STREAM_DATA,
        1L,
        0L,
        Collections.singletonList(StandardWriteOption.CLOSE),
        Unpooled.buffer(0));

    final CheckedBiFunction<RaftClientRequest, Set<RaftPeer>, Set<DataStreamOutputImpl>, IOException> getStreams =
        (r, p) -> Collections.emptySet();

    try {
      // This read should fail early (missing stream info) and must clear ChannelMap entries.
      management.read(request, ctx, getStreams);
      assertEquals(0, management.getChannelInvocationCount(channelId),
          "channel map should be cleared on early read failure");
    } finally {
      embeddedChannel.finishAndReleaseAll();
      management.shutdown();
    }
  }

  private static RaftServer newRaftServer(RaftPeerId serverId, RaftProperties properties) {
    return (RaftServer) Proxy.newProxyInstance(TestDataStreamManagement.class.getClassLoader(),
        new Class<?>[]{RaftServer.class},
        (proxy, method, args) -> {
          if (method.getDeclaringClass() == Object.class) {
            switch (method.getName()) {
              case "toString":
                return "RaftServerProxy(" + serverId + ")";
              case "hashCode":
                return System.identityHashCode(proxy);
              case "equals":
                return proxy == args[0];
              default:
                return null;
            }
          }
          switch (method.getName()) {
            case "getId":
              return serverId;
            case "getProperties":
              return properties;
            default:
              throw new UnsupportedOperationException("Unexpected RaftServer call: " + method);
          }
        });
  }
}
