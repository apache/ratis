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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto.RaftNettyServerReplyCase;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Tests for {@link NettyRpcService} request handling. */
public class TestNettyRpcService {
  private static final RaftPeerId ID = RaftPeerId.valueOf("s0");

  private static RaftServer newMockServer() {
    final RaftServer server = Mockito.mock(RaftServer.class);
    Mockito.when(server.getId()).thenReturn(ID);
    Mockito.when(server.getProperties()).thenReturn(new RaftProperties());
    return server;
  }

  private static RaftNettyServerRequestProto newRequestVoteProto() {
    final RaftRpcRequestProto rpc = RaftRpcRequestProto.newBuilder()
        .setRequestorId(ID.toByteString())
        .setReplyId(ID.toByteString())
        .setCallId(1)
        .build();
    final RequestVoteRequestProto request = RequestVoteRequestProto.newBuilder()
        .setServerRequest(rpc)
        .build();
    return RaftNettyServerRequestProto.newBuilder()
        .setRequestVoteRequest(request)
        .build();
  }

  /**
   * A non-{@link java.io.IOException} thrown by the server must be turned into an error reply
   * instead of escaping the handler and leaving the client to block until its request timeout.
   */
  @Test
  public void testHandleReturnsErrorReplyOnRuntimeException() throws Exception {
    final RaftServer server = newMockServer();
    Mockito.when(server.requestVote(Mockito.any())).thenThrow(new RuntimeException("injected"));

    final NettyRpcService service = NettyRpcService.newBuilder().setServer(server).build();
    service.start();
    try {
      final RaftNettyServerReplyProto reply = service.handle(newRequestVoteProto());
      Assertions.assertEquals(RaftNettyServerReplyCase.EXCEPTIONREPLY, reply.getRaftNettyServerReplyCase());
    } finally {
      service.close();
    }
  }

  /** Requests must be handled off the Netty I/O event loop, on the request executor thread. */
  @Test
  public void testRequestHandledOffEventLoop() throws Exception {
    final RaftServer server = newMockServer();
    final CompletableFuture<String> handlingThreadName = new CompletableFuture<>();
    Mockito.when(server.requestVote(Mockito.any())).thenAnswer(invocation -> {
      handlingThreadName.complete(Thread.currentThread().getName());
      throw new RuntimeException("injected");
    });

    final NettyRpcService service = NettyRpcService.newBuilder().setServer(server).build();
    service.start();
    try {
      final ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
      service.new InboundHandler().channelRead0(ctx, newRequestVoteProto());

      final String threadName = handlingThreadName.get(5, TimeUnit.SECONDS);
      Assertions.assertTrue(threadName.startsWith(ID + "-request-"),
          "Request was handled on an unexpected thread: " + threadName);
      Assertions.assertNotEquals(Thread.currentThread().getName(), threadName,
          "Request was handled on the calling thread, not offloaded");
    } finally {
      service.close();
    }
  }
}
