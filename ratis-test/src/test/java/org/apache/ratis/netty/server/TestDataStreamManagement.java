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

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.client.impl.OrderedAsync;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuf;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.netty.metrics.NettyServerStreamRpcMetrics;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataApi;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelId;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoop;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDataStreamManagement {
  @Test
  void readOnlyRequestInvokesReadStreamManagement() throws Exception {
    final RaftPeerId serverId = RaftPeerId.valueOf("s1");
    final ClientId clientId = ClientId.randomId();
    final RaftGroupId groupId = RaftGroupId.randomId();
    final ByteString query = ByteString.copyFromUtf8("query");
    final ByteString response = ByteString.copyFromUtf8("response");

    final AtomicReference<Message> messageRef = new AtomicReference<>();
    final AtomicReference<WritableByteChannel> streamRef = new AtomicReference<>();
    final DataApi dataApi = new DataApi() {
      @Override
      public void query(Message request, WritableByteChannel stream) {
        messageRef.set(request);
        streamRef.set(stream);
      }
    };
    final ReadStreamManagement management = newReadStreamManagement(serverId, groupId, dataApi);
    final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());

    final ReadOnlyRequest readOnlyRequest = newReadOnlyRequest(clientId, serverId, groupId, 1L, query);

    try {
      assertTrue(management.process(readOnlyRequest.request, embeddedChannel.pipeline().firstContext()));
      assertEquals(0, readOnlyRequest.headerBuf.refCnt());

      JavaUtils.attempt(() -> assertNotNull(streamRef.get()), 10,
          TimeDuration.valueOf(100, TimeUnit.MILLISECONDS), "read-only stream", null);
      final WritableByteChannel stream = streamRef.get();
      stream.write(response.asReadOnlyByteBuffer());
      stream.close();

      final List<DataStreamReply> replies = new ArrayList<>();
      JavaUtils.attempt(() -> {
        for (Object outbound; (outbound = embeddedChannel.readOutbound()) != null;) {
          replies.add((DataStreamReply) outbound);
        }
        assertEquals(2, replies.size());
      }, 10, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS), "read-only replies", null);

      assertEquals(query, messageRef.get().getContent());
      assertFalse(streamRef.get().isOpen(), "state machine should close the streaming query channel");
      assertSuccessReply(Type.STREAM_DATA, response.size(), replies.get(0));
      assertSuccessReply(Type.STREAM_HEADER, 0, replies.get(1));
      assertTrue(ClientProtoUtils.getRaftClientReply(replies.get(1)).isSuccess());
    } finally {
      embeddedChannel.finishAndReleaseAll();
      management.shutdown();
    }
  }

  @Test
  void readOnlyRequestWaitsForLinearizableCheck() throws Exception {
    final RaftPeerId serverId = RaftPeerId.valueOf("s1");
    final ClientId clientId = ClientId.randomId();
    final RaftGroupId groupId = RaftGroupId.randomId();
    final ByteString query = ByteString.copyFromUtf8("query");
    final CompletableFuture<RaftClientReply> readOnlyCheck = new CompletableFuture<>();
    final AtomicReference<RaftClientRequest> submittedReadOnlyCheck = new AtomicReference<>();
    final AtomicReference<Message> messageRef = new AtomicReference<>();
    final AtomicReference<WritableByteChannel> streamRef = new AtomicReference<>();

    final DataApi dataApi = new DataApi() {
      @Override
      public void query(Message request, WritableByteChannel stream) {
        messageRef.set(request);
        streamRef.set(stream);
      }
    };
    final ReadStreamManagement management = newReadStreamManagement(serverId, groupId, dataApi, request -> {
      submittedReadOnlyCheck.set(request);
      return readOnlyCheck;
    });
    final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
    final ReadOnlyRequest readOnlyRequest = newReadOnlyRequest(clientId, serverId, groupId, 1L, query);

    try {
      assertTrue(management.process(readOnlyRequest.request, embeddedChannel.pipeline().firstContext()));
      assertEquals(0, readOnlyRequest.headerBuf.refCnt());

      final RaftClientRequest checkRequest = submittedReadOnlyCheck.get();
      assertNotNull(checkRequest);
      assertEquals(OrderedAsync.DUMMY.getContent(), checkRequest.getMessage().getContent());
      assertNull(streamRef.get(), "state machine query should wait for the read-only check");

      readOnlyCheck.complete(RaftClientReply.newBuilder().setRequest(checkRequest).setSuccess().build());
      JavaUtils.attempt(() -> assertNotNull(streamRef.get()), 10,
          TimeDuration.valueOf(100, TimeUnit.MILLISECONDS), "linearizable read-only stream", null);
      assertEquals(query, messageRef.get().getContent());
    } finally {
      embeddedChannel.finishAndReleaseAll();
      management.shutdown();
    }
  }

  @Test
  void readOnlyCheckFailureSkipsStateMachineQuery() throws Exception {
    final RaftPeerId serverId = RaftPeerId.valueOf("s1");
    final ClientId clientId = ClientId.randomId();
    final RaftGroupId groupId = RaftGroupId.randomId();
    final ByteString query = ByteString.copyFromUtf8("query");
    final AtomicBoolean queryCalled = new AtomicBoolean();

    final DataApi dataApi = new DataApi() {
      @Override
      public void query(Message request, WritableByteChannel stream) {
        queryCalled.set(true);
      }
    };
    final ReadStreamManagement management = newReadStreamManagement(serverId, groupId, dataApi, request ->
        CompletableFuture.completedFuture(RaftClientReply.newBuilder()
            .setRequest(request)
            .setException(new ReadIndexException("read index failed"))
            .build()));
    final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
    final ReadOnlyRequest readOnlyRequest = newReadOnlyRequest(clientId, serverId, groupId, 1L, query);

    try {
      assertTrue(management.process(readOnlyRequest.request, embeddedChannel.pipeline().firstContext()));
      assertEquals(0, readOnlyRequest.headerBuf.refCnt());

      final List<DataStreamReply> replies = new ArrayList<>();
      JavaUtils.attempt(() -> {
        for (Object outbound; (outbound = embeddedChannel.readOutbound()) != null;) {
          replies.add((DataStreamReply) outbound);
        }
        assertEquals(1, replies.size());
      }, 10, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS), "read-only check failure reply", null);

      assertFalse(queryCalled.get(), "state machine query should not run when the read-only check fails");
      final DataStreamReply reply = replies.get(0);
      assertEquals(Type.STREAM_HEADER, reply.getType());
      assertFalse(reply.isSuccess());
      final RaftClientReply clientReply = ClientProtoUtils.getRaftClientReply(reply);
      assertFalse(clientReply.isSuccess());
      assertNotNull(clientReply.getReadIndexException());
      assertEquals(serverId, clientReply.getServerId());
    } finally {
      embeddedChannel.finishAndReleaseAll();
      management.shutdown();
    }
  }

  @Test
  void readOnlyQueryDoesNotRunOnNettyEventLoop() throws Exception {
    final RaftPeerId serverId = RaftPeerId.valueOf("s1");
    final ClientId clientId = ClientId.randomId();
    final RaftGroupId groupId = RaftGroupId.randomId();
    final CountDownLatch queryDone = new CountDownLatch(1);
    final AtomicBoolean queryInEventLoop = new AtomicBoolean();
    final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
    final EventLoop eventLoop = eventLoopGroup.next();
    final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
    final ChannelHandlerContext ctx = embeddedChannel.pipeline().firstContext();
    assertNotNull(ctx, "ChannelHandlerContext should be initialized");

    final DataApi dataApi = new DataApi() {
      @Override
      public void query(Message request, WritableByteChannel stream) {
        queryInEventLoop.set(eventLoop.inEventLoop());
        queryDone.countDown();
      }
    };
    final StateMachine stateMachine = new BaseStateMachine() {
      @Override
      public DataApi data() {
        return dataApi;
      }
    };
    final RaftServer server = newRaftServer(serverId, new RaftProperties(), groupId, newDivision(stateMachine));
    final ReadStreamManagement management = new ReadStreamManagement(server);

    final RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(serverId)
        .setGroupId(groupId)
        .setCallId(1L)
        .setMessage(Message.valueOf(ByteString.copyFromUtf8("query")))
        .setType(RaftClientRequest.readRequestType())
        .build();
    final ByteBuffer header = ClientProtoUtils.toRaftClientRequestProtoByteBuffer(raftClientRequest);
    final ByteBuf headerBuf = Unpooled.wrappedBuffer(header);
    final DataStreamRequestByteBuf request = new DataStreamRequestByteBuf(
        clientId,
        Type.STREAM_HEADER,
        raftClientRequest.getCallId(),
        0L,
        Collections.singletonList(StandardWriteOption.FLUSH),
        headerBuf);

    try {
      eventLoop.submit(() -> assertTrue(management.process(request, ctx))).sync();

      assertTrue(queryDone.await(10, TimeUnit.SECONDS));
      assertEquals(0, headerBuf.refCnt());
      assertFalse(queryInEventLoop.get(), "read-only state machine query should not run on Netty event loop");
    } finally {
      embeddedChannel.finishAndReleaseAll();
      management.shutdown();
      eventLoopGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).sync();
    }
  }

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

  private static class ReadOnlyRequest {
    private final DataStreamRequestByteBuf request;
    private final ByteBuf headerBuf;

    ReadOnlyRequest(DataStreamRequestByteBuf request, ByteBuf headerBuf) {
      this.request = request;
      this.headerBuf = headerBuf;
    }
  }

  private static ReadStreamManagement newReadStreamManagement(
      RaftPeerId serverId, RaftGroupId groupId, DataApi dataApi) {
    return newReadStreamManagement(serverId, groupId, dataApi, TestDataStreamManagement::successReply);
  }

  private static ReadStreamManagement newReadStreamManagement(RaftPeerId serverId, RaftGroupId groupId,
      DataApi dataApi, Function<RaftClientRequest, CompletableFuture<RaftClientReply>> submitClientRequestAsync) {
    final StateMachine stateMachine = new BaseStateMachine() {
      @Override
      public DataApi data() {
        return dataApi;
      }
    };
    final RaftServer server = newRaftServer(serverId, new RaftProperties(), groupId, newDivision(stateMachine),
        submitClientRequestAsync);
    return new ReadStreamManagement(server);
  }

  private static ReadOnlyRequest newReadOnlyRequest(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId,
      long callId, ByteString query) {
    final RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(serverId)
        .setGroupId(groupId)
        .setCallId(callId)
        .setMessage(Message.valueOf(query))
        .setType(RaftClientRequest.readRequestType())
        .build();
    final ByteBuffer header = ClientProtoUtils.toRaftClientRequestProtoByteBuffer(raftClientRequest);
    final ByteBuf headerBuf = Unpooled.wrappedBuffer(header);
    return new ReadOnlyRequest(new DataStreamRequestByteBuf(
        clientId,
        Type.STREAM_HEADER,
        raftClientRequest.getCallId(),
        0L,
        Collections.singletonList(StandardWriteOption.FLUSH),
        headerBuf), headerBuf);
  }

  private static void assertSuccessReply(Type expectedType, long expectedBytesWritten, DataStreamReply reply) {
    assertEquals(expectedType, reply.getType());
    assertTrue(reply.isSuccess());
    assertEquals(expectedBytesWritten, reply.getBytesWritten());
    assertTrue(reply instanceof DataStreamReplyByteBuffer);
  }

  private static RaftServer newRaftServer(RaftPeerId serverId, RaftProperties properties) {
    return newRaftServer(serverId, properties, null, null);
  }

  private static RaftServer newRaftServer(RaftPeerId serverId, RaftProperties properties,
      RaftGroupId groupId, RaftServer.Division division) {
    return newRaftServer(serverId, properties, groupId, division, TestDataStreamManagement::successReply);
  }

  private static CompletableFuture<RaftClientReply> successReply(RaftClientRequest request) {
    return CompletableFuture.completedFuture(RaftClientReply.newBuilder().setRequest(request).setSuccess().build());
  }

  private static RaftServer newRaftServer(RaftPeerId serverId, RaftProperties properties,
      RaftGroupId groupId, RaftServer.Division division,
      Function<RaftClientRequest, CompletableFuture<RaftClientReply>> submitClientRequestAsync) {
    return (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(), new Class<?>[]{RaftServer.class},
        (proxy, method, args) -> {
          switch (method.getName()) {
          case "getId":
            return serverId;
          case "getProperties":
            return properties;
          case "getDivision":
            if (groupId != null && groupId.equals(args[0])) {
              return division;
            }
            throw new IOException("Division not found: " + args[0]);
          case "submitClientRequestAsync":
            return submitClientRequestAsync.apply((RaftClientRequest) args[0]);
          case "close":
            return null;
          case "toString":
            return serverId.toString();
          case "hashCode":
            return System.identityHashCode(proxy);
          case "equals":
            return proxy == args[0];
          default:
            throw new UnsupportedOperationException(method.toString());
          }
        });
  }

  private static RaftServer.Division newDivision(StateMachine stateMachine) {
    return (RaftServer.Division) Proxy.newProxyInstance(RaftServer.Division.class.getClassLoader(),
        new Class<?>[]{RaftServer.Division.class},
        (proxy, method, args) -> {
          switch (method.getName()) {
          case "getStateMachine":
            return stateMachine;
          case "close":
            return null;
          case "toString":
            return stateMachine.toString();
          case "hashCode":
            return System.identityHashCode(proxy);
          case "equals":
            return proxy == args[0];
          default:
            throw new UnsupportedOperationException(method.toString());
          }
        });
  }
}
