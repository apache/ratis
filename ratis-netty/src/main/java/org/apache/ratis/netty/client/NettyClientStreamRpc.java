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

package org.apache.ratis.netty.client;

import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestFilePositionCount;
import org.apache.ratis.netty.NettyDataStreamUtils;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class NettyClientStreamRpc implements DataStreamClientRpc {
  public static final Logger LOG = LoggerFactory.getLogger(NettyClientStreamRpc.class);

  private final String name;
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final Supplier<Channel> channel;
  private final ConcurrentMap<ClientInvocationId, Queue<CompletableFuture<DataStreamReply>>> replies =
      new ConcurrentHashMap<>();

  public NettyClientStreamRpc(RaftPeer server, RaftProperties properties){
    this.name = JavaUtils.getClassSimpleName(getClass()) + "->" + server;

    final ChannelFuture f = new Bootstrap()
        .group(workerGroup)
        .channel(NioSocketChannel.class)
        .handler(getInitializer())
        .option(ChannelOption.SO_KEEPALIVE, true)
        .connect(NetUtils.createSocketAddr(server.getDataStreamAddress()));
    this.channel = JavaUtils.memoize(() -> f.syncUninterruptibly().channel());
  }

  private Channel getChannel() {
    return channel.get();
  }

  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof DataStreamReply)) {
          LOG.error("{}: unexpected message {}", this, msg.getClass());
          return;
        }
        final DataStreamReply reply = (DataStreamReply) msg;
        LOG.debug("{}: read {}", this, reply);
        ClientInvocationId clientInvocationId = ClientInvocationId.valueOf(reply.getClientId(), reply.getStreamId());
        Optional.ofNullable(replies.get(clientInvocationId))
            .map(Queue::poll)
            .ifPresent(f -> f.complete(reply));
      }
    };
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(newEncoder());
        p.addLast(newEncoderDataStreamRequestFilePositionCount());
        p.addLast(newDecoder());
        p.addLast(getClientHandler());
      }
    };
  }

  MessageToMessageEncoder<DataStreamRequestByteBuffer> newEncoder() {
    return new MessageToMessageEncoder<DataStreamRequestByteBuffer>() {
      @Override
      protected void encode(ChannelHandlerContext context, DataStreamRequestByteBuffer request, List<Object> out) {
        NettyDataStreamUtils.encodeDataStreamRequestByteBuffer(request, out::add, context.alloc());
      }
    };
  }

  MessageToMessageEncoder<DataStreamRequestFilePositionCount> newEncoderDataStreamRequestFilePositionCount() {
    return new MessageToMessageEncoder<DataStreamRequestFilePositionCount>() {
      @Override
      protected void encode(ChannelHandlerContext ctx, DataStreamRequestFilePositionCount request, List<Object> out) {
        NettyDataStreamUtils.encodeDataStreamRequestFilePositionCount(request, out::add, ctx.alloc());
      }
    };
  }

  ByteToMessageDecoder newDecoder() {
    return new ByteToMessageDecoder() {
      {
        this.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
      }

      @Override
      protected void decode(ChannelHandlerContext context, ByteBuf buf, List<Object> out) {
        Optional.ofNullable(NettyDataStreamUtils.decodeDataStreamReplyByteBuffer(buf)).ifPresent(out::add);
      }
    };
  }

  @Override
  public CompletableFuture<DataStreamReply> streamAsync(DataStreamRequest request) {
    final CompletableFuture<DataStreamReply> f = new CompletableFuture<>();
    ClientInvocationId clientInvocationId = ClientInvocationId.valueOf(request.getClientId(), request.getStreamId());
    final Queue<CompletableFuture<DataStreamReply>> q = replies.computeIfAbsent(
       clientInvocationId, key -> new ConcurrentLinkedQueue<>());
    if (!q.offer(f)) {
      f.completeExceptionally(new IllegalStateException(this + ": Failed to offer a future for " + request));
      return f;
    }
    LOG.debug("{}: write {}", this, request);
    getChannel().writeAndFlush(request);
    return f;
  }

  @Override
  public void close() {
    getChannel().close().syncUninterruptibly();
    workerGroup.shutdownGracefully();
  }

  @Override
  public String toString() {
    return name;
  }
}
