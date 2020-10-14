/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ratis.netty.server;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamReplyByteBuffer;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NettyServerStreamRpc implements DataStreamServerRpc {
  public static final Logger LOG = LoggerFactory.getLogger(NettyServerStreamRpc.class);

  private final RaftPeer raftServer;
  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final ChannelFuture channelFuture;

  private final StateMachine stateMachine;
  private final ConcurrentMap<Long, CompletableFuture<DataStream>> streams = new ConcurrentHashMap<>();

  public NettyServerStreamRpc(RaftPeer server, StateMachine stateMachine) {
    this.raftServer = server;
    this.stateMachine = stateMachine;
    this.channelFuture = buildChannel();
  }

  private CompletableFuture<DataStream> getDataStreamFuture(ByteBuf buf, AtomicBoolean released) {
    try {
      final RaftClientRequest request =
          ClientProtoUtils.toRaftClientRequest(RaftProtos.RaftClientRequestProto.parseFrom(buf.nioBuffer()));
      return stateMachine.data().stream(request);
    } catch (InvalidProtocolBufferException e) {
      throw new CompletionException(e);
    } finally {
      buf.release();
      released.set(true);
    }
  }

  private void writeTo(ByteBuf buf, DataStream stream, boolean released) {
    if (released) {
      return;
    }
    try {
      if (stream == null) {
        return;
      }

      final WritableByteChannel channel = stream.getWritableByteChannel();
      for (ByteBuffer buffer : buf.nioBuffers()) {
        try {
          channel.write(buffer);
        } catch (Throwable t) {
          throw new CompletionException(t);
        }
      }
    } finally {
      buf.release();
    }
  }

  private void sendReply(DataStreamRequestByteBuf request, ChannelHandlerContext ctx) {
    final DataStreamReply reply = new DataStreamReplyByteBuffer(
        request.getStreamId(), request.getDataOffset(), ByteBuffer.wrap("OK".getBytes()));
    ctx.writeAndFlush(reply);
  }

  private ChannelInboundHandler getServerHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        final DataStreamRequestByteBuf req = (DataStreamRequestByteBuf)msg;
        final long streamId = req.getStreamId();
        final ByteBuf buf = req.getBuf();
        final AtomicBoolean released = new AtomicBoolean();
        streams.computeIfAbsent(streamId, id -> getDataStreamFuture(buf, released))
            .thenAccept(stream -> writeTo(buf, stream, released.get()))
            .thenAccept(dummy -> sendReply(req, ctx));
      }
    };
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new DataStreamRequestDecoder());
        p.addLast(new DataStreamReplyEncoder());
        p.addLast(getServerHandler());
      }
    };
  }

  ChannelFuture buildChannel() {
    return new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(getInitializer())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .localAddress(NetUtils.createSocketAddr(raftServer.getAddress()))
        .bind();
  }

  private Channel getChannel() {
    return channelFuture.awaitUninterruptibly().channel();
  }

  @Override
  public void startServer() {
    channelFuture.syncUninterruptibly();
  }

  @Override
  public void closeServer() {
    final ChannelFuture f = getChannel().close();
    f.syncUninterruptibly();
    bossGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    workerGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    try {
      bossGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
      workerGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("Interrupt EventLoopGroup terminate", e);
    }
  }
}
