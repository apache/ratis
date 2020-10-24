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
import org.apache.ratis.netty.NettyDataStreamUtils;
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
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class NettyClientStreamRpc implements DataStreamClientRpc {
  public static final Logger LOG = LoggerFactory.getLogger(NettyClientStreamRpc.class);

  private RaftPeer server;
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private Channel channel;
  private Queue<CompletableFuture<DataStreamReply>> replies
      = new LinkedList<>();

  public NettyClientStreamRpc(RaftPeer server, RaftProperties properties){
    this.server = server;
  }

  synchronized CompletableFuture<DataStreamReply> pollReply() {
    return replies.poll();
  }

  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws InterruptedException {
        final DataStreamReply reply = (DataStreamReply) msg;
        pollReply().complete(reply);
      }
    };
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch)
          throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(newEncoder());
        p.addLast(newDecoder());
        p.addLast(getClientHandler());
      }
    };
  }

  MessageToMessageEncoder<DataStreamRequestByteBuffer> newEncoder() {
    return new MessageToMessageEncoder<DataStreamRequestByteBuffer>() {
      @Override
      protected void encode(ChannelHandlerContext context, DataStreamRequestByteBuffer request, List<Object> out) {
        NettyDataStreamUtils.encodeDataStreamPacketByteBuffer(request, out::add);
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
  public synchronized CompletableFuture<DataStreamReply> streamAsync(DataStreamRequest request) {
    CompletableFuture<DataStreamReply> f = new CompletableFuture<>();
    replies.offer(f);
    channel.writeAndFlush(request);
    return f;
  }

  @Override
  public void startClient() {
    final InetSocketAddress address = NetUtils.createSocketAddr(server.getAddress());
    try {
      channel = (new Bootstrap())
          .group(workerGroup)
          .channel(NioSocketChannel.class)
          .handler(getInitializer())
          .option(ChannelOption.SO_KEEPALIVE, true)
          .connect(address)
          .sync()
          .channel();
      System.out.println(channel);
    } catch (Exception e){
      LOG.info("Exception {}", e.getCause());
    }
  }

  @Override
  public void closeClient(){
    channel.close().syncUninterruptibly();
    workerGroup.shutdownGracefully();
  }
}
