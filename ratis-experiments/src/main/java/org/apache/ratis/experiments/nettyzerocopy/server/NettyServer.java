/**
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

package org.apache.ratis.experiments.nettyzerocopy.server;

import org.apache.ratis.experiments.nettyzerocopy.decoders.RequestDecoderComposite;
import org.apache.ratis.experiments.nettyzerocopy.encoders.ResponseEncoder;
import org.apache.ratis.experiments.nettyzerocopy.objects.RequestDataComposite;
import org.apache.ratis.experiments.nettyzerocopy.objects.ResponseData;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * A server program that handles messages from Client
 * using {@link RequestDecoderComposite}, allowing for
 * zero-copy on the server.
 */
public class NettyServer {
  private EventLoopGroup bossGroup = new NioEventLoopGroup();
  private EventLoopGroup workerGroup = new NioEventLoopGroup();

  public EventLoopGroup getBossGroup(){
    return bossGroup;
  }

  public EventLoopGroup getWorkerGroup() {
    return workerGroup;
  }

  /**
   * Casts inbound message as {@link RequestDataComposite}
   * because a NIO CompositeByteBuf interface is needed to allow for zero-copy.
   * @return ChannelInboundHandler
   */
  private ChannelInboundHandler getServerHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ResponseData reply = new ResponseData();
        RequestDataComposite req = (RequestDataComposite)msg;
        req.getBuff().release();
        reply.setId(req.getDataId());
        ctx.writeAndFlush(reply);
      }
    };
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch)
          throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new RequestDecoderComposite());
        p.addLast(new ResponseEncoder());
        p.addLast(getServerHandler());
      }
    };
  }

  public void setupServer() throws InterruptedException {
    int port = 50053;
    String host = "localhost";
    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(getInitializer())
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true);

    b.bind(port).sync();
  }

  public static void main(String[] args) throws InterruptedException {
    NettyServer s = new NettyServer();
    s.setupServer();
  }
}
