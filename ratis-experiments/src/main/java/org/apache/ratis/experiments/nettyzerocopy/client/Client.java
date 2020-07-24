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

package org.apache.ratis.experiments.nettyzerocopy.client;

import org.apache.ratis.experiments.nettyzerocopy.RequestData;
import org.apache.ratis.experiments.nettyzerocopy.RequestEncoder;
import org.apache.ratis.experiments.nettyzerocopy.ResponseData;
import org.apache.ratis.experiments.nettyzerocopy.ResponseDecoder;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Client{
  int times = 100;
  Channel channel;
  EventLoopGroup workerGroup = new NioEventLoopGroup();

  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        final ResponseData reply = (ResponseData)msg;
        System.out.println(reply.getId());
        if(reply.getId() == times){
          ctx.close();
        }
      }
    };
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch)
          throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new RequestEncoder());
        p.addLast(new ResponseDecoder());
        p.addLast(getClientHandler());
      }
    };
  }

  private void transferMessage(Object msg){
    channel.writeAndFlush(msg);
  }

  public void startTransfer() throws InterruptedException {
    int cur = 1;
    ByteBuffer bf = ByteBuffer.allocateDirect(1024*1024);
    for (int i = 0; i < bf.capacity(); i++) {
      bf.put((byte)'a');
    }
    bf.flip();
    while(cur <= times){
      RequestData msg = new RequestData();
      msg.setDataId(cur);
      bf.position(0).limit(bf.capacity());
      msg.setBuff(bf.slice());
      transferMessage(msg);
      cur++;
    }
    System.out.println("Done....");
  }

  public void setupClient() throws Exception{
    String host = "localhost";
    int port = 50053;
    channel = (new Bootstrap())
        .group(workerGroup)
        .channel(NioSocketChannel.class)
        .handler(getInitializer())
        .option(ChannelOption.SO_KEEPALIVE, true)
        .connect(host, port)
        .sync()
        .channel();
  }

  public static void main(String args[]) throws Exception{
      Client c = new Client();
      c.setupClient();
      c.startTransfer();
  }
}