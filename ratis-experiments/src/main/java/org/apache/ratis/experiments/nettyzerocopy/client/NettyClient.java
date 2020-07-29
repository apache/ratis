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

import org.apache.ratis.experiments.nettyzerocopy.objects.RequestData;
import org.apache.ratis.experiments.nettyzerocopy.encoders.RequestEncoder;
import org.apache.ratis.experiments.nettyzerocopy.objects.ResponseData;
import org.apache.ratis.experiments.nettyzerocopy.decoders.ResponseDecoder;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.ByteBuffer;

/**
 * A client program the utilizes Netty async-I/O to
 * stream 10,000 messages using {@link RequestEncoder}
 * with zero-copy semantics.
 */
public class NettyClient {
  private final int times = 10000;
  private Channel channel;
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private long startTime;

  public int getTimes() {
    return times;
  }

  public EventLoopGroup getWorkerGroup() {
    return workerGroup;
  }

  public Channel getChannel() {
    return channel;
  }

  public long getStartTime() {
    return startTime;
  }

  public void timeClient(){
    long endTime = System.nanoTime();
    System.out.printf("Time taken by Client to send %d messages is %f seconds\n",
        times, (endTime - startTime)/(1_000_000_000.0));
  }

  /**
   * Closes client worker, invoked on program completion
   * @throws InterruptedException
   */
  private void closeClient() throws InterruptedException {
    try {
      channel.closeFuture().sync();
    } finally {
      workerGroup.shutdownGracefully();
    }
  }

  /**
   * Client handler for server messages.
   * @return ChannelInboundHandler
   */
  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws InterruptedException {
        final ResponseData reply = (ResponseData)msg;
        if(reply.getId() == times){
          System.out.println("Received all messages, closing client.");
          timeClient();
          closeClient();
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
    startTime = System.nanoTime();
    while(cur <= times){
      RequestData msg = new RequestData();
      msg.setDataId(cur);
      bf.position(0).limit(bf.capacity());
      msg.setBuff(bf.slice());
      transferMessage(msg);
      cur++;
    }
    System.out.println("Sent all messages.");
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

  public static void main(String[] args) throws Exception{
      NettyClient c = new NettyClient();
      c.setupClient();
      c.startTransfer();
  }
}