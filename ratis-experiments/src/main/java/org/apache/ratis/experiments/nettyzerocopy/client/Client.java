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

import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.ratis.proto.ExperimentsProtos.TransferMsgProto;
import org.apache.ratis.proto.ExperimentsProtos.TransferReplyProto;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Client{
  int times = 100000;
  Channel channel;
  EventLoopGroup workerGroup = new NioEventLoopGroup();

  private ChannelInboundHandler getClientHandler(){
    return new SimpleChannelInboundHandler<TransferReply>(){
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, TransferReply proto) {
        if(proto.getId() == times){
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
        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(TransferReplyProto.getDefaultInstance()));
        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());
        p.addLast(getClientHandler());
      }
    };
  }

  private void transferMessage(Object msg){
    channel.writeAndFlush(msg);
  }

  public void startTransfer(){
    int i = 0;
    ByteBuffer bf = ByteBuffer.allocateDirect(1024*1024);
    if(bf.hasArray()){
      Arrays.fill(bf.array(), (byte) 'a');
    }
    while(i < times){
      TransferMsgProto msg = TransferMsgProto.newBuilder().
          setPartId(i).
          setData(UnsafeByteOperations.unsafeWrap(bf)).build();
      transferMessage(msg);
    }
  }

  public void setupClient() throws Exception{
    String host = "localhost";
    int port = 50051;
    channel = (new Bootstrap())
        .group(workerGroup)
        .channel(NioSocketChannel.class)
        .handler(getInitializer())
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