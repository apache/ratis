package org.apache.ratis.experiments.nettyzerocopy.server;

import org.apache.ratis.flatbufs.TransferMsg;
import org.apache.ratis.proto.netty.NettyProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;

public class Server {
  EventLoopGroup bossGroup = new NioEventLoopGroup();
  EventLoopGroup workerGroup = new NioEventLoopGroup();

  private ChannelInboundHandler getClientHandler(){
    return new SimpleChannelInboundHandler<TransferMsgProto>(){
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, TransferMsgProto proto) {
        final TransferReplyProto reply = TransferReplyProto.newBuilder()
            .setPartId(proto.getId())
            .setMessage("OK").build();
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
        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(TransferMsgProto.getDefaultInstance()));
        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());
        p.addLast(getClientHandler());
      }
    };
  }

  public void setupServer(){
    int port = 50053;
    String host = "localhost";
    new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(getInitializer())
        .bind(port)
        .syncUninterruptibly();
  }

  public static void main(String args[]){
    Server s = new Server();
    s.setupServer();
  }
}
