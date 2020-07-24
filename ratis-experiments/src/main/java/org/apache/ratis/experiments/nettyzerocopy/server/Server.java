package org.apache.ratis.experiments.nettyzerocopy.server;

import org.apache.ratis.experiments.nettyzerocopy.RequestData;
import org.apache.ratis.experiments.nettyzerocopy.RequestDecoder;
import org.apache.ratis.experiments.nettyzerocopy.ResponseData;
import org.apache.ratis.experiments.nettyzerocopy.ResponseEncoder;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;


public class Server {
  EventLoopGroup bossGroup = new NioEventLoopGroup();
  EventLoopGroup workerGroup = new NioEventLoopGroup();

  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ResponseData reply = new ResponseData();
        RequestData req = (RequestData)msg;
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
        p.addLast(new RequestDecoder());
        p.addLast(new ResponseEncoder());
        p.addLast(getClientHandler());
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

  public static void main(String args[]) throws InterruptedException {
    Server s = new Server();
    s.setupServer();
  }
}
