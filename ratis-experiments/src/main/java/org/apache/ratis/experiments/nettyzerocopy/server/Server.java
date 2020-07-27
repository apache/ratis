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


public class Server {
  private EventLoopGroup bossGroup = new NioEventLoopGroup();
  private EventLoopGroup workerGroup = new NioEventLoopGroup();

  public EventLoopGroup getBossGroup(){
    return bossGroup;
  }

  public EventLoopGroup getWorkerGroup() {
    return workerGroup;
  }

  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ResponseData reply = new ResponseData();
        RequestDataComposite req = (RequestDataComposite)msg;
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

  public static void main(String[] args) throws InterruptedException {
    Server s = new Server();
    s.setupServer();
  }
}
