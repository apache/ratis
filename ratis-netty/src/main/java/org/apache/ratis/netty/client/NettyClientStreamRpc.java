package org.apache.ratis.netty.client;

import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.impl.RaftClientImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.decoders.DataStreamReplyDecoder;
import org.apache.ratis.netty.encoders.DataStreamRequestClientEncoder;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamReplyImpl;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ratis.util.NetUtils;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class NettyClientStreamRpc implements DataStreamClientRpc {
  private RaftPeer server;
  private RaftProperties raftProperties;
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private Channel channel;
  private Queue<CompletableFuture<DataStreamReply>> replies
      = new LinkedList<>();

  public NettyClientStreamRpc(RaftPeer server, RaftProperties properties){
    this.server = server;
    this.raftProperties = properties;
  }

  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws InterruptedException {
        final DataStreamReplyImpl reply = (DataStreamReplyImpl) msg;
      }
    };
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch)
          throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new DataStreamRequestClientEncoder());
        p.addLast(new DataStreamReplyDecoder());
        p.addLast(getClientHandler());
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
  public void startClient() throws InterruptedException {
    final InetSocketAddress address = NetUtils.createSocketAddr(server.getAddress());
    channel = (new Bootstrap())
        .group(workerGroup)
        .channel(NioSocketChannel.class)
        .handler(getInitializer())
        .option(ChannelOption.SO_KEEPALIVE, true)
        .connect(address)
        .sync()
        .channel();
  }

  @Override
  public void closeClient(){
    channel.close().syncUninterruptibly();
  }
}
