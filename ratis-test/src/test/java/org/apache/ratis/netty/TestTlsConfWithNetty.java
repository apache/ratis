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
package org.apache.ratis.netty;

import org.apache.ratis.security.SecurityTestUtils;
import org.apache.ratis.security.TlsConf;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandler;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.util.JavaUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


/**
 * Testing {@link TlsConf} and the security related utility methods in {@link NettyUtils}.
 */
public class TestTlsConfWithNetty {
  private final static Logger LOG = LoggerFactory.getLogger(TestTlsConfWithNetty.class);

  static String buffer2String(ByteBuf buf){
    try {
      return buf.toString(StandardCharsets.UTF_8);
    } finally {
      buf.release();
    }
  }

  static ByteBuf unpooledBuffer(String s) {
    final ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(s.getBytes(StandardCharsets.UTF_8));
    return buf;
  }

  static int randomPort() {
    final int port = 50000 + ThreadLocalRandom.current().nextInt(10000);
    LOG.info("randomPort: {}", port);
    return port;
  }

  @Test
  public void testNoSsl() throws Exception {
    runTest(randomPort(), null, null);
  }

  @Test
  public void testSsl() throws Exception {
    final TlsConf serverTlsConfig = SecurityTestUtils.newServerTlsConfig(true);
    final TlsConf clientTlsConfig = SecurityTestUtils.newClientTlsConfig(true);
    runTest(randomPort(), serverTlsConfig, clientTlsConfig);
  }

  static void runTest(int port, TlsConf serverSslConf, TlsConf clientSslConf) throws Exception {
    final SslContext serverSslContext = serverSslConf == null? null
        : NettyUtils.buildSslContextForServer(serverSslConf);
    final SslContext clientSslContext = clientSslConf == null? null
        : NettyUtils.buildSslContextForClient(clientSslConf);

    final String message = "Hey, how are you?";
    final String[] words = message.split(" ");
    try (NettyTestServer server = new NettyTestServer(port, serverSslContext);
         NettyTestClient client = new NettyTestClient("localhost", port, clientSslContext)) {
      final List<CompletableFuture<String>> replyFutures = new ArrayList<>();
      for(String word : words) {
        final ByteBuf buf = unpooledBuffer(word + " ");
        final CompletableFuture<String> f = client.writeAndFlush(buf);
        replyFutures.add(f);
      }
      for(int i = 0; i < replyFutures.size(); i++) {
        final CompletableFuture<String> future = replyFutures.get(i);
        final String reply = future.get(3, TimeUnit.SECONDS);
        LOG.info(reply);
        Assert.assertEquals(NettyTestServer.toReply(words[i]), reply);
      }
    }
  }

  static class NettyTestServer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(NettyTestServer.class);

    static final String CLASS_NAME = JavaUtils.getClassSimpleName(NettyTestServer.class);

    private final EventLoopGroup bossGroup = NettyUtils.newEventLoopGroup(
        CLASS_NAME + "-bossGroup", 3, true);
    private final EventLoopGroup workerGroup = NettyUtils.newEventLoopGroup(
        CLASS_NAME + "-workerGroup", 3, true);
    private final ChannelFuture channelFuture;

    public NettyTestServer(int port, SslContext sslContext) {
      this.channelFuture = new ServerBootstrap()
          .group(bossGroup, workerGroup)
          .channel(NettyUtils.getServerChannelClass(bossGroup))
          .handler(new LoggingHandler(getClass(), LogLevel.INFO))
          .childHandler(newChannelInitializer(sslContext))
          .bind(port)
          .syncUninterruptibly();
    }

    private ChannelInitializer<SocketChannel> newChannelInitializer(SslContext sslContext){
      return new ChannelInitializer<SocketChannel>(){
        @Override
        public void initChannel(SocketChannel ch) {
          final ChannelPipeline p = ch.pipeline();
          if (sslContext != null) {
            p.addLast("ssl", sslContext.newHandler(ch.alloc()));
          }
          p.addLast(newServerHandler());
        }
      };
    }

    private ChannelInboundHandler newServerHandler(){
      return new ChannelInboundHandlerAdapter() {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object obj) {
          if (obj instanceof ByteBuf) {
            final String s = buffer2String((ByteBuf) obj);
            LOG.info("channelRead: " + s);
            for(String word : s.split(" ")) {
              ctx.writeAndFlush(unpooledBuffer(toReply(word) + " "));
            }
          }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable throwable) {
          LOG.error(NettyTestServer.this.getClass().getSimpleName() + ": exceptionCaught", throwable);
          ctx.close();
        }
      };
    }

    static String toReply(String request) {
      return "[" + request + "]";
    }


    @Override
    public void close() {
      channelFuture.channel().close();
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  static class NettyTestClient implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(NettyTestClient.class);

    private final EventLoopGroup workerGroup = new NioEventLoopGroup(3);
    private final ChannelFuture channelFuture;


    private final Queue<CompletableFuture<String>> queue = new LinkedList<>();

    public NettyTestClient(String host, int port, SslContext sslContext) {
      this.channelFuture = new Bootstrap()
          .group(workerGroup)
          .channel(NioSocketChannel.class)
          .handler(new LoggingHandler(getClass(), LogLevel.INFO))
          .handler(newChannelInitializer(sslContext, host, port))
          .option(ChannelOption.SO_KEEPALIVE, true)
          .option(ChannelOption.TCP_NODELAY, true)
          .connect(host, port)
          .syncUninterruptibly();
    }

    public CompletableFuture<String> writeAndFlush(ByteBuf buf) {
      final CompletableFuture<String> reply = new CompletableFuture<>();
      queue.offer(reply);
      channelFuture.channel().writeAndFlush(buf);
      return reply;
    }

    private ChannelInitializer<SocketChannel> newChannelInitializer(SslContext sslContext, String host, int port) {
      return new ChannelInitializer<SocketChannel>(){
        @Override
        public void initChannel(SocketChannel ch) {
          ChannelPipeline p = ch.pipeline();
          if (sslContext != null) {
            p.addLast("ssl", sslContext.newHandler(ch.alloc(), host, port));
          }
          p.addLast(getClientHandler());
        }
      };
    }

    private ChannelInboundHandler getClientHandler(){
      return new ChannelInboundHandlerAdapter(){
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object obj) {
          final String s = buffer2String((ByteBuf) obj);
          LOG.info("received: " + s);
          for(String word : s.split(" ")) {
            queue.remove().complete(word);
          }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
          LOG.error(NettyTestClient.this.getClass().getSimpleName() + ": exceptionCaught", cause);
          ctx.close();
        }
      };
    }

    @Override
    public void close() {
      channelFuture.channel().close();
      workerGroup.shutdownGracefully();
    }
  }
}
