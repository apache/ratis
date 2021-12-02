/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ratis.netty.server;

import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.DataStreamOutputRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.NettyDataStreamUtils;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
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
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.thirdparty.io.netty.util.AttributeKey;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class NettyServerStreamRpc implements DataStreamServerRpc {
  public static final Logger LOG = LoggerFactory.getLogger(NettyServerStreamRpc.class);

  /** Proxies to other peers. */
  static class Proxies {
    private final PeerProxyMap<DataStreamClient> map;

    Proxies(PeerProxyMap<DataStreamClient> map) {
      this.map = map;
    }

    void addPeers(Collection<RaftPeer> newPeers) {
      // add to the map first in order to preserve the invariant.
      map.addRaftPeers(newPeers);
    }

    Set<DataStreamOutputRpc> getDataStreamOutput(RaftClientRequest request, Set<RaftPeer> peers) throws IOException {
      final Set<DataStreamOutputRpc> outs = new HashSet<>();
      try {
        getDataStreamOutput(request, peers, outs);
      } catch (IOException e) {
        outs.forEach(DataStreamOutputRpc::closeAsync);
        throw e;
      }
      return outs;
    }

    private void getDataStreamOutput(RaftClientRequest request, Set<RaftPeer> peers, Set<DataStreamOutputRpc> outs)
        throws IOException {
      for (RaftPeer peer : peers) {
        try {
          outs.add((DataStreamOutputRpc) map.getProxy(peer.getId()).stream(request));
        } catch (IOException e) {
          throw new IOException(map.getName() + ": Failed to getDataStreamOutput for " + peer, e);
        }
      }
    }

    void close() {
      map.close();
    }
  }

  private final String name;
  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final ChannelFuture channelFuture;

  private final DataStreamManagement requests;
  private final List<Proxies> proxies = new ArrayList<>();

  public NettyServerStreamRpc(RaftServer server) {
    this.name = server.getId() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.requests = new DataStreamManagement(server);

    final RaftProperties properties = server.getProperties();

    int clientPoolSize = RaftServerConfigKeys.DataStream.clientPoolSize(properties);
    for (int i = 0; i < clientPoolSize; i ++) {
      this.proxies.add(new Proxies(new PeerProxyMap<>(name, peer -> newClient(peer, properties))));
    }

    final int port = NettyConfigKeys.DataStream.port(properties);
    this.channelFuture = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(getInitializer())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .bind(port);
  }

  static DataStreamClient newClient(RaftPeer peer, RaftProperties properties) {
    return DataStreamClient.newBuilder()
        .setClientId(ClientId.randomId())
        .setDataStreamServer(peer)
        .setProperties(properties)
        .build();
  }

  @Override
  public void addRaftPeers(Collection<RaftPeer> newPeers) {
    for (int i = 0; i < proxies.size(); i ++) {
      proxies.get(i).addPeers(newPeers);
    }
  }

  private ChannelInboundHandler newChannelInboundHandlerAdapter(){
    return new ChannelInboundHandlerAdapter(){

      private static final String REQUEST_ATTR_NAME = "DATA-STREAM-REQUEST-%s";

      private void setRequest(ChannelHandlerContext ctx, DataStreamRequestByteBuf request) {
        ctx.channel().attr(AttributeKey.newInstance(String.format(REQUEST_ATTR_NAME, ctx.name()))).set(request);
      }

      private DataStreamRequestByteBuf getAndRemoveRequest(ChannelHandlerContext ctx) {
        Object o = ctx.channel().attr(AttributeKey.newInstance(String.format(REQUEST_ATTR_NAME, ctx.name())))
            .getAndRemove();
        if (o instanceof DataStreamRequestByteBuf) {
          return (DataStreamRequestByteBuf) o;
        }
        return null;
      }

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof DataStreamRequestByteBuf)) {
          LOG.error("Unexpected message class {}, ignoring ...", msg.getClass().getName());
          return;
        }
        final DataStreamRequestByteBuf request = (DataStreamRequestByteBuf)msg;
        setRequest(ctx, request);
        int index = Math.toIntExact(
            ((0xFFFFFFFFL & request.getClientId().hashCode()) + request.getStreamId()) % proxies.size());
        requests.read(request, ctx, proxies.get(index)::getDataStreamOutput);
      }

      @Override
      public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        getAndRemoveRequest(ctx);
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable throwable) {
        LOG.warn(name + ": exceptionCaught", throwable);
        Optional.ofNullable(getAndRemoveRequest(ctx))
            .ifPresent(request -> requests.replyDataStreamException(throwable, request, ctx));
        ctx.close();
      }
    };
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(newDecoder());
        p.addLast(newEncoder());
        p.addLast(newChannelInboundHandlerAdapter());
      }
    };
  }

  static ByteToMessageDecoder newDecoder() {
    return new ByteToMessageDecoder() {
      {
        this.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
      }

      @Override
      protected void decode(ChannelHandlerContext context, ByteBuf buf, List<Object> out) {
        Optional.ofNullable(NettyDataStreamUtils.decodeDataStreamRequestByteBuf(buf)).ifPresent(out::add);
      }
    };
  }

  static MessageToMessageEncoder<DataStreamReplyByteBuffer> newEncoder() {
    return new MessageToMessageEncoder<DataStreamReplyByteBuffer>() {
      @Override
      protected void encode(ChannelHandlerContext context, DataStreamReplyByteBuffer reply, List<Object> out) {
        NettyDataStreamUtils.encodeDataStreamReplyByteBuffer(reply, out::add, context.alloc());
      }
    };
  }

  @Override
  public void start() {
    channelFuture.syncUninterruptibly();
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    channelFuture.awaitUninterruptibly();
    return (InetSocketAddress) channelFuture.channel().localAddress();
  }

  @Override
  public void close() {
    try {
      channelFuture.channel().close().sync();
      bossGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
      workerGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
      bossGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
      workerGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error(this + ": Interrupted close()", e);
    }

    for (int i = 0; i < proxies.size(); i ++) {
      proxies.get(i).close();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
