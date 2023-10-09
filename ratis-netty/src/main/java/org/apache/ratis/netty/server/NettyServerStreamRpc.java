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
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.NettyDataStreamUtils;
import org.apache.ratis.netty.NettyUtils;
import org.apache.ratis.netty.metrics.NettyServerStreamRpcMetrics;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamPacket;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.security.TlsConf;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandler;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandler;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
          outs.add((DataStreamOutputRpc) map.computeIfAbsent(peer).get().stream(request));
        } catch (IOException e) {
          map.handleException(peer.getId(), e, true);
          throw new IOException(map.getName() + ": Failed to getDataStreamOutput for " + peer, e);
        }
      }
    }

    void close() {
      map.close();
    }
  }

  static class ProxiesPool {
    private final List<Proxies> list;

    ProxiesPool(String name, RaftProperties properties, Parameters parameters) {
      final int clientPoolSize = RaftServerConfigKeys.DataStream.clientPoolSize(properties);
      final List<Proxies> proxies = new ArrayList<>(clientPoolSize);
      for (int i = 0; i < clientPoolSize; i++) {
        proxies.add(new Proxies(new PeerProxyMap<>(name, peer -> newClient(peer, properties, parameters))));
      }
      this.list = Collections.unmodifiableList(proxies);
    }

    void addRaftPeers(Collection<RaftPeer> newPeers) {
      list.forEach(proxy -> proxy.addPeers(newPeers));
    }

    Proxies get(DataStreamPacket p) {
      final long hash = Integer.toUnsignedLong(Objects.hash(p.getClientId(), p.getStreamId()));
      return list.get(Math.toIntExact(hash % list.size()));
    }

    void close() {
      list.forEach(Proxies::close);
    }
  }

  private final String name;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ChannelFuture channelFuture;

  private final DataStreamManagement requests;
  private final ProxiesPool proxies;

  private final NettyServerStreamRpcMetrics metrics;

  private final TimeDuration channelInactiveGracePeriod;

  public NettyServerStreamRpc(RaftServer server, Parameters parameters) {
    this.name = server.getId() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.metrics = new NettyServerStreamRpcMetrics(this.name);
    this.requests = new DataStreamManagement(server, metrics);

    final RaftProperties properties = server.getProperties();

    this.channelInactiveGracePeriod = NettyConfigKeys.DataStream.Server
        .channelInactiveGracePeriod(properties);

    this.proxies = new ProxiesPool(name, properties, parameters);

    final boolean useEpoll = NettyConfigKeys.DataStream.Server.useEpoll(properties);
    this.bossGroup = NettyUtils.newEventLoopGroup(name + "-bossGroup",
        NettyConfigKeys.DataStream.Server.bossGroupSize(properties), useEpoll);
    this.workerGroup = NettyUtils.newEventLoopGroup(name + "-workerGroup",
        NettyConfigKeys.DataStream.Server.workerGroupSize(properties), useEpoll);

    final TlsConf tlsConf = NettyConfigKeys.DataStream.Server.tlsConf(parameters);
    final SslContext sslContext = NettyUtils.buildSslContextForServer(tlsConf);
    final String host = NettyConfigKeys.DataStream.host(server.getProperties());
    final int port = NettyConfigKeys.DataStream.port(properties);
    InetSocketAddress socketAddress =
            host == null || host.isEmpty() ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
    this.channelFuture = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NettyUtils.getServerChannelClass(bossGroup))
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(newChannelInitializer(sslContext))
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .bind(socketAddress);
  }

  static DataStreamClient newClient(RaftPeer peer, RaftProperties properties, Parameters parameters) {
    return DataStreamClient.newBuilder()
        .setClientId(ClientId.randomId())
        .setDataStreamServer(peer)
        .setProperties(properties)
        .setParameters(parameters)
        .build();
  }

  @Override
  public void addRaftPeers(Collection<RaftPeer> newPeers) {
    proxies.addRaftPeers(newPeers);
  }

  static class RequestRef {
    private final AtomicReference<DataStreamRequestByteBuf> ref = new AtomicReference<>();

    UncheckedAutoCloseable set(DataStreamRequestByteBuf current) {
      final DataStreamRequestByteBuf previous = ref.getAndUpdate(p -> p == null ? current : p);
      Preconditions.assertNull(previous, () -> "previous = " + previous + " != null, current = " + current);

      return () -> Preconditions.assertSame(current, getAndSetNull(), "RequestRef");
    }

    DataStreamRequestByteBuf getAndSetNull() {
      return ref.getAndSet(null);
    }
  }

  private ChannelInboundHandler newChannelInboundHandlerAdapter(){
    return new ChannelInboundHandlerAdapter(){
      private final RequestRef requestRef = new RequestRef();

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        metrics.onRequestCreate(NettyServerStreamRpcMetrics.RequestType.CHANNEL_READ);
        if (!(msg instanceof DataStreamRequestByteBuf)) {
          LOG.error("Unexpected message class {}, ignoring ...", msg.getClass().getName());
          return;
        }

        final DataStreamRequestByteBuf request = (DataStreamRequestByteBuf)msg;
        try(UncheckedAutoCloseable autoReset = requestRef.set(request)) {
          requests.read(request, ctx, proxies.get(request)::getDataStreamOutput);
        }
      }

      @Override
      public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        requests.cleanUpOnChannelInactive(ctx.channel().id(), channelInactiveGracePeriod);
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable throwable) {
        Optional.ofNullable(requestRef.getAndSetNull())
            .ifPresent(request -> requests.replyDataStreamException(throwable, request, ctx));
      }
    };
  }

  private ChannelInitializer<SocketChannel> newChannelInitializer(SslContext sslContext){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        if (sslContext != null) {
          p.addLast("ssl", sslContext.newHandler(ch.alloc()));
        }
        p.addLast(newDecoder());
        p.addLast(ENCODER);
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

  static final MessageToMessageEncoder<DataStreamReplyByteBuffer> ENCODER = new Encoder();

  @ChannelHandler.Sharable
  static class Encoder extends MessageToMessageEncoder<DataStreamReplyByteBuffer> {
    @Override
    protected void encode(ChannelHandlerContext context, DataStreamReplyByteBuffer reply, List<Object> out) {
      NettyDataStreamUtils.encodeDataStreamReplyByteBuffer(reply, out::add, context.alloc());
    }
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
      ConcurrentUtils.shutdownAndWait(TimeDuration.ONE_SECOND, bossGroup,
          timeout -> LOG.warn("{}: bossGroup shutdown timeout in " + timeout, this));
      ConcurrentUtils.shutdownAndWait(TimeDuration.ONE_SECOND, workerGroup,
          timeout -> LOG.warn("{}: workerGroup shutdown timeout in " + timeout, this));
    } catch (InterruptedException e) {
      LOG.error(this + ": Interrupted close()", e);
    }

    proxies.close();
  }

  @Override
  public String toString() {
    return name;
  }
}
