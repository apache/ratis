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
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.io.CloseAsync;
import org.apache.ratis.netty.NettyDataStreamUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class NettyServerStreamRpc implements DataStreamServerRpc {
  public static final Logger LOG = LoggerFactory.getLogger(NettyServerStreamRpc.class);

  /**
   * Proxies to other peers.
   *
   * Invariant: all the {@link #peers} must exist in the {@link #map}.
   */
  static class Proxies {
    private final Set<RaftPeer> peers = new CopyOnWriteArraySet<>();
    private final PeerProxyMap<DataStreamClient> map;

    Proxies(PeerProxyMap<DataStreamClient> map) {
      this.map = map;
    }

    void addPeers(Collection<RaftPeer> newPeers) {
      // add to the map first in order to preserve the invariant.
      map.addRaftPeers(newPeers);
      // must use atomic addAll
      peers.addAll(newPeers);
    }

    List<DataStreamOutput> getDataStreamOutput() throws IOException {
      final List<DataStreamOutput> outs = new ArrayList<>();
      try {
        getDataStreamOutput(outs);
      } catch (IOException e) {
        outs.forEach(CloseAsync::closeAsync);
        throw e;
      }
      return outs;
    }

    private void getDataStreamOutput(List<DataStreamOutput> outs) throws IOException {
      for (RaftPeer peer : peers) {
        try {
          outs.add(map.getProxy(peer.getId()).stream());
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

  private final StateMachine stateMachine;
  private final ConcurrentMap<Long, CompletableFuture<DataStream>> streams = new ConcurrentHashMap<>();
  private final ConcurrentMap<Long, List<DataStreamOutput>> peersStreamOutput = new ConcurrentHashMap<>();

  private final Proxies proxies;

  public NettyServerStreamRpc(RaftPeer server, StateMachine stateMachine, RaftProperties properties) {
    this.name = server + "-" + getClass().getSimpleName();
    this.stateMachine = stateMachine;
    this.channelFuture = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(getInitializer())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .localAddress(NetUtils.createSocketAddr(server.getAddress()))
        .bind();

    this.proxies = new Proxies(new PeerProxyMap<>(name, peer -> newClient(peer, properties)));
  }

  static DataStreamClient newClient(RaftPeer peer, RaftProperties properties) {
    final DataStreamClient client = DataStreamClient.newBuilder()
        .setRaftServer(peer)
        .setProperties(properties)
        .build();
    client.start();
    return client;
  }

  @Override
  public void addRaftPeers(Collection<RaftPeer> newPeers) {
    proxies.addPeers(newPeers);
  }

  private CompletableFuture<DataStream> getDataStreamFuture(ByteBuf buf) {
    try {
      final RaftClientRequest request =
          ClientProtoUtils.toRaftClientRequest(RaftProtos.RaftClientRequestProto.parseFrom(buf.nioBuffer()));
      return stateMachine.data().stream(request);
    } catch (InvalidProtocolBufferException e) {
      throw new CompletionException(e);
    }
  }

  private long writeTo(ByteBuf buf, DataStream stream) {
    if (stream == null) {
      return 0;
    }

    final WritableByteChannel channel = stream.getWritableByteChannel();
    long byteWritten = 0;
    for (ByteBuffer buffer : buf.nioBuffers()) {
      try {
        byteWritten += channel.write(buffer);
      } catch (Throwable t) {
        throw new CompletionException(t);
      }
    }
    return byteWritten;
  }

  private void sendReplyNotSuccess(DataStreamRequestByteBuf request, ChannelHandlerContext ctx) {
    final DataStreamReplyByteBuffer reply = new DataStreamReplyByteBuffer(
        request.getStreamId(), request.getStreamOffset(), null, -1, false, request.getType());
    ctx.writeAndFlush(reply);
  }

  private void sendReplySuccess(DataStreamRequestByteBuf request, long bytesWritten, ChannelHandlerContext ctx) {
    final DataStreamReplyByteBuffer reply = new DataStreamReplyByteBuffer(
        request.getStreamId(), request.getStreamOffset(), null, bytesWritten, true, request.getType());
    ctx.writeAndFlush(reply);
  }

  private void sendReply(List<CompletableFuture<DataStreamReply>> remoteWrites,
      DataStreamRequestByteBuf request, long bytesWritten, ChannelHandlerContext ctx) {
    try {
      if (!checkSuccessRemoteWrite(remoteWrites, bytesWritten)) {
        sendReplyNotSuccess(request, ctx);
      } else {
        sendReplySuccess(request, bytesWritten, ctx);
      }
    } catch (ExecutionException | InterruptedException e) {
      IOUtils.asIOException(e);
    }
  }

  private ChannelInboundHandler getServerHandler(){
    return new ChannelInboundHandlerAdapter(){
      private final AtomicReference<CompletableFuture<?>> previous
          = new AtomicReference<>(CompletableFuture.completedFuture(null));

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
        final DataStreamRequestByteBuf request = (DataStreamRequestByteBuf) msg;
        final ByteBuf buf = request.slice();
        final boolean isHeader = request.getType() == Type.STREAM_HEADER;

        final CompletableFuture<Long> localWrite = isHeader ?
                streams.computeIfAbsent(request.getStreamId(), id -> getDataStreamFuture(buf)).thenApply(stream -> 0L)
                : streams.get(request.getStreamId()).thenApply(stream -> writeTo(buf, stream));

        final List<CompletableFuture<DataStreamReply>> remoteWrites = new ArrayList<>();
        if (isHeader) {
          // do not need to forward header request
          final List<DataStreamOutput> outs = proxies.getDataStreamOutput();
          peersStreamOutput.put(request.getStreamId(), outs);
          for (DataStreamOutput out : outs) {
            remoteWrites.add(out.getHeaderFuture());
          }
        } else {
          // body
          for (DataStreamOutput out : peersStreamOutput.get(request.getStreamId())) {
            remoteWrites.add(out.writeAsync(request.slice().nioBuffer()));
          }
        }

        final CompletableFuture<?> current = previous.get()
            .thenCombine(JavaUtils.allOf(remoteWrites), (u, v) -> null)
            .thenCombine(localWrite, (v, bytesWritten) -> {
              buf.release();
              sendReply(remoteWrites, request, bytesWritten, ctx);
              return null;
        });
        previous.set(current);
      }
    };
  }

  private boolean checkSuccessRemoteWrite(
          List<CompletableFuture<DataStreamReply>> replyFutures, long bytesWritten)
          throws ExecutionException, InterruptedException {
    for (CompletableFuture<DataStreamReply> replyFuture : replyFutures) {
      DataStreamReply reply = replyFuture.get();
      if (!reply.isSuccess() || reply.getBytesWritten() != bytesWritten) {
        return false;
      }
    }
    return true;
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(newDecoder());
        p.addLast(newEncoder());
        p.addLast(getServerHandler());
      }
    };
  }

  ByteToMessageDecoder newDecoder() {
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

  MessageToMessageEncoder<DataStreamReplyByteBuffer> newEncoder() {
    return new MessageToMessageEncoder<DataStreamReplyByteBuffer>() {
      @Override
      protected void encode(ChannelHandlerContext context, DataStreamReplyByteBuffer reply, List<Object> out) {
        NettyDataStreamUtils.encodeDataStreamReplyByteBuffer(reply, out::add);
      }
    };
  }

  private Channel getChannel() {
    return channelFuture.awaitUninterruptibly().channel();
  }

  @Override
  public void start() {
    channelFuture.syncUninterruptibly();
  }

  @Override
  public void close() {
    final ChannelFuture f = getChannel().close();
    f.syncUninterruptibly();
    bossGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    workerGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    try {
      bossGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
      workerGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("Interrupt EventLoopGroup terminate", e);
    }

    proxies.close();
  }

  @Override
  public String toString() {
    return name;
  }
}
