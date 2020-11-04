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
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.NettyDataStreamUtils;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataStream;
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
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

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

    List<DataStreamOutput> getDataStreamOutput(RaftGroupId groupId) throws IOException {
      final List<DataStreamOutput> outs = new ArrayList<>();
      try {
        getDataStreamOutput(outs, groupId);
      } catch (IOException e) {
        outs.forEach(CloseAsync::closeAsync);
        throw e;
      }
      return outs;
    }

    private void getDataStreamOutput(List<DataStreamOutput> outs, RaftGroupId groupId) throws IOException {
      for (RaftPeer peer : peers) {
        try {
          outs.add(map.getProxy(peer.getId()).stream(groupId));
        } catch (IOException e) {
          throw new IOException(map.getName() + ": Failed to getDataStreamOutput for " + peer, e);
        }
      }
    }

    void close() {
      map.close();
    }
  }

  static class StreamInfo {
    private final RaftClientRequest request;
    private final CompletableFuture<DataStream> stream;
    private final List<DataStreamOutput> outs;
    private final AtomicReference<CompletableFuture<?>> previous
        = new AtomicReference<>(CompletableFuture.completedFuture(null));

    StreamInfo(RaftClientRequest request, CompletableFuture<DataStream> stream, List<DataStreamOutput> outs) {
      this.request = request;
      this.stream = stream;
      this.outs = outs;
    }

    CompletableFuture<DataStream> getStream() {
      return stream;
    }

    List<DataStreamOutput> getDataStreamOutputs() {
      return outs;
    }

    AtomicReference<CompletableFuture<?>> getPrevious() {
      return previous;
    }

    RaftClientRequest getRequest() {
      return request;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + ":" + request;
    }
  }

  static class StreamMap {
    static class Key {
      private final ChannelId channelId;
      private final long streamId;

      Key(ChannelId channelId, long streamId) {
        this.channelId = channelId;
        this.streamId = streamId;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        } else if (obj == null || getClass() != obj.getClass()) {
          return false;
        }
        final Key that = (Key) obj;
        return this.streamId == that.streamId && Objects.equals(this.channelId, that.channelId);
      }

      @Override
      public int hashCode() {
        return Objects.hash(channelId, streamId);
      }

      @Override
      public String toString() {
        return channelId + "-" + streamId;
      }
    }

    private final ConcurrentMap<Key, StreamInfo> map = new ConcurrentHashMap<>();

    StreamInfo computeIfAbsent(Key key, Function<Key, StreamInfo> function) {
      final StreamInfo info = map.computeIfAbsent(key, function);
      LOG.debug("computeIfAbsent({}) returns {}", key, info);
      return info;
    }

    StreamInfo get(Key key) {
      final StreamInfo info = map.get(key);
      LOG.debug("get({}) returns {}", key, info);
      return info;
    }
  }

  private final RaftServer server;
  private final String name;
  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final ChannelFuture channelFuture;

  private final StreamMap streams = new StreamMap();

  private final Proxies proxies;

  private final ExecutorService executorService;

  public NettyServerStreamRpc(RaftServer server) {
    this.server = server;
    this.name = server.getId() + "-" + getClass().getSimpleName();

    final RaftProperties properties = server.getProperties();
    final int port = NettyConfigKeys.DataStream.port(properties);
    this.channelFuture = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(getInitializer())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .bind(port);
    this.proxies = new Proxies(new PeerProxyMap<>(name, peer -> newClient(peer, properties)));
    this.executorService = Executors.newFixedThreadPool(
        RaftServerConfigKeys.DataStream.asyncThreadPoolSize(server.getProperties()));
  }

  static DataStreamClient newClient(RaftPeer peer, RaftProperties properties) {
    return DataStreamClient.newBuilder()
        .setRaftServer(peer)
        .setProperties(properties)
        .build();
  }

  @Override
  public void addRaftPeers(Collection<RaftPeer> newPeers) {
    proxies.addPeers(newPeers);
  }

  private StreamInfo newStreamInfo(ByteBuf buf) {
    try {
      final RaftClientRequest request = ClientProtoUtils.toRaftClientRequest(
          RaftClientRequestProto.parseFrom(buf.nioBuffer()));
      final StateMachine stateMachine = server.getStateMachine(request.getRaftGroupId());
      return new StreamInfo(request, stateMachine.data().stream(request),
          proxies.getDataStreamOutput(request.getRaftGroupId()));
    } catch (Throwable e) {
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

  private void sendReplySuccess(DataStreamRequestByteBuf request, ByteBuffer buffer, long bytesWritten,
      ChannelHandlerContext ctx) {
    final DataStreamReplyByteBuffer reply = new DataStreamReplyByteBuffer(
        request.getStreamId(), request.getStreamOffset(), buffer, bytesWritten, true, request.getType());
    LOG.debug("{}: write {}", this, reply);
    ctx.writeAndFlush(reply);
  }

  private void sendReply(List<CompletableFuture<DataStreamReply>> remoteWrites,
      DataStreamRequestByteBuf request, long bytesWritten, ChannelHandlerContext ctx) {
      if (!checkSuccessRemoteWrite(remoteWrites, bytesWritten)) {
        sendReplyNotSuccess(request, ctx);
      } else {
        sendReplySuccess(request, null, bytesWritten, ctx);
      }
  }

  private ChannelInboundHandler newChannelInboundHandlerAdapter(){
    return new ChannelInboundHandlerAdapter(){
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        read(ctx, (DataStreamRequestByteBuf)msg);
      }
    };
  }

  private int startTransaction(StreamInfo info, DataStreamRequestByteBuf request, ChannelHandlerContext ctx) {
    try {
      server.submitClientRequestAsync(info.getRequest()).thenAcceptAsync(reply -> {
        if (reply.isSuccess()) {
          ByteBuffer buffer = ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer();
          sendReplySuccess(request, buffer, -1, ctx);
        } else if (request.getType() == Type.STREAM_CLOSE) {
          // if this server is not the leader, forward start transition to the other peers
          // there maybe other unexpected reason cause failure except not leader, forwardStartTransaction anyway
          forwardStartTransaction(info, request, ctx);
        } else if (request.getType() == Type.START_TRANSACTION){
          sendReplyNotSuccess(request, ctx);
        } else {
          LOG.error("{}: Unexpected type:{}", this, request.getType());
        }
      }, executorService);
    } catch (IOException e) {
      sendReplyNotSuccess(request, ctx);
    }
    return 0;
  }

  private void forwardStartTransaction(
      final StreamInfo info, final DataStreamRequestByteBuf request, final ChannelHandlerContext ctx) {
    final List<CompletableFuture<Boolean>> results = new ArrayList<>();
    for (DataStreamOutput out : info.getDataStreamOutputs()) {
      final CompletableFuture<Boolean> f = out.startTransactionAsync().thenApplyAsync(reply -> {
        if (reply.isSuccess()) {
          final ByteBuffer buffer = reply instanceof DataStreamReplyByteBuffer?
              ((DataStreamReplyByteBuffer)reply).slice(): null;
          sendReplySuccess(request, buffer, -1, ctx);
          return true;
        } else {
          return false;
        }
      }, executorService);

      results.add(f);
    }

    JavaUtils.allOf(results).thenAccept(v -> {
      if (!results.stream().map(CompletableFuture::join).reduce(false, Boolean::logicalOr)) {
        sendReplyNotSuccess(request, ctx);
      }
    });
  }

  private void read(ChannelHandlerContext ctx, DataStreamRequestByteBuf request) {
    LOG.debug("{}: read {}", this, request);
    final ByteBuf buf = request.slice();

    final StreamInfo info;
    final CompletableFuture<Long> localWrite;
    final List<CompletableFuture<DataStreamReply>> remoteWrites = new ArrayList<>();
    final StreamMap.Key key = new StreamMap.Key(ctx.channel().id(), request.getStreamId());
    if (request.getType() == Type.STREAM_HEADER) {
      info = streams.computeIfAbsent(key, id -> newStreamInfo(buf));
      localWrite = CompletableFuture.completedFuture(0L);
      for (DataStreamOutput out : info.getDataStreamOutputs()) {
        remoteWrites.add(out.getHeaderFuture());
      }
    } else if (request.getType() == Type.STREAM_DATA) {
      info = streams.get(key);
      final CompletableFuture<?> previous = info.getPrevious().get();

      localWrite = previous.thenCombineAsync(info.getStream(), (u, stream) -> writeTo(buf, stream), executorService);
      for (DataStreamOutput out : info.getDataStreamOutputs()) {
        remoteWrites.add(previous.thenComposeAsync(v -> out.writeAsync(request.slice().nioBuffer()), executorService));
      }
    } else if (request.getType() == Type.STREAM_CLOSE || request.getType() == Type.STREAM_CLOSE_FORWARD) {
      info = streams.get(key);
      final CompletableFuture<?> previous = info.getPrevious().get();

      localWrite = previous.thenCombineAsync(info.getStream(), (u, stream) -> {
        try {
          stream.getWritableByteChannel().close();
          return 0L;
        } catch (IOException e) {
          throw new CompletionException("Failed to close " + stream, e);
        }
      }, executorService);

      if (request.getType() == Type.STREAM_CLOSE) {
        for (DataStreamOutput out : info.getDataStreamOutputs()) {
          remoteWrites.add(previous.thenComposeAsync(v -> out.closeForwardAsync(), executorService));
        }
      }
    } else {
      // peer server start transaction
      info = streams.get(key);
      final CompletableFuture<?> previous = info.getPrevious().get();
      previous.thenApplyAsync(v -> startTransaction(streams.get(key), request, ctx), executorService);
      return;
    }

    final CompletableFuture<?> current = JavaUtils.allOf(remoteWrites)
        .thenCombineAsync(localWrite, (v, bytesWritten) -> {
          buf.release();
          if (request.getType() == Type.STREAM_HEADER
              || request.getType() == Type.STREAM_DATA
              || request.getType() == Type.STREAM_CLOSE_FORWARD) {
            sendReply(remoteWrites, request, bytesWritten, ctx);
          } else if (request.getType() == Type.STREAM_CLOSE) {
            // after all server close stream, primary server start transaction
            // TODO(runzhiwang): send start transaction to leader directly
            startTransaction(info, request, ctx);
          } else {
            LOG.error("{}: Unexpected type:{}", this, request.getType());
          }
          return null;
        }, executorService);

    info.getPrevious().set(current);
  }

  private boolean checkSuccessRemoteWrite(
          List<CompletableFuture<DataStreamReply>> replyFutures, long bytesWritten) {
    for (CompletableFuture<DataStreamReply> replyFuture : replyFutures) {
      final DataStreamReply reply = replyFuture.join();
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
        p.addLast(newChannelInboundHandlerAdapter());
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
        NettyDataStreamUtils.encodeDataStreamReplyByteBuffer(reply, out::add, context.alloc());
      }
    };
  }

  @Override
  public void start() {
    channelFuture.syncUninterruptibly();
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

    proxies.close();
  }

  @Override
  public String toString() {
    return name;
  }
}
