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
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.io.CloseAsync;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.NettyDataStreamUtils;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
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
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    List<DataStreamOutputRpc> getDataStreamOutput(RaftClientRequest request) throws IOException {
      final List<DataStreamOutputRpc> outs = new ArrayList<>();
      try {
        getDataStreamOutput(outs, request);
      } catch (IOException e) {
        outs.forEach(CloseAsync::closeAsync);
        throw e;
      }
      return outs;
    }

    private void getDataStreamOutput(List<DataStreamOutputRpc> outs, RaftClientRequest request)
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

  static class LocalStream {
    private final CompletableFuture<DataStream> streamFuture;
    private final AtomicReference<CompletableFuture<Long>> writeFuture;

    LocalStream(CompletableFuture<DataStream> streamFuture) {
      this.streamFuture = streamFuture;
      this.writeFuture = new AtomicReference<>(streamFuture.thenApply(s -> 0L));
    }

    CompletableFuture<Long> write(ByteBuf buf, Executor executor) {
      return composeAsync(writeFuture, executor,
          n -> streamFuture.thenApplyAsync(stream -> writeTo(buf, stream), executor));
    }

    CompletableFuture<Long> close(Executor executor) {
      return composeAsync(writeFuture, executor,
          n -> streamFuture.thenApplyAsync(NettyServerStreamRpc::close, executor));
    }
  }

  static class RemoteStream {
    private final DataStreamOutputRpc out;

    RemoteStream(DataStreamOutputRpc out) {
      this.out = out;
    }

    CompletableFuture<DataStreamReply> write(DataStreamRequestByteBuf request) {
      return out.writeAsync(request.slice().nioBuffer());
    }

    CompletableFuture<DataStreamReply> startTransaction(DataStreamRequestByteBuf request, ChannelHandlerContext ctx,
        Executor executor) {
      return out.startTransactionAsync().thenApplyAsync(reply -> {
        if (reply.isSuccess()) {
          final ByteBuffer buffer = reply instanceof DataStreamReplyByteBuffer?
              ((DataStreamReplyByteBuffer)reply).slice(): null;
          sendReplySuccess(request, buffer, -1, ctx);
        }
        return reply;
      }, executor);
    }

    CompletableFuture<DataStreamReply> close() {
      return out.closeAsync();
    }
  }

  static class StreamInfo {
    private final RaftClientRequest request;
    private final boolean primary;
    private final LocalStream local;
    private final List<RemoteStream> remotes;
    private final AtomicReference<CompletableFuture<Void>> previous
        = new AtomicReference<>(CompletableFuture.completedFuture(null));

    StreamInfo(RaftClientRequest request, boolean primary,
        CompletableFuture<DataStream> stream, List<DataStreamOutputRpc> outs) {
      this.request = request;
      this.primary = primary;
      this.local = new LocalStream(stream);
      this.remotes = outs.stream().map(RemoteStream::new).collect(Collectors.toList());
    }

    AtomicReference<CompletableFuture<Void>> getPrevious() {
      return previous;
    }

    RaftClientRequest getRequest() {
      return request;
    }

    boolean isPrimary() {
      return primary;
    }

    LocalStream getLocal() {
      return local;
    }

    <T> List<T> applyToRemotes(Function<RemoteStream, T> function) {
      return remotes.isEmpty()?Collections.emptyList(): remotes.stream().map(function).collect(Collectors.toList());
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" + request;
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

  private final Executor executor;

  public NettyServerStreamRpc(RaftServer server) {
    this.server = server;
    this.name = server.getId() + "-" + JavaUtils.getClassSimpleName(getClass());

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
    this.executor = Executors.newFixedThreadPool(
        RaftServerConfigKeys.DataStream.asyncThreadPoolSize(server.getProperties()));
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
    proxies.addPeers(newPeers);
  }

  private StreamInfo newStreamInfo(ByteBuf buf) {
    try {
      final RaftClientRequest request = ClientProtoUtils.toRaftClientRequest(
          RaftClientRequestProto.parseFrom(buf.nioBuffer()));
      final StateMachine stateMachine = server.getStateMachine(request.getRaftGroupId());
      final boolean isPrimary = server.getId().equals(request.getServerId());
      return new StreamInfo(request, isPrimary, stateMachine.data().stream(request),
          isPrimary? proxies.getDataStreamOutput(request): Collections.emptyList());
    } catch (Throwable e) {
      throw new CompletionException(e);
    }
  }

  static <T> CompletableFuture<T> composeAsync(AtomicReference<CompletableFuture<T>> future, Executor executor,
      Function<T, CompletableFuture<T>> function) {
    final CompletableFuture<T> composed = future.get().thenComposeAsync(function, executor);
    future.set(composed);
    return composed;
  }

  static long writeTo(ByteBuf buf, DataStream stream) {
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

  static long close(DataStream stream) {
    try {
      stream.getWritableByteChannel().close();
      return 0L;
    } catch (IOException e) {
      throw new CompletionException("Failed to close " + stream, e);
    }
  }

  private void sendReplyNotSuccess(DataStreamRequestByteBuf request, ByteBuffer buffer, ChannelHandlerContext ctx) {
    final DataStreamReplyByteBuffer reply = new DataStreamReplyByteBuffer(
        request.getStreamId(), request.getStreamOffset(), buffer, -1, false, request.getType());
    ctx.writeAndFlush(reply);
  }

  static void sendReplySuccess(DataStreamRequestByteBuf request, ByteBuffer buffer, long bytesWritten,
      ChannelHandlerContext ctx) {
    final DataStreamReplyByteBuffer reply = new DataStreamReplyByteBuffer(
        request.getStreamId(), request.getStreamOffset(), buffer, bytesWritten, true, request.getType());
    ctx.writeAndFlush(reply);
  }

  private void sendReply(List<CompletableFuture<DataStreamReply>> remoteWrites,
      DataStreamRequestByteBuf request, long bytesWritten, ChannelHandlerContext ctx) {
      if (!checkSuccessRemoteWrite(remoteWrites, bytesWritten)) {
        sendReplyNotSuccess(request, null, ctx);
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

  private CompletableFuture<Void> startTransaction(StreamInfo info, DataStreamRequestByteBuf request,
      ChannelHandlerContext ctx) {
    try {
      return server.submitClientRequestAsync(info.getRequest()).thenAcceptAsync(reply -> {
        if (reply.isSuccess()) {
          ByteBuffer buffer = ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer();
          sendReplySuccess(request, buffer, -1, ctx);
        } else if (request.getType() == Type.STREAM_CLOSE) {
          // if this server is not the leader, forward start transition to the other peers
          // there maybe other unexpected reason cause failure except not leader, forwardStartTransaction anyway
          forwardStartTransaction(info, request, ctx, reply);
        } else if (request.getType() == Type.START_TRANSACTION){
          ByteBuffer buffer = ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer();
          sendReplyNotSuccess(request, buffer, ctx);
        } else {
          throw new IllegalStateException(this + ": Unexpected type " + request.getType() + ", request=" + request);
        }
      }, executor);
    } catch (IOException e) {
      sendReplyNotSuccess(request, null, ctx);
      return CompletableFuture.completedFuture(null);
    }
  }

  private void sendLeaderFailedReply(final List<CompletableFuture<DataStreamReply>> results,
      final DataStreamRequestByteBuf request, final ChannelHandlerContext ctx, RaftClientReply localReply) {
    // get replies from the results, ignored exceptional replies
    final Stream<RaftClientReply> remoteReplies = results.stream()
        .filter(r -> !r.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .map(this::getRaftClientReply);

    // choose the leader's reply if there is any.  Otherwise, use the local reply
    final RaftClientReply chosen = Stream.concat(Stream.of(localReply), remoteReplies)
        .filter(reply -> reply.getNotLeaderException() == null)
        .findAny().orElse(localReply);

    // send reply
    final ByteBuffer buffer = ClientProtoUtils.toRaftClientReplyProto(chosen).toByteString().asReadOnlyByteBuffer();
    sendReplyNotSuccess(request, buffer, ctx);
  }

  private void forwardStartTransaction(
      final StreamInfo info, final DataStreamRequestByteBuf request,
      final ChannelHandlerContext ctx, RaftClientReply localReply) {
    final List<CompletableFuture<DataStreamReply>> results = info.applyToRemotes(
        out -> out.startTransaction(request, ctx, executor));

    JavaUtils.allOf(results).thenAccept(v -> {
      for (CompletableFuture<DataStreamReply> result : results) {
        if (result.join().isSuccess()) {
          return;
        }
      }

      sendLeaderFailedReply(results, request, ctx, localReply);
    });
  }

  private RaftClientReply getRaftClientReply(DataStreamReply dataStreamReply) {
    if (dataStreamReply instanceof DataStreamReplyByteBuffer) {
      try {
        return ClientProtoUtils.toRaftClientReply(
            RaftClientReplyProto.parseFrom(((DataStreamReplyByteBuffer) dataStreamReply).slice()));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(this + ": Failed to decode RaftClientReply");
      }
    } else {
      throw new IllegalStateException(this + ": Unexpected reply type");
    }
  }

  private void read(ChannelHandlerContext ctx, DataStreamRequestByteBuf request) {
    LOG.debug("{}: read {}", this, request);
    final ByteBuf buf = request.slice();
    final StreamMap.Key key = new StreamMap.Key(ctx.channel().id(), request.getStreamId());

    if (request.getType() == Type.START_TRANSACTION) {
      // for peers to start transaction
      final StreamInfo info = streams.get(key);
      composeAsync(info.getPrevious(), executor, v -> startTransaction(info, request, ctx))
          .thenAccept(v -> buf.release());
      return;
    }

    final StreamInfo info;
    final CompletableFuture<Long> localWrite;
    final List<CompletableFuture<DataStreamReply>> remoteWrites;
    if (request.getType() == Type.STREAM_HEADER) {
      info = streams.computeIfAbsent(key, id -> newStreamInfo(buf));
      localWrite = CompletableFuture.completedFuture(0L);
      remoteWrites = Collections.emptyList();
    } else if (request.getType() == Type.STREAM_DATA) {
      info = streams.get(key);
      localWrite = info.getLocal().write(buf, executor);
      remoteWrites = info.applyToRemotes(out -> out.write(request));
    } else if (request.getType() == Type.STREAM_CLOSE) {
      info = streams.get(key);
      localWrite = info.getLocal().close(executor);
      remoteWrites = info.isPrimary()? info.applyToRemotes(RemoteStream::close): Collections.emptyList();
    } else {
      throw new IllegalStateException(this + ": Unexpected type " + request.getType() + ", request=" + request);
    }

    composeAsync(info.getPrevious(), executor, n -> JavaUtils.allOf(remoteWrites)
        .thenCombineAsync(localWrite, (v, bytesWritten) -> {
          if (request.getType() == Type.STREAM_HEADER
              || request.getType() == Type.STREAM_DATA) {
            sendReply(remoteWrites, request, bytesWritten, ctx);
          } else if (request.getType() == Type.STREAM_CLOSE) {
            if (info.isPrimary()) {
              // after all server close stream, primary server start transaction
              // TODO(runzhiwang): send start transaction to leader directly
              startTransaction(info, request, ctx);
            } else {
              sendReply(remoteWrites, request, bytesWritten, ctx);
            }
          } else {
            throw new IllegalStateException(this + ": Unexpected type " + request.getType() + ", request=" + request);
          }
          buf.release();
          return null;
        }, executor));
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
