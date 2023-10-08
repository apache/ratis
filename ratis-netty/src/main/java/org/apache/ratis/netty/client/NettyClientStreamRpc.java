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

package org.apache.ratis.netty.client;

import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestFilePositionCount;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.NettyDataStreamUtils;
import org.apache.ratis.netty.NettyUtils;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.security.TlsConf;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.Channel;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFutureListener;
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
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.thirdparty.io.netty.util.concurrent.ScheduledFuture;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.ratis.datastream.impl.DataStreamPacketByteBuffer.EMPTY_BYTE_BUFFER;

public class NettyClientStreamRpc implements DataStreamClientRpc {
  public static final Logger LOG = LoggerFactory.getLogger(NettyClientStreamRpc.class);

  private static class WorkerGroupGetter implements Supplier<EventLoopGroup> {
    private static final AtomicReference<EventLoopGroup> SHARED_WORKER_GROUP = new AtomicReference<>();

    static EventLoopGroup newWorkerGroup(RaftProperties properties) {
      return NettyUtils.newEventLoopGroup(
          JavaUtils.getClassSimpleName(NettyClientStreamRpc.class) + "-workerGroup",
          NettyConfigKeys.DataStream.Client.workerGroupSize(properties),
          NettyConfigKeys.DataStream.Client.useEpoll(properties));
    }

    private final EventLoopGroup workerGroup;
    private final boolean ignoreShutdown;

    WorkerGroupGetter(RaftProperties properties) {
      if (NettyConfigKeys.DataStream.Client.workerGroupShare(properties)) {
        workerGroup = SHARED_WORKER_GROUP.updateAndGet(g -> g != null? g: newWorkerGroup(properties));
        ignoreShutdown = true;
      } else {
        workerGroup = newWorkerGroup(properties);
        ignoreShutdown = false;
      }
    }

    @Override
    public EventLoopGroup get() {
      return workerGroup;
    }

    void shutdownGracefully() {
      if (!ignoreShutdown) {
        workerGroup.shutdownGracefully();
      }
    }
  }

  static class Connection {
    static final TimeDuration RECONNECT = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);

    private final InetSocketAddress address;
    private final WorkerGroupGetter workerGroup;
    private final Supplier<ChannelInitializer<SocketChannel>> channelInitializerSupplier;

    /** The {@link ChannelFuture} is null when this connection is closed. */
    private final AtomicReference<ChannelFuture> ref;

    Connection(InetSocketAddress address, WorkerGroupGetter workerGroup,
        Supplier<ChannelInitializer<SocketChannel>> channelInitializerSupplier) {
      this.address = address;
      this.workerGroup = workerGroup;
      this.channelInitializerSupplier = channelInitializerSupplier;
      this.ref = new AtomicReference<>(connect());
    }

    Channel getChannelUninterruptibly() {
      final ChannelFuture future = ref.get();
      if (future == null) {
        return null; //closed
      }
      final Channel channel = future.syncUninterruptibly().channel();
      if (channel.isActive()) {
        return channel;
      }
      ChannelFuture f = reconnect();
      return f == null ? null : f.syncUninterruptibly().channel();
    }

    private EventLoopGroup getWorkerGroup() {
      return workerGroup.get();
    }

    private ChannelFuture connect() {
      return new Bootstrap()
          .group(getWorkerGroup())
          .channel(NettyUtils.getSocketChannelClass(getWorkerGroup()))
          .handler(channelInitializerSupplier.get())
          .option(ChannelOption.SO_KEEPALIVE, true)
          .connect(address)
          .addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
              if (!future.isSuccess()) {
                scheduleReconnect(this + " failed", future.cause());
              } else {
                LOG.trace("{} succeed.", this);
              }
            }
          });
    }

    void scheduleReconnect(String message, Throwable cause) {
      LOG.warn("{}: {}; schedule reconnecting to {} in {}", this, message, address, RECONNECT);
      if (cause != null) {
        LOG.warn("", cause);
      }
      getWorkerGroup().schedule(this::reconnect, RECONNECT.getDuration(), RECONNECT.getUnit());
    }

    private synchronized ChannelFuture reconnect() {
      // concurrent reconnect double check
      ChannelFuture channelFuture = ref.get();
      if (channelFuture != null) {
        Channel channel = channelFuture.syncUninterruptibly().channel();
        if (channel.isActive()) {
          return channelFuture;
        }
      }

      final MemoizedSupplier<ChannelFuture> supplier = MemoizedSupplier.valueOf(this::connect);
      final ChannelFuture previous = ref.getAndUpdate(prev -> prev == null? null: supplier.get());
      if (previous != null) {
        previous.channel().close();
      }
      return supplier.isInitialized() ? supplier.get() : null;
    }

    void close() {
      final ChannelFuture previous = ref.getAndSet(null);
      if (previous != null) {
        // wait channel closed, do shutdown workerGroup
        previous.channel().close().addListener((future) -> workerGroup.shutdownGracefully());
      }
    }

    boolean isClosed() {
      return ref.get() == null;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + "-" + address;
    }
  }

  class OutstandingRequests {
    private int count;
    private long bytes;

    synchronized boolean write(DataStreamRequest request) {
      count++;
      bytes += request.getDataLength();
      final List<WriteOption> options = request.getWriteOptionList();
      final boolean isClose = options.contains(StandardWriteOption.CLOSE);
      final boolean isFlush = options.contains(StandardWriteOption.FLUSH);
      final boolean flush = shouldFlush(isClose || isFlush, flushRequestCountMin, flushRequestBytesMin);
      LOG.debug("Stream{} outstanding: count={}, bytes={}, options={}, flush? {}",
          request.getStreamId(), count, bytes, options, flush);
      return flush;
    }

    synchronized boolean shouldFlush(boolean force, int countMin, SizeInBytes bytesMin) {
      if (force || count >= countMin || bytes >= bytesMin.getSize()) {
        count = 0;
        bytes = 0;
        return true;
      }
      return false;
    }
  }

  private final String name;
  private final Connection connection;

  private final NettyClientReplies replies = new NettyClientReplies();
  private final TimeDuration requestTimeout;
  private final TimeDuration closeTimeout;

  private final int flushRequestCountMin;
  private final SizeInBytes flushRequestBytesMin;
  private final OutstandingRequests outstandingRequests = new OutstandingRequests();

  public NettyClientStreamRpc(RaftPeer server, TlsConf tlsConf, RaftProperties properties) {
    this.name = JavaUtils.getClassSimpleName(getClass()) + "->" + server;
    this.requestTimeout = RaftClientConfigKeys.DataStream.requestTimeout(properties);
    this.closeTimeout = requestTimeout.multiply(2);

    this.flushRequestCountMin = RaftClientConfigKeys.DataStream.flushRequestCountMin(properties);
    this.flushRequestBytesMin = RaftClientConfigKeys.DataStream.flushRequestBytesMin(properties);

    final InetSocketAddress address = NetUtils.createSocketAddr(server.getDataStreamAddress());
    final SslContext sslContext = NettyUtils.buildSslContextForClient(tlsConf);
    this.connection = new Connection(address,
        new WorkerGroupGetter(properties),
        () -> newChannelInitializer(address, sslContext, getClientHandler()));
  }

  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof DataStreamReply)) {
          LOG.error("{}: unexpected message {}", this, msg.getClass());
          return;
        }
        final DataStreamReply reply = (DataStreamReply) msg;
        LOG.debug("{}: read {}", this, reply);
        final ClientInvocationId clientInvocationId = ClientInvocationId.valueOf(
            reply.getClientId(), reply.getStreamId());
        final NettyClientReplies.ReplyMap replyMap = replies.getReplyMap(clientInvocationId);
        if (replyMap == null) {
          LOG.error("{}: {} replyMap not found for reply: {}", this, clientInvocationId, reply);
          return;
        }

        try {
          replyMap.receiveReply(reply);
        } catch (Throwable cause) {
          LOG.warn(name + ": channelRead error:", cause);
          replyMap.completeExceptionally(cause);
        }
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn(name + ": exceptionCaught", cause);

        ctx.close();
      }

      @Override
      public void channelInactive(ChannelHandlerContext ctx) {
        if (!connection.isClosed()) {
          connection.scheduleReconnect("channel is inactive", null);
        }
      }
    };
  }

  static ChannelInitializer<SocketChannel> newChannelInitializer(
      InetSocketAddress address, SslContext sslContext, ChannelInboundHandler handler) {
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        if (sslContext != null) {
          p.addLast("ssl", sslContext.newHandler(ch.alloc(), address.getHostName(), address.getPort()));
        }
        p.addLast(ENCODER);
        p.addLast(ENCODER_FILE_POSITION_COUNT);
        p.addLast(ENCODER_BYTE_BUFFER);
        p.addLast(newDecoder());
        p.addLast(handler);
      }
    };
  }

  static final MessageToMessageEncoder<DataStreamRequestByteBuffer> ENCODER = new Encoder();

  @ChannelHandler.Sharable
  static class Encoder extends MessageToMessageEncoder<DataStreamRequestByteBuffer> {
    @Override
    protected void encode(ChannelHandlerContext context, DataStreamRequestByteBuffer request, List<Object> out) {
      NettyDataStreamUtils.encodeDataStreamRequestByteBuffer(request, out::add, context.alloc());
    }
  }

  static final MessageToMessageEncoder<DataStreamRequestFilePositionCount> ENCODER_FILE_POSITION_COUNT
      = new EncoderFilePositionCount();

  @ChannelHandler.Sharable
  static class EncoderFilePositionCount extends MessageToMessageEncoder<DataStreamRequestFilePositionCount> {
    @Override
    protected void encode(ChannelHandlerContext ctx, DataStreamRequestFilePositionCount request, List<Object> out) {
      NettyDataStreamUtils.encodeDataStreamRequestFilePositionCount(request, out::add, ctx.alloc());
    }
  }

  static final MessageToMessageEncoder<ByteBuffer> ENCODER_BYTE_BUFFER = new EncoderByteBuffer();

  @ChannelHandler.Sharable
  static class EncoderByteBuffer extends MessageToMessageEncoder<ByteBuffer> {
    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuffer request, List<Object> out) {
      NettyDataStreamUtils.encodeByteBuffer(request, out::add);
    }
  }

  static ByteToMessageDecoder newDecoder() {
    return new ByteToMessageDecoder() {
      {
        this.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
      }

      @Override
      protected void decode(ChannelHandlerContext context, ByteBuf buf, List<Object> out) {
        Optional.ofNullable(NettyDataStreamUtils.decodeDataStreamReplyByteBuffer(buf)).ifPresent(out::add);
      }
    };
  }

  @Override
  public CompletableFuture<DataStreamReply> streamAsync(DataStreamRequest request) {
    final CompletableFuture<DataStreamReply> f = new CompletableFuture<>();
    ClientInvocationId clientInvocationId = ClientInvocationId.valueOf(request.getClientId(), request.getStreamId());
    final boolean isClose = request.getWriteOptionList().contains(StandardWriteOption.CLOSE);

    final NettyClientReplies.ReplyMap replyMap = replies.getReplyMap(clientInvocationId);
    final ChannelFuture channelFuture;
    final Channel channel;
    final NettyClientReplies.RequestEntry requestEntry = new NettyClientReplies.RequestEntry(request);
    final NettyClientReplies.ReplyEntry replyEntry;
    LOG.debug("{}: write begin {}", this, request);
    synchronized (replyMap) {
      channel = connection.getChannelUninterruptibly();
      if (channel == null) {
        f.completeExceptionally(new AlreadyClosedException(this + ": Failed to send " + request));
        return f;
      }
      replyEntry = replyMap.submitRequest(requestEntry, isClose, f);
      final Function<DataStreamRequest, ChannelFuture> writeMethod = outstandingRequests.write(request)?
          channel::writeAndFlush: channel::write;
      channelFuture = writeMethod.apply(request);
    }
    channelFuture.addListener(future -> {
      if (!future.isSuccess()) {
        final IOException e = new IOException(this + ": Failed to send " + request + " to " + channel.remoteAddress(),
            future.cause());
        f.completeExceptionally(e);
        replyMap.fail(requestEntry);
        LOG.error("Channel write failed", e);
      } else {
        LOG.debug("{}: write after {}", this, request);

        final TimeDuration timeout = isClose ? closeTimeout : requestTimeout;
        // if reply success cancel this future
        final ScheduledFuture<?> timeoutFuture = channel.eventLoop().schedule(() -> {
          if (!f.isDone()) {
            f.completeExceptionally(new TimeoutIOException(
                "Timeout " + timeout + ": Failed to send " + request + " channel: " + channel));
            replyMap.fail(requestEntry);
          }
        }, timeout.toLong(timeout.getUnit()), timeout.getUnit());
        replyEntry.setTimeoutFuture(timeoutFuture);
      }
    });
    return f;
  }

  @Override
  public void close() {
    final boolean flush = outstandingRequests.shouldFlush(true, 0, SizeInBytes.ZERO);
    LOG.debug("flush? {}", flush);
    if (flush) {
      Optional.ofNullable(connection.getChannelUninterruptibly())
          .map(c -> c.writeAndFlush(EMPTY_BYTE_BUFFER))
          .ifPresent(f -> f.addListener(dummy -> connection.close()));
    } else {
      connection.close();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
