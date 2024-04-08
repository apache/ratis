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
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuf;
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
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
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

    private static final AtomicReference<CompletableFuture<ReferenceCountedObject<EventLoopGroup>>> SHARED_WORKER_GROUP
        = new AtomicReference<>();

    static WorkerGroupGetter newInstance(RaftProperties properties) {
      final boolean shared = NettyConfigKeys.DataStream.Client.workerGroupShare(properties);
      if (shared) {
        final CompletableFuture<ReferenceCountedObject<EventLoopGroup>> created = new CompletableFuture<>();
        final CompletableFuture<ReferenceCountedObject<EventLoopGroup>> current
            = SHARED_WORKER_GROUP.updateAndGet(g -> g != null ? g : created);
        if (current == created) {
          created.complete(ReferenceCountedObject.wrap(newWorkerGroup(properties)));
        }
        return new WorkerGroupGetter(current.join().retain()) {
          @Override
          void shutdownGracefully() {
            final CompletableFuture<ReferenceCountedObject<EventLoopGroup>> returned
                = SHARED_WORKER_GROUP.updateAndGet(previous -> {
              Preconditions.assertSame(current, previous, "SHARED_WORKER_GROUP");
              return previous.join().release() ? null : previous;
            });
            if (returned == null) {
              get().shutdownGracefully();
            }
          }
        };
      } else {
        return new WorkerGroupGetter(newWorkerGroup(properties));
      }
    }

    static EventLoopGroup newWorkerGroup(RaftProperties properties) {
      return NettyUtils.newEventLoopGroup(
          JavaUtils.getClassSimpleName(NettyClientStreamRpc.class) + "-workerGroup",
          NettyConfigKeys.DataStream.Client.workerGroupSize(properties),
          NettyConfigKeys.DataStream.Client.useEpoll(properties));
    }

    private final EventLoopGroup workerGroup;

    private WorkerGroupGetter(EventLoopGroup workerGroup) {
      this.workerGroup = workerGroup;
    }

    @Override
    public final EventLoopGroup get() {
      return workerGroup;
    }

    void shutdownGracefully() {
      workerGroup.shutdownGracefully();
    }
  }

  static class Connection {
    static final TimeDuration RECONNECT = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);

    private final InetSocketAddress address;
    private final WorkerGroupGetter workerGroup;
    private final Supplier<ChannelInitializer<SocketChannel>> channelInitializerSupplier;

    /** The {@link ChannelFuture} is null when this connection is closed. */
    private final AtomicReference<MemoizedSupplier<ChannelFuture>> ref;

    Connection(InetSocketAddress address, WorkerGroupGetter workerGroup,
        Supplier<ChannelInitializer<SocketChannel>> channelInitializerSupplier) {
      this.address = address;
      this.workerGroup = workerGroup;
      this.channelInitializerSupplier = channelInitializerSupplier;
      this.ref = new AtomicReference<>(MemoizedSupplier.valueOf(this::connect));
    }

    ChannelFuture getChannelFuture() {
      final Supplier<ChannelFuture> referenced = ref.get();
      return referenced != null? referenced.get(): null;
    }

    Channel getChannelUninterruptibly() {
      final ChannelFuture future = getChannelFuture();
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
      if (isClosed()) {
        return null;
      }
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
                scheduleReconnect(Connection.this + " failed", future.cause());
              } else {
                LOG.trace("{} succeed.", Connection.this);
              }
            }
          });
    }

    void scheduleReconnect(String message, Throwable cause) {
      if (isClosed()) {
        return;
      }
      LOG.warn("{}: {}; schedule reconnecting to {} in {}", this, message, address, RECONNECT);
      if (cause != null) {
        LOG.warn("", cause);
      }
      getWorkerGroup().schedule(this::reconnect, RECONNECT.getDuration(), RECONNECT.getUnit());
    }

    private synchronized ChannelFuture reconnect() {
      // concurrent reconnect double check
      final ChannelFuture channelFuture = getChannelFuture();
      if (channelFuture != null) {
        Channel channel = channelFuture.syncUninterruptibly().channel();
        if (channel.isActive()) {
          return channelFuture;
        }
      }

      // Two levels of MemoizedSupplier as a side-effect-free function:
      // AtomicReference.getAndUpdate may call the update function multiple times and discard the old objects.
      // The outer supplier creates only an inner supplier, which can be discarded without any leakage.
      // The inner supplier will be invoked (i.e. connect) ONLY IF it is successfully set to the reference.
      final MemoizedSupplier<MemoizedSupplier<ChannelFuture>> supplier = MemoizedSupplier.valueOf(
          () -> MemoizedSupplier.valueOf(this::connect));
      final MemoizedSupplier<ChannelFuture> previous = ref.getAndUpdate(prev -> prev == null? null: supplier.get());
      if (previous != null && previous.isInitialized()) {
        previous.get().channel().close();
      }
      return getChannelFuture();
    }

    void close() {
      final MemoizedSupplier<ChannelFuture> previous = ref.getAndSet(null);
      if (previous != null && previous.isInitialized()) {
        // wait channel closed, do shutdown workerGroup
        previous.get().channel().close().addListener(future -> workerGroup.shutdownGracefully());
      } else {
        workerGroup.shutdownGracefully();
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

  static class OutstandingRequests {
    private int count;
    private long bytes;

    private boolean shouldFlush(List<WriteOption> options, int countMin, SizeInBytes bytesMin) {
      if (options.contains(StandardWriteOption.CLOSE)) {
        // flush in order to send the CLOSE option.
        return true;
      } else if (bytes == 0 && count == 0) {
        // nothing to flush (when bytes == 0 && count > 0, client may have written empty packets for including options)
        return false;
      } else {
        return count >= countMin
            || bytes >= bytesMin.getSize()
            || options.contains(StandardWriteOption.FLUSH);
      }
    }

    synchronized boolean shouldFlush(int countMin, SizeInBytes bytesMin, DataStreamRequest request) {
      final List<WriteOption> options;
      if (request == null) {
        options = Collections.emptyList();
      } else {
        options = request.getWriteOptionList();
        count++;
        final long length = request.getDataLength();
        Preconditions.assertTrue(length >= 0, () -> "length = " + length + " < 0, request: " + request);
        bytes += length;
      }

      final boolean flush = shouldFlush(options, countMin, bytesMin);
      LOG.debug("flush? {}, (count, bytes)=({}, {}), min=({}, {}), request={}, options={}",
          flush, count, bytes, countMin, bytesMin, request, options);
      if (flush) {
        count = 0;
        bytes = 0;
      }
      return flush;
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
    this.name = JavaUtils.getClassSimpleName(getClass()) + "->" + server.getId();
    this.requestTimeout = RaftClientConfigKeys.DataStream.requestTimeout(properties);
    this.closeTimeout = requestTimeout.multiply(2);

    this.flushRequestCountMin = RaftClientConfigKeys.DataStream.flushRequestCountMin(properties);
    this.flushRequestBytesMin = RaftClientConfigKeys.DataStream.flushRequestBytesMin(properties);

    final InetSocketAddress address = NetUtils.createSocketAddr(server.getDataStreamAddress());
    final SslContext sslContext = NettyUtils.buildSslContextForClient(tlsConf);
    this.connection = new Connection(address, WorkerGroupGetter.newInstance(properties),
        () -> newChannelInitializer(address, sslContext, getClientHandler()));
  }

  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof DataStreamReply)) {
          LOG.error("{}: unexpected message {}", name, msg.getClass());
          return;
        }
        final DataStreamReply reply = (DataStreamReply) msg;
        LOG.debug("{}: read {}", name, reply);
        final ClientInvocationId clientInvocationId = ClientInvocationId.valueOf(
            reply.getClientId(), reply.getStreamId());
        final NettyClientReplies.ReplyMap replyMap = replies.getReplyMap(clientInvocationId);
        if (replyMap == null) {
          LOG.error("{}: {} replyMap not found for reply: {}", name, clientInvocationId, reply);
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
        connection.scheduleReconnect("channel is inactive", null);
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
        p.addLast(ENCODER_BYTE_BUF);
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

  static final MessageToMessageEncoder<DataStreamRequestByteBuf> ENCODER_BYTE_BUF = new EncoderByteBuf();

  @ChannelHandler.Sharable
  static class EncoderByteBuf extends MessageToMessageEncoder<DataStreamRequestByteBuf> {
    @Override
    protected void encode(ChannelHandlerContext context, DataStreamRequestByteBuf request, List<Object> out) {
      NettyDataStreamUtils.encodeDataStreamRequestByteBuf(request, out::add, context.alloc());
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
      final Function<DataStreamRequest, ChannelFuture> writeMethod = outstandingRequests.shouldFlush(
          flushRequestCountMin, flushRequestBytesMin, request)? channel::writeAndFlush: channel::write;
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
    final boolean flush = outstandingRequests.shouldFlush(0, SizeInBytes.ZERO, null);
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
