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
package org.apache.ratis.quic;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicClientCodecBuilder;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerRequestProto;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexReplyProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.quic.codec.ShadedProtobufDecoder;
import org.apache.ratis.quic.codec.ShadedProtobufEncoder;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto.RaftNettyServerReplyCase.EXCEPTIONREPLY;

/**
 * Client-side QUIC proxy for one remote Raft peer.
 *
 * <p>Maintains a single {@link QuicChannel} with persistent bidirectional streams.
 * In the default layout a server-to-server connection carries one stream per message
 * type (AppendEntries, Heartbeat, InstallSnapshot, RequestVote, plus one for the
 * remaining requests), so a heartbeat never queues behind a log batch. With
 * {@link QuicConfigKeys.Server#SINGLE_STREAM_KEY} set, the connection carries one
 * stream for everything, which restores the ordering of a TCP connection; the layout
 * is a configuration parameter, the wire protocol is otherwise unchanged.  When the
 * connection drops, {@link #scheduleReconnect()} is called automatically and a
 * new {@link Connection} is established after a short delay — matching the
 * reconnect behaviour that Netty/TCP gets for free from {@code PeerProxyMap}.
 */
public class QuicRpcProxy implements Closeable {

  public static final Logger LOG = LoggerFactory.getLogger(QuicRpcProxy.class);

  /** Max wait for a single QUIC connect attempt (bounded, like Netty's
   *  ChannelOption.CONNECT_TIMEOUT_MILLIS). connect() is synchronous: on failure it
   *  throws and PeerProxyMap recreates the proxy on the next request — no in-proxy
   *  async reconnect / null "reconnecting" state (that combination busy-looped into a
   *  connection storm under load). 30 s = Netty's default CONNECT_TIMEOUT_MILLIS, so both
   *  transports wait equally long for a handshake; bounded so a request to a dead leader
   *  eventually fails over instead of blocking forever. */
  private static final long CONNECT_TIMEOUT_MS = 30_000;

  /** Diagnostics only: with -Dratis.quic.connect.timing=true every connect() prints how its
   *  latency splits between building the quiche codec, binding the UDP socket, the handshake
   *  itself and opening the first stream. Off by default; no effect on behaviour. */
  private static final boolean CONNECT_TIMING = Boolean.getBoolean("ratis.quic.connect.timing");
  private long timingCodecNanos;
  private long timingBindNanos;

  // ---- PeerMap ------------------------------------------------------------

  public static class PeerMap extends PeerProxyMap<QuicRpcProxy> {

    /** Every client in this JVM shares one event loop group. Netty's own guidance is that an
     *  EventLoopGroup is meant to be shared: giving each PeerMap its own means connection mode A
     *  builds a pool of (2 x cores) threads per request, and shutdownGracefully() then keeps the
     *  threads of each closed pool alive for its default two-second quiet period — so hundreds of
     *  threads end up competing for a handful of cores. That hurts QUIC far more than TCP, whose
     *  handshake runs in the kernel rather than on these threads. */
    private static final EventLoopGroup SHARED_CLIENT_GROUP = new NioEventLoopGroup(0,
        (java.util.concurrent.ThreadFactory) r -> {
          final Thread t = new Thread(r, "QuicRpcProxy-client-");
          t.setDaemon(true);
          return t;
        });

    private final EventLoopGroup group;
    /** False for {@link #SHARED_CLIENT_GROUP}, which outlives any single PeerMap. */
    private final boolean ownsGroup;

    /** Server-server proxy map (creates the 4 Raft-consensus streams per connection). */
    public PeerMap(String name, RaftProperties properties) {
      this(name, properties, false);
    }

    /** @param clientMode true for external client connections, which only need the
     *  single client-request stream (not the 4 server-server streams). */
    public PeerMap(String name, RaftProperties properties, boolean clientMode) {
      // A server keeps its own group: there is exactly one per server, created at startup.
      this(name, properties, clientMode,
          clientMode ? SHARED_CLIENT_GROUP
              : new NioEventLoopGroup(0,
                  (java.util.concurrent.ThreadFactory) r ->
                      new Thread(r, "QuicRpcProxy-" + name + "-")),
          !clientMode);
    }

    private PeerMap(String name, RaftProperties properties, boolean clientMode,
        EventLoopGroup group, boolean ownsGroup) {
      super(name, peer -> {
        try {
          final QuicSslContext sslCtx = buildClientSslContext(properties);
          return new QuicRpcProxy(peer, properties, group, sslCtx, clientMode);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw IOUtils.toInterruptedIOException(
              "Interrupted connecting to " + peer, e);
        }
      });
      this.group = group;
      this.ownsGroup = ownsGroup;
    }

    @Override
    public void close() {
      super.close();
      if (ownsGroup) {
        group.shutdownGracefully();
      }
    }
  }

  // ---- Connection (holds QuicChannel + 4 streams + 4 handlers) ------------

  /**
   * All mutable connection state bundled into one object so it can be swapped
   * atomically via {@link AtomicReference} on reconnect.
   */
  private class Connection {
    final QuicChannel quicChannel;
    final QuicStreamChannel appendEntriesStream;
    final QuicStreamChannel heartbeatStream;
    final QuicStreamChannel installSnapshotStream;
    final QuicStreamChannel requestVoteStream;
    final QuicStreamChannel clientRequestStream;
    final StreamHandler appendEntriesHandler;
    final StreamHandler heartbeatHandler;
    final StreamHandler installSnapshotHandler;
    final StreamHandler requestVoteHandler;
    final StreamHandler clientRequestHandler;

    Connection(QuicChannel qc,
        QuicStreamChannel ae, QuicStreamChannel hb,
        QuicStreamChannel is, QuicStreamChannel rv,
        QuicStreamChannel cr,
        StreamHandler aeH, StreamHandler hbH,
        StreamHandler isH, StreamHandler rvH,
        StreamHandler crH) {
      this.quicChannel           = qc;
      this.appendEntriesStream   = ae;
      this.heartbeatStream       = hb;
      this.installSnapshotStream = is;
      this.requestVoteStream     = rv;
      this.clientRequestStream   = cr;
      this.appendEntriesHandler  = aeH;
      this.heartbeatHandler      = hbH;
      this.installSnapshotHandler = isH;
      this.requestVoteHandler    = rvH;
      this.clientRequestHandler  = crH;
    }

    void failAll(Throwable cause) {
      // Server-server handlers are null on client connections (client mode). In the
      // single-stream layout all five slots hold the same handler: the first call clears
      // its pending map, the remaining ones are no-ops.
      for (StreamHandler h : new StreamHandler[] {appendEntriesHandler, heartbeatHandler,
          installSnapshotHandler, requestVoteHandler, clientRequestHandler}) {
        if (h != null) {
          h.failAll(cause);
        }
      }
    }
  }

  // ---- Per-stream handler -------------------------------------------------

  class StreamHandler extends SimpleChannelInboundHandler<RaftNettyServerReplyProto> {

    /** True for the four long-lived per-connection streams (ae/hb/is/rv).
     *  False for ephemeral per-request streams opened by sendOnNewStream(). */
    private final boolean persistent;
    private final Map<Long, CompletableFuture<RaftNettyServerReplyProto>> pending =
        new ConcurrentHashMap<>();

    StreamHandler(boolean persistent) {
      this.persistent = persistent;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx,
        RaftNettyServerReplyProto proto) {
      final long callId = getCallId(proto);
      final CompletableFuture<RaftNettyServerReplyProto> future =
          pending.remove(callId);
      if (future == null) {
        LOG.debug("{}: no pending request for callId={}", peer, callId);
        return;
      }
      if (proto.getRaftNettyServerReplyCase() == EXCEPTIONREPLY) {
        future.completeExceptionally(
            (IOException) org.apache.ratis.util.ProtoUtils.toObject(
                proto.getExceptionReply().getException()));
      } else {
        future.complete(proto);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.warn("{}: stream exception", peer, cause);
      failAll(new IOException("Stream error to " + peer, cause));
      ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      // Fail in-flight requests, like NettyRpcProxy.failOutstandingRequests. No in-proxy
      // reconnect: the AlreadyClosedException is retriable, so the client's PeerProxyMap
      // discards and recreates this proxy (fresh connect) on the next request.
      failAll(new AlreadyClosedException("Stream to " + peer + " is inactive"));
      super.channelInactive(ctx);
    }

    CompletableFuture<RaftNettyServerReplyProto> send(
        QuicStreamChannel streamChannel, RaftNettyServerRequestProto request) {
      final CompletableFuture<RaftNettyServerReplyProto> future =
          new CompletableFuture<>();
      final long callId = getCallIdFromRequest(request);
      pending.put(callId, future);

      final java.util.concurrent.ScheduledFuture<?> timer =
          streamChannel.eventLoop().schedule(() -> {
            if (pending.remove(callId, future)) {
              // Fail the request; the retriable TimeoutIOException lets the client's
              // PeerProxyMap discard and recreate this proxy on the next attempt.
              future.completeExceptionally(
                  new org.apache.ratis.protocol.exceptions.TimeoutIOException(
                      "Request " + callId + " to " + peer + " timed out"));
            }
          }, requestTimeout.getDuration(), requestTimeout.getUnit());

      future.whenComplete((r, t) -> timer.cancel(false));

      streamChannel.writeAndFlush(request).addListener(cf -> {
        if (!cf.isSuccess()) {
          if (pending.remove(callId, future)) {
            future.completeExceptionally(cf.cause());
          }
        }
      });
      return future;
    }

    void failAll(Throwable cause) {
      if (!pending.isEmpty()) {
        pending.values().forEach(f -> f.completeExceptionally(cause));
        pending.clear();
      }
    }
  }

  // ---- Fields -------------------------------------------------------------

  private final RaftPeer peer;
  private final TimeDuration requestTimeout;
  private final QuicSslContext sslCtx;
  private final EventLoopGroup group;

  /** Current active UDP socket. Replaced on every connect attempt so that a
   *  failed/cancelled handshake never leaves stale codec state behind. */
  private volatile Channel udpChannel;

  /** Current live connection; null only after {@link #close()}. */
  private final AtomicReference<Connection> connectionRef = new AtomicReference<>();

  private volatile boolean closed = false;

  /** True for external client connections: only the client-request stream is created,
   *  not the 4 server-server (AppendEntries/Heartbeat/InstallSnapshot/RequestVote) streams
   *  that a client never uses. Keeps the client's handshake light (1 stream vs 5). */
  private final boolean clientMode;

  /** Single-stream layout for server-to-server connections
   *  ({@link QuicConfigKeys.Server#SINGLE_STREAM_KEY}): one persistent stream carries every
   *  message type. Always false in client mode, which opens a single stream anyway. */
  private final boolean singleStream;

  // ---- Construction -------------------------------------------------------

  QuicRpcProxy(RaftPeer peer, RaftProperties properties, EventLoopGroup group,
      QuicSslContext sslCtx, boolean clientMode) throws InterruptedException, IOException {
    this.peer           = peer;
    this.requestTimeout = RaftClientConfigKeys.Rpc.requestTimeout(properties);
    this.sslCtx         = sslCtx;
    this.group          = group;
    this.clientMode     = clientMode;
    this.singleStream   = !clientMode && QuicConfigKeys.Server.singleStream(properties);

    // Connect synchronously, like NettyRpcProxy. If the peer is unreachable the connect
    // throws and the exception propagates to PeerProxyMap, which recreates the proxy on
    // the next request. No in-proxy async reconnect / null "reconnecting" state.
    connectionRef.set(connect());
  }

  /** Creates a fresh NIO-UDP socket with its own QUIC codec instance.
   *  Called before every connection attempt so that a cancelled/failed prior attempt
   *  cannot leave stale quiche state in the codec. */
  private Channel newUdpChannel() throws InterruptedException {
    final long t0 = System.nanoTime();
    final ChannelHandler codec = new QuicClientCodecBuilder()
        .sslContext(sslCtx)
        .maxIdleTimeout(0, TimeUnit.MILLISECONDS)
        .initialMaxData(128 * 1024 * 1024)
        .initialMaxStreamDataBidirectionalLocal(16 * 1024 * 1024)
        .initialMaxStreamDataBidirectionalRemote(16 * 1024 * 1024)
        .initialMaxStreamsBidirectional(100)
        .build();
    final long t1 = System.nanoTime();
    final Channel ch = new Bootstrap()
        .group(group)
        .channel(NioDatagramChannel.class)
        .handler(codec)
        .bind(0)
        .sync()
        .channel();
    if (CONNECT_TIMING) {
      timingCodecNanos = t1 - t0;
      timingBindNanos = System.nanoTime() - t1;
    }
    return ch;
  }

  /**
   * Opens a fresh {@link QuicChannel} to the peer and creates the persistent streams on
   * top of it: one per message type (default), a single one for everything
   * ({@link #singleStream}), or just the client-request stream in client mode.
   * Called on first connect and on every reconnect.
   */
  private Connection connect() throws InterruptedException, IOException {
    final long connectStartNanos = System.nanoTime();
    final InetSocketAddress remoteAddr = NetUtils.createSocketAddr(peer.getAddress());

    // Fresh UDP channel + QUIC codec per attempt — avoids stale quiche state after cancel().
    final Channel freshUdp = newUdpChannel();

    final io.netty.util.concurrent.Future<QuicChannel> connectFuture =
        QuicChannel.newBootstrap(freshUdp)
            .remoteAddress(remoteAddr)
            .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.close(); // server-initiated streams not expected
              }
            })
            .connect();

    if (!connectFuture.await(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
      freshUdp.close(); // causes the pending connect future to fail naturally, avoiding cancel() race
      throw new AlreadyClosedException("QUIC connect to " + peer + " timed out after " + CONNECT_TIMEOUT_MS + "ms");
    }
    if (!connectFuture.isSuccess()) {
      freshUdp.close();
      throw new AlreadyClosedException("QUIC connect to " + peer + " failed: "
          + connectFuture.cause());
    }

    // Handshake succeeded — swap in the new channel and discard the old one.
    final Channel old = udpChannel;
    udpChannel = freshUdp;
    if (old != null) old.close();

    final QuicChannel qc = connectFuture.getNow();

    // Client connections only ever send RaftClientRequests, so they open a single
    // client-request stream (multiplexed by callId, like NettyRpcProxy's one channel).
    // Server-server connections additionally open the 4 Raft-consensus streams. Opening
    // only what is needed keeps the client handshake light (1 stream vs 5), which matters
    // under concurrency where each openStream().sync() adds serial event-loop work.
    final long tHandshakeDone = System.nanoTime();
    if (singleStream) {
      // Single-stream layout: one persistent stream carries every message type, so a
      // heartbeat waits behind a log batch exactly as on a TCP connection. All five slots
      // of the Connection alias the same stream and handler, so sendAsync() keeps its
      // per-type dispatch untouched and every reply lands in the one callId map.
      final StreamHandler oneH = new StreamHandler(true);
      final QuicStreamChannel one = openStream(qc, QuicRpcService.TAG_PEER_SINGLE, oneH);
      printConnectTiming(connectStartNanos, tHandshakeDone);
      return new Connection(qc, one, one, one, one, one, oneH, oneH, oneH, oneH, oneH);
    }

    final StreamHandler crH  = new StreamHandler(true);
    final QuicStreamChannel cr = openStream(qc, QuicRpcService.TAG_CLIENT_REQUEST, crH);
    printConnectTiming(connectStartNanos, tHandshakeDone);

    if (clientMode) {
      return new Connection(qc, null, null, null, null, cr, null, null, null, null, crH);
    }

    final StreamHandler aeH  = new StreamHandler(true);
    final StreamHandler hbH  = new StreamHandler(true);
    final StreamHandler isH  = new StreamHandler(true);
    final StreamHandler rvH  = new StreamHandler(true);

    final QuicStreamChannel ae = openStream(qc, QuicRpcService.TAG_APPEND_ENTRIES,   aeH);
    final QuicStreamChannel hb = openStream(qc, QuicRpcService.TAG_HEARTBEAT,        hbH);
    final QuicStreamChannel is = openStream(qc, QuicRpcService.TAG_INSTALL_SNAPSHOT, isH);
    final QuicStreamChannel rv = openStream(qc, QuicRpcService.TAG_REQUEST_VOTE,     rvH);

    return new Connection(qc, ae, hb, is, rv, cr, aeH, hbH, isH, rvH, crH);
  }

  /** {@link #CONNECT_TIMING} only: how the latency of connect() split between the codec,
   *  the UDP bind, the handshake and the first stream opened on the connection. */
  private void printConnectTiming(long connectStartNanos, long tHandshakeDone) {
    if (!CONNECT_TIMING) {
      return;
    }
    final double ms = 1_000_000.0;
    System.err.printf("QUIC-CONNECT codec=%.1fms bind=%.1fms handshake=%.1fms stream=%.1fms%n",
        timingCodecNanos / ms, timingBindNanos / ms,
        (tHandshakeDone - connectStartNanos) / ms - (timingCodecNanos + timingBindNanos) / ms,
        (System.nanoTime() - tHandshakeDone) / ms);
  }

  // ---- Stream helpers -----------------------------------------------------

  private QuicStreamChannel openStream(QuicChannel qc, byte tag, StreamHandler handler)
      throws InterruptedException {

    final ChannelInboundHandlerAdapter tagWriter = new ChannelInboundHandlerAdapter() {
      @Override
      public void channelActive(ChannelHandlerContext ctx) {
        final ByteBuf buf = ctx.alloc().buffer(1).writeByte(tag);
        ctx.writeAndFlush(buf);
        ctx.pipeline().remove(this);
        ctx.fireChannelActive();
      }
    };

    return qc.createStream(QuicStreamType.BIDIRECTIONAL,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel ch) {
            final ChannelPipeline p = ch.pipeline();
            p.addLast(tagWriter);
            p.addLast(new ProtobufVarint32FrameDecoder());
            p.addLast(new ShadedProtobufDecoder<>(
                RaftNettyServerReplyProto.getDefaultInstance()));
            p.addLast(new ProtobufVarint32LengthFieldPrepender());
            p.addLast(ShadedProtobufEncoder.INSTANCE);
            p.addLast(handler);
          }
        }).sync().getNow();
  }

  // ---- Public API ---------------------------------------------------------

  public CompletableFuture<RaftNettyServerReplyProto> sendAsync(
      RaftNettyServerRequestProto proto) {

    final Connection conn = connectionRef.get();
    if (conn == null) {
      final CompletableFuture<RaftNettyServerReplyProto> f = new CompletableFuture<>();
      f.completeExceptionally(new AlreadyClosedException("Proxy to " + peer + " is closed"));
      return f;
    }

    final QuicStreamChannel stream;
    final StreamHandler handler;

    switch (proto.getRaftNettyServerRequestCase()) {
      case APPENDENTRIESREQUEST:
        if (proto.getAppendEntriesRequest().getEntriesCount() == 0) {
          stream  = conn.heartbeatStream;
          handler = conn.heartbeatHandler;
        } else {
          stream  = conn.appendEntriesStream;
          handler = conn.appendEntriesHandler;
        }
        break;
      case INSTALLSNAPSHOTREQUEST:
        stream  = conn.installSnapshotStream;
        handler = conn.installSnapshotHandler;
        break;
      case REQUESTVOTEREQUEST:
      case STARTLEADERELECTIONREQUEST:
        stream  = conn.requestVoteStream;
        handler = conn.requestVoteHandler;
        break;
      default:
        // External client requests (RaftClientRequest, group/config/etc.) all share the
        // persistent client-request stream, multiplexed by callId — same model as
        // NettyRpcProxy's single reused channel.
        stream  = conn.clientRequestStream;
        handler = conn.clientRequestHandler;
        break;
    }
    return handler.send(stream, proto);
  }

  public RaftNettyServerReplyProto send(RaftRpcRequestProto rpcRequest,
      RaftNettyServerRequestProto proto) throws IOException {
    final CompletableFuture<RaftNettyServerReplyProto> future = sendAsync(proto);
    try {
      final TimeDuration timeout = requestTimeout.add(
          rpcRequest.getTimeoutMs(), TimeUnit.MILLISECONDS);
      return future.get(timeout.getDuration(), timeout.getUnit());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(
          org.apache.ratis.util.ProtoUtils.toString(rpcRequest)
              + " interrupted sending to " + peer, e);
    } catch (ExecutionException e) {
      throw IOUtils.toIOException(e);
    } catch (TimeoutException e) {
      throw new org.apache.ratis.protocol.exceptions.TimeoutIOException(
          e.getMessage(), e);
    }
  }

  public CompletableFuture<ReadIndexReplyProto> readIndexAsync(
      ReadIndexRequestProto request) {
    final CompletableFuture<ReadIndexReplyProto> result = new CompletableFuture<>();
    final Connection conn = connectionRef.get();
    if (conn == null) {
      result.completeExceptionally(new AlreadyClosedException("Proxy to " + peer + " is closed"));
      return result;
    }
    try {
      final CompletableFuture<ReadIndexReplyProto> replyFuture = new CompletableFuture<>();
      final SimpleChannelInboundHandler<ReadIndexReplyProto> replyHandler =
          new SimpleChannelInboundHandler<ReadIndexReplyProto>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx,
                ReadIndexReplyProto reply) {
              replyFuture.complete(reply);
            }
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
              replyFuture.completeExceptionally(cause);
              ctx.close();
            }
          };

      final ChannelInboundHandlerAdapter tagWriter = new ChannelInboundHandlerAdapter() {
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
          final ByteBuf buf = ctx.alloc().buffer(1)
              .writeByte(QuicRpcService.TAG_READ_INDEX);
          ctx.writeAndFlush(buf);
          ctx.pipeline().remove(this);
          ctx.fireChannelActive();
        }
      };

      final QuicStreamChannel ch = conn.quicChannel.createStream(
          QuicStreamType.BIDIRECTIONAL,
          new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              final ChannelPipeline p = ch.pipeline();
              p.addLast(tagWriter);
              p.addLast(new ProtobufVarint32FrameDecoder());
              p.addLast(new ShadedProtobufDecoder<>(
                  ReadIndexReplyProto.getDefaultInstance()));
              p.addLast(new ProtobufVarint32LengthFieldPrepender());
              p.addLast(ShadedProtobufEncoder.INSTANCE);
              p.addLast(replyHandler);
            }
          }).sync().getNow();

      ch.writeAndFlush(request).addListener(cf -> {
        if (!cf.isSuccess()) {
          replyFuture.completeExceptionally(cf.cause());
        }
      });

      replyFuture.whenComplete((reply, ex) -> {
        ch.close();
        if (ex != null) {
          result.completeExceptionally(ex);
        } else {
          result.complete(reply);
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      result.completeExceptionally(e);
    }
    return result;
  }

  @Override
  public void close() {
    closed = true;
    final Connection conn = connectionRef.getAndSet(null);
    if (conn != null) {
      conn.quicChannel.close();
    }
    final Channel ch = udpChannel;
    if (ch != null) ch.close();
  }

  // ---- Helpers ------------------------------------------------------------

  static long getCallId(RaftNettyServerReplyProto proto) {
    switch (proto.getRaftNettyServerReplyCase()) {
      case REQUESTVOTEREPLY:
        return proto.getRequestVoteReply().getServerReply().getCallId();
      case STARTLEADERELECTIONREPLY:
        return proto.getStartLeaderElectionReply().getServerReply().getCallId();
      case APPENDENTRIESREPLY:
        return proto.getAppendEntriesReply().getServerReply().getCallId();
      case INSTALLSNAPSHOTREPLY:
        return proto.getInstallSnapshotReply().getServerReply().getCallId();
      case RAFTCLIENTREPLY:
        return proto.getRaftClientReply().getRpcReply().getCallId();
      case GROUPLISTREPLY:
        return proto.getGroupListReply().getRpcReply().getCallId();
      case GROUPINFOREPLY:
        return proto.getGroupInfoReply().getRpcReply().getCallId();
      case EXCEPTIONREPLY:
        return proto.getExceptionReply().getRpcReply().getCallId();
      default:
        throw new UnsupportedOperationException(
            "Reply case not supported: " + proto.getRaftNettyServerReplyCase());
    }
  }

  static long getCallIdFromRequest(RaftNettyServerRequestProto proto) {
    final RaftRpcRequestProto rpc;
    switch (proto.getRaftNettyServerRequestCase()) {
      case REQUESTVOTEREQUEST:
        rpc = proto.getRequestVoteRequest().getServerRequest(); break;
      case APPENDENTRIESREQUEST:
        rpc = proto.getAppendEntriesRequest().getServerRequest(); break;
      case INSTALLSNAPSHOTREQUEST:
        rpc = proto.getInstallSnapshotRequest().getServerRequest(); break;
      case STARTLEADERELECTIONREQUEST:
        rpc = proto.getStartLeaderElectionRequest().getServerRequest(); break;
      case RAFTCLIENTREQUEST:
        rpc = proto.getRaftClientRequest().getRpcRequest(); break;
      case SETCONFIGURATIONREQUEST:
        rpc = proto.getSetConfigurationRequest().getRpcRequest(); break;
      case GROUPMANAGEMENTREQUEST:
        rpc = proto.getGroupManagementRequest().getRpcRequest(); break;
      case GROUPLISTREQUEST:
        rpc = proto.getGroupListRequest().getRpcRequest(); break;
      case GROUPINFOREQUEST:
        rpc = proto.getGroupInfoRequest().getRpcRequest(); break;
      case TRANSFERLEADERSHIPREQUEST:
        rpc = proto.getTransferLeadershipRequest().getRpcRequest(); break;
      case SNAPSHOTMANAGEMENTREQUEST:
        rpc = proto.getSnapshotManagementRequest().getRpcRequest(); break;
      case LEADERELECTIONMANAGEMENTREQUEST:
        rpc = proto.getLeaderElectionManagementRequest().getRpcRequest(); break;
      default:
        throw new UnsupportedOperationException(
            "Cannot extract callId for: " + proto.getRaftNettyServerRequestCase());
    }
    return rpc.getCallId();
  }

  public static QuicSslContext buildClientSslContext(RaftProperties properties) {
    final String caCert    = QuicConfigKeys.Client.tlsCaCert(properties);
    final boolean insecure = QuicConfigKeys.Client.tlsInsecure(properties);
    final QuicSslContextBuilder b = QuicSslContextBuilder.forClient()
        .applicationProtocols(QuicConfigKeys.ALPN);
    if (insecure) {
      b.trustManager(InsecureTrustManagerFactory.INSTANCE);
    } else if (caCert != null) {
      b.trustManager(new File(caCert));
    }
    final String cert = QuicConfigKeys.Client.tlsCert(properties);
    final String key  = QuicConfigKeys.Client.tlsKey(properties);
    if (cert != null && key != null) {
      b.keyManager(new File(key), null, new File(cert));
    }
    return b.build();
  }
}
