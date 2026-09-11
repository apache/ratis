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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicServerCodecBuilder;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexReplyProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupListRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.LeaderElectionManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.proto.RaftProtos.SnapshotManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionReplyProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.proto.RaftProtos.TransferLeadershipRequestProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyExceptionReplyProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerRequestProto;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.quic.codec.ShadedProtobufDecoder;
import org.apache.ratis.quic.codec.ShadedProtobufEncoder;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * QUIC-based server RPC endpoint for Apache Ratis.
 *
 * <h3>Transport</h3>
 * Binds a single UDP socket. The QUIC Incubator codec upgrades incoming UDP
 * datagrams to QUIC connections ({@link io.netty.incubator.codec.quic.QuicChannel}).
 * TLS 1.3 is mandatory (built into the QUIC handshake).
 *
 * <h3>P2P stream protocol</h3>
 * Each peer that connects opens persistent bidirectional streams: by default one per
 * message type, or a single one for everything when the peer runs with
 * {@link QuicConfigKeys.Server#SINGLE_STREAM_KEY} (the TCP-like baseline used in the
 * benchmarks). The first byte on every new stream is a one-byte tag that declares its role:
 * <ul>
 *   <li>{@link #TAG_APPEND_ENTRIES}   (0x00) – AppendEntries with log entries</li>
 *   <li>{@link #TAG_HEARTBEAT}        (0x01) – AppendEntries with no entries</li>
 *   <li>{@link #TAG_INSTALL_SNAPSHOT} (0x02) – InstallSnapshot</li>
 *   <li>{@link #TAG_REQUEST_VOTE}     (0x03) – RequestVote</li>
 *   <li>{@link #TAG_PEER_SINGLE}      (0x06) – all of the above on one stream</li>
 * </ul>
 * After reading the tag, a one-shot {@link StreamTypeDecoder} configures the rest
 * of the pipeline (varint32 framing + shaded Protobuf codec) and removes itself.
 *
 * <h3>Client-request streams</h3>
 * External Raft clients (e.g. CounterClient) use tag {@link #TAG_CLIENT_REQUEST}
 * (0x04). Each client RPC opens a new short-lived stream, sends one request, and
 * the server closes its write-side after sending the reply.
 */
public final class QuicRpcService
    extends RaftServerRpcWithProxy<QuicRpcProxy, QuicRpcProxy.PeerMap> {

  public static final Logger LOG = LoggerFactory.getLogger(QuicRpcService.class);
  static final String CLASS_NAME = JavaUtils.getClassSimpleName(QuicRpcService.class);

  // ---- Stream type tags (first byte on every new inbound stream) ----------

  /** AppendEntries with entriesCount > 0 (actual log replication). */
  public static final byte TAG_APPEND_ENTRIES   = 0x00;
  /** Heartbeat: AppendEntries with entriesCount == 0. Kept on a separate stream
   *  so that leader keep-alive packets never get stuck behind large AE batches. */
  public static final byte TAG_HEARTBEAT        = 0x01;
  /** InstallSnapshot (full snapshot transfer). */
  public static final byte TAG_INSTALL_SNAPSHOT = 0x02;
  /** RequestVote during leader election. */
  public static final byte TAG_REQUEST_VOTE     = 0x03;
  /** Single-request client RPC (stream closes after one reply). */
  public static final byte TAG_CLIENT_REQUEST   = 0x04;
  /** Server-to-server ReadIndex request for Linearizable Read. */
  public static final byte TAG_READ_INDEX       = 0x05;
  /** Single-stream layout ({@link QuicConfigKeys.Server#SINGLE_STREAM_KEY}): the one
   *  persistent stream of a server-to-server connection, carrying every message type
   *  (AppendEntries, heartbeat, InstallSnapshot, RequestVote, other) in one ordered
   *  sequence, the way a TCP connection does. Handled exactly like the per-type streams. */
  public static final byte TAG_PEER_SINGLE      = 0x06;

  // ---- Builder ------------------------------------------------------------

  public static final class Builder {
    private RaftServer server;
    private Builder() {}

    public Builder setServer(RaftServer raftServer) {
      this.server = raftServer;
      return this;
    }

    public QuicRpcService build() {
      return new QuicRpcService(server);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  // ---- Inbound handler (shared across all P2P streams) --------------------

  /**
   * Processes any {@link RaftNettyServerRequestProto} received on any P2P stream,
   * delegates to {@link #handle(RaftNettyServerRequestProto)}, and flushes the reply.
   * Stateless → safely shared.
   */
  @ChannelHandler.Sharable
  class InboundHandler
      extends SimpleChannelInboundHandler<RaftNettyServerRequestProto> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx,
        RaftNettyServerRequestProto proto) {
      ctx.writeAndFlush(handle(proto));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.warn("{}: exception on stream {}", getId(), ctx.channel(), cause);
      ctx.close();
    }
  }

  private final InboundHandler inboundHandler = new InboundHandler();

  /** Handles ReadIndex requests from peer servers (Linearizable Read protocol). */
  @ChannelHandler.Sharable
  class ReadIndexInboundHandler extends SimpleChannelInboundHandler<ReadIndexRequestProto> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ReadIndexRequestProto request) {
      final CompletableFuture<ReadIndexReplyProto> f;
      try {
        f = server.readIndexAsync(request);
      } catch (IOException e) {
        LOG.warn("{}: readIndex failed", getId(), e);
        ctx.close();
        return;
      }
      f.whenComplete((reply, ex) -> {
        if (ex != null) {
          LOG.warn("{}: readIndex failed", getId(), ex);
          ctx.close();
        } else {
          ctx.writeAndFlush(reply);
        }
      });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.warn("{}: exception on ReadIndex stream {}", getId(), ctx.channel(), cause);
      ctx.close();
    }
  }

  private final ReadIndexInboundHandler readIndexInboundHandler = new ReadIndexInboundHandler();

  // ---- One-shot stream tag decoder ----------------------------------------

  /**
   * First handler on every new inbound {@link QuicStreamChannel}.
   * Reads the single tag byte, builds the remaining pipeline (framing + codec +
   * request handler), then removes itself so that subsequent bytes flow directly
   * into the configured handlers.
   *
   * <p>Removing a {@link ByteToMessageDecoder} causes Netty to forward any
   * already-buffered bytes to the next handler automatically, so no bytes are lost.
   */
  class StreamTypeDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      if (!in.isReadable()) {
        return;
      }
      final byte tag = in.readByte();
      final ChannelPipeline p = ctx.pipeline();

      p.addLast(new ProtobufVarint32FrameDecoder());
      p.addLast(new ProtobufVarint32LengthFieldPrepender());
      p.addLast(ShadedProtobufEncoder.INSTANCE);

      if (tag == TAG_READ_INDEX) {
        p.addLast(new ShadedProtobufDecoder<>(ReadIndexRequestProto.getDefaultInstance()));
        p.addLast(readIndexInboundHandler);
      } else if (tag == TAG_CLIENT_REQUEST) {
        // Handled off the event loop; see clientRequestExecutor. Server-to-server streams
        // fall through to the branch below and keep running on the event loop as before.
        p.addLast(new ShadedProtobufDecoder<>(RaftNettyServerRequestProto.getDefaultInstance()));
        p.addLast(clientRequestExecutor, inboundHandler);
      } else {
        // AppendEntries / heartbeat / InstallSnapshot / RequestVote (or all of them on one
        // TAG_PEER_SINGLE stream) — see peerRequestExecutor.
        p.addLast(new ShadedProtobufDecoder<>(RaftNettyServerRequestProto.getDefaultInstance()));
        p.addLast(peerRequestExecutor, inboundHandler);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: new stream tag=0x{} channel={}", getId(),
            String.format("%02x", tag), ctx.channel());
      }

      // Remove self; Netty will push remaining bytes in the cumulation buffer
      // to the next handler (ProtobufVarint32FrameDecoder).
      p.remove(this);
    }
  }

  // ---- Fields -------------------------------------------------------------

  private final RaftServer server;
  private final EventLoopGroup group;
  private final InetSocketAddress socketAddress;
  private final MemoizedSupplier<ChannelFuture> channelFuture;
  /** Kodeki gniazd 2..N (puste gdy SOCKETS == 1). QuicheQuicCodec.isSharable() == false,
   *  wiec kazde gniazdo musi dostac wlasny egzemplarz. */
  private final List<ChannelHandler> extraCodecs = new ArrayList<>();
  /** Gniazda 2..N, zbindowane w startImpl(), zamykane w closeImpl(). */
  private final List<Channel> extraChannels = new ArrayList<>();

  // TYMCZASOWE (eksperyment 2026-08-27): -Dratis.quic.single.pool=true laczy pule
  // clientRequest i peerRequest w jedna, 2x rdzenie — tyle co workerGroup w Netty.
  // Domyslnie false = zachowanie dotychczasowe. DO USUNIECIA po pomiarach.
  private static final boolean SINGLE_POOL = Boolean.getBoolean("ratis.quic.single.pool");

  // TYMCZASOWE (eksperyment 2026-08-27): -Dratis.quic.sockets=N binduje N gniazd UDP na tym
  // samym porcie z SO_REUSEPORT, kazde z wlasnym kodekiem i wlasnym watkiem petli — inaczej
  // caly transport (syscalle, krypto quiche, kodeki) siedzi na jednym watku, bo jedno gniazdo
  // = jeden kanal = jeden event loop. Domyslnie 1 = zachowanie dotychczasowe.
  // DO USUNIECIA po pomiarach.
  private static final int SOCKETS = Math.max(1, Integer.getInteger("ratis.quic.sockets", 1));

  /**
   * Runs the blocking client-request handler off the event loop.
   *
   * <p>Every QUIC connection of this server is multiplexed over the one UDP socket, and Netty
   * binds a channel to a single event loop thread — so without this, all client requests, plus
   * the server-to-server streams queued behind them, would be served strictly one at a time,
   * each blocking for the length of a consensus round (replication + fsync). TCP needs no such
   * pool: one connection is one socket is one channel, which Netty already spreads across its
   * worker group. Only {@link #TAG_CLIENT_REQUEST} uses this; the AppendEntries, heartbeat,
   * InstallSnapshot, RequestVote and ReadIndex streams keep running on the event loop as before.
   */
  private final EventExecutorGroup clientRequestExecutor = new DefaultEventExecutorGroup(
      Runtime.getRuntime().availableProcessors() * 2,
      (java.util.concurrent.ThreadFactory) r -> {
        final Thread t = new Thread(r, SINGLE_POOL
            ? "QuicRpcService-request-" : "QuicRpcService-clientRequest-");
        t.setDaemon(true);
        return t;
      });

  /**
   * Runs the server-to-server handler off the event loop, on a pool of its own so a burst of
   * client requests cannot starve consensus traffic.
   *
   * <p>Appending entries blocks on the log write, and on the event loop that stalls every other
   * stream sharing the UDP socket — which is how the separate AppendEntries/heartbeat streams
   * lose the very head-of-line freedom they exist to provide. Netty pins each channel to one
   * executor for its lifetime, so a stream still sees its messages in order; only distinct
   * streams and distinct peers proceed in parallel. TCP reaches parallelism per connection and
   * can go no finer, since one connection carries one ordered byte stream.
   */
  private final EventExecutorGroup peerRequestExecutor = SINGLE_POOL
      ? clientRequestExecutor
      : new DefaultEventExecutorGroup(
          Runtime.getRuntime().availableProcessors(),
          (java.util.concurrent.ThreadFactory) r -> {
            final Thread t = new Thread(r, "QuicRpcService-peerRequest-");
            t.setDaemon(true);
            return t;
          });

  // ---- Constructor --------------------------------------------------------

  private QuicRpcService(RaftServer server) {
    super(server::getId,
        id -> new QuicRpcProxy.PeerMap(id.toString(), server.getProperties()));
    this.server = server;

    // Layout of the streams this server opens to its peers (QuicRpcProxy.connect), logged
    // once at startup so a benchmark run can be verified from the server log.
    LOG.info("{}: server-to-server stream layout: {}", getId(),
        QuicConfigKeys.Server.singleStream(server.getProperties())
            ? "single stream (" + QuicConfigKeys.Server.SINGLE_STREAM_KEY + "=true)"
            : "one stream per message type");

    final QuicSslContext sslCtx = buildServerSslContext(server);

    // Stream initializer: attach the one-shot tag decoder to every new stream.
    final ChannelInitializer<QuicStreamChannel> streamInit =
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel ch) {
            ch.pipeline().addLast(new StreamTypeDecoder());
          }
        };

    final ChannelHandler quicCodec = newQuicCodec(sslCtx, streamInit);
    for (int i = 1; i < SOCKETS; i++) {
      extraCodecs.add(newQuicCodec(sslCtx, streamInit));
    }

    this.group = new NioEventLoopGroup(0,
        (java.util.concurrent.ThreadFactory) r ->
            new Thread(r, CLASS_NAME + "-worker-" + server.getId()));

    final String host = QuicConfigKeys.Server.host(server.getProperties());
    final int port    = QuicConfigKeys.Server.port(server.getProperties());
    this.socketAddress = (host == null || host.isEmpty())
        ? new InetSocketAddress(port)
        : new InetSocketAddress(host, port);

    // Lazy bind — actual socket open happens in startImpl().
    this.channelFuture = JavaUtils.memoize(() -> newBootstrap(quicCodec).bind(socketAddress));
  }

  /** Kodek QUIC dla jednego gniazda. Wolane raz na gniazdo — patrz {@link #extraCodecs}. */
  private static ChannelHandler newQuicCodec(QuicSslContext sslCtx,
      ChannelInitializer<QuicStreamChannel> streamInit) {
    return new QuicServerCodecBuilder()
        .sslContext(sslCtx)
        .maxIdleTimeout(0, TimeUnit.MILLISECONDS)
        .initialMaxData(128 * 1024 * 1024)
        .initialMaxStreamDataBidirectionalLocal(16 * 1024 * 1024)
        .initialMaxStreamDataBidirectionalRemote(16 * 1024 * 1024)
        .initialMaxStreamsBidirectional(100)
        .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
        .streamHandler(streamInit)
        .build();
  }

  /** SO_REUSEPORT ustawiany tylko gdy gniazd jest wiecej niz jedno, zeby domyslna sciezka
   *  byla identyczna jak przed eksperymentem. */
  private Bootstrap newBootstrap(ChannelHandler codec) {
    final Bootstrap b = new Bootstrap()
        .group(group)
        .channel(NioDatagramChannel.class)
        .handler(codec);
    if (SOCKETS > 1) {
      b.option(NioChannelOption.of(reusePortOption()), true);
    }
    return b;
  }

  /** StandardSocketOptions.SO_REUSEPORT przez refleksje: projekt kompiluje sie z API Javy 8
   *  (javaVersion=8), a pole istnieje od Javy 9 — bez refleksji kompilator Eclipse wszywa
   *  blad "cannot be resolved" do klasy. W runtime (jdk21) pole zawsze jest. */
  @SuppressWarnings("unchecked")
  private static java.net.SocketOption<Boolean> reusePortOption() {
    try {
      return (java.net.SocketOption<Boolean>)
          StandardSocketOptions.class.getField("SO_REUSEPORT").get(null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "-Dratis.quic.sockets > 1 requires SO_REUSEPORT (Java 9+)", e);
    }
  }

  // ---- RaftServerRpc interface --------------------------------------------

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.QUIC;
  }

  @Override
  public void startImpl() throws IOException {
    try {
      channelFuture.get().syncUninterruptibly();
      if (!extraCodecs.isEmpty()) {
        // Port moze byc efemeryczny (PORT_DEFAULT == 0), wiec pozostale gniazda binduja sie
        // pod adres, ktory dostalo pierwsze — nie pod socketAddress.
        final InetSocketAddress bound =
            (InetSocketAddress) channelFuture.get().channel().localAddress();
        for (ChannelHandler codec : extraCodecs) {
          extraChannels.add(newBootstrap(codec).bind(bound).syncUninterruptibly().channel());
        }
        LOG.info("{}: {} UDP sockets bound on {} (SO_REUSEPORT)",
            getId(), extraChannels.size() + 1, bound);
      }
      LOG.info("{}: QUIC server started on {}", getId(), getInetSocketAddress());
    } catch (Exception e) {
      throw new IOException(getId() + ": Failed to start " + CLASS_NAME, e);
    }
  }

  @Override
  public void closeImpl() throws IOException {
    for (Channel ch : extraChannels) {
      ch.close().syncUninterruptibly();
    }
    extraChannels.clear();
    if (channelFuture.isInitialized()) {
      channelFuture.get().awaitUninterruptibly().channel()
          .close().syncUninterruptibly();
    }
    group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    try {
      group.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("{}: interrupted while shutting down event loop", getId(), e);
      Thread.currentThread().interrupt();
    }
    super.closeImpl();
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    if (!channelFuture.isInitialized()) {
      if (socketAddress.getPort() != QuicConfigKeys.Server.PORT_DEFAULT) {
        return socketAddress;
      }
      throw new IllegalStateException(getId() + ": server not yet started");
    }
    return (InetSocketAddress) channelFuture.get()
        .awaitUninterruptibly().channel().localAddress();
  }

  // ---- Outgoing server-to-server calls (via QuicRpcProxy) -----------------

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setRequestVoteRequest(request).build();
    return sendRequest(request.getServerRequest(), proto).getRequestVoteReply();
  }

  @Override
  public StartLeaderElectionReplyProto startLeaderElection(
      StartLeaderElectionRequestProto request) throws IOException {
    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setStartLeaderElectionRequest(request).build();
    return sendRequest(request.getServerRequest(), proto).getStartLeaderElectionReply();
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request)
      throws IOException {
    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setAppendEntriesRequest(request).build();
    return sendRequest(request.getServerRequest(), proto).getAppendEntriesReply();
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request)
      throws IOException {
    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setInstallSnapshotRequest(request).build();
    return sendRequest(request.getServerRequest(), proto).getInstallSnapshotReply();
  }

  private RaftNettyServerReplyProto sendRequest(RaftRpcRequestProto rpcRequest,
      RaftNettyServerRequestProto proto) throws IOException {
    final RaftPeerId peerId = RaftPeerId.valueOf(rpcRequest.getReplyId());
    try {
      return getProxies().getProxy(peerId).send(rpcRequest, proto);
    } catch (Exception e) {
      getProxies().handleException(peerId, e, false);
      throw e;
    }
  }

  // ---- Incoming request dispatcher (same logic as NettyRpcService) --------

  RaftNettyServerReplyProto handle(RaftNettyServerRequestProto proto) {
    RaftRpcRequestProto rpcRequest = null;
    try {
      switch (proto.getRaftNettyServerRequestCase()) {

        case REQUESTVOTEREQUEST: {
          final RequestVoteRequestProto req = proto.getRequestVoteRequest();
          rpcRequest = req.getServerRequest();
          final RequestVoteReplyProto reply = server.requestVote(req);
          return RaftNettyServerReplyProto.newBuilder()
              .setRequestVoteReply(reply).build();
        }

        case APPENDENTRIESREQUEST: {
          final AppendEntriesRequestProto req = proto.getAppendEntriesRequest();
          rpcRequest = req.getServerRequest();
          final AppendEntriesReplyProto reply = server.appendEntries(req);
          return RaftNettyServerReplyProto.newBuilder()
              .setAppendEntriesReply(reply).build();
        }

        case INSTALLSNAPSHOTREQUEST: {
          final InstallSnapshotRequestProto req = proto.getInstallSnapshotRequest();
          rpcRequest = req.getServerRequest();
          final InstallSnapshotReplyProto reply = server.installSnapshot(req);
          return RaftNettyServerReplyProto.newBuilder()
              .setInstallSnapshotReply(reply).build();
        }

        case STARTLEADERELECTIONREQUEST: {
          final StartLeaderElectionRequestProto req =
              proto.getStartLeaderElectionRequest();
          rpcRequest = req.getServerRequest();
          return RaftNettyServerReplyProto.newBuilder()
              .setStartLeaderElectionReply(server.startLeaderElection(req)).build();
        }

        case TRANSFERLEADERSHIPREQUEST: {
          final TransferLeadershipRequestProto req =
              proto.getTransferLeadershipRequest();
          rpcRequest = req.getRpcRequest();
          final RaftClientReply reply = server.transferLeadership(
              ClientProtoUtils.toTransferLeadershipRequest(req));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(reply))
              .build();
        }

        case SNAPSHOTMANAGEMENTREQUEST: {
          final SnapshotManagementRequestProto req =
              proto.getSnapshotManagementRequest();
          rpcRequest = req.getRpcRequest();
          final RaftClientReply reply = server.snapshotManagement(
              ClientProtoUtils.toSnapshotManagementRequest(req));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(reply))
              .build();
        }

        case LEADERELECTIONMANAGEMENTREQUEST: {
          final LeaderElectionManagementRequestProto req =
              proto.getLeaderElectionManagementRequest();
          rpcRequest = req.getRpcRequest();
          final RaftClientReply reply = server.leaderElectionManagement(
              ClientProtoUtils.toLeaderElectionManagementRequest(req));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(reply))
              .build();
        }

        case RAFTCLIENTREQUEST: {
          final RaftClientRequestProto req = proto.getRaftClientRequest();
          rpcRequest = req.getRpcRequest();
          final RaftClientReply reply = server.submitClientRequest(
              ClientProtoUtils.toRaftClientRequest(req));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(reply))
              .build();
        }

        case SETCONFIGURATIONREQUEST: {
          final SetConfigurationRequestProto req =
              proto.getSetConfigurationRequest();
          rpcRequest = req.getRpcRequest();
          final RaftClientReply reply = server.setConfiguration(
              ClientProtoUtils.toSetConfigurationRequest(req));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(reply))
              .build();
        }

        case GROUPMANAGEMENTREQUEST: {
          final GroupManagementRequestProto req =
              proto.getGroupManagementRequest();
          rpcRequest = req.getRpcRequest();
          final RaftClientReply reply = server.groupManagement(
              ClientProtoUtils.toGroupManagementRequest(req));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(reply))
              .build();
        }

        case GROUPLISTREQUEST: {
          final GroupListRequestProto req = proto.getGroupListRequest();
          rpcRequest = req.getRpcRequest();
          final GroupListReply reply = server.getGroupList(
              ClientProtoUtils.toGroupListRequest(req));
          return RaftNettyServerReplyProto.newBuilder()
              .setGroupListReply(ClientProtoUtils.toGroupListReplyProto(reply))
              .build();
        }

        case GROUPINFOREQUEST: {
          final GroupInfoRequestProto req = proto.getGroupInfoRequest();
          rpcRequest = req.getRpcRequest();
          final GroupInfoReply reply = server.getGroupInfo(
              ClientProtoUtils.toGroupInfoRequest(req));
          return RaftNettyServerReplyProto.newBuilder()
              .setGroupInfoReply(ClientProtoUtils.toGroupInfoReplyProto(reply))
              .build();
        }

        case RAFTNETTYSERVERREQUEST_NOT_SET:
          throw new IllegalArgumentException(
              "Request case not set: " + proto.getRaftNettyServerRequestCase());

        default:
          throw new UnsupportedOperationException(
              "Request case not supported: " + proto.getRaftNettyServerRequestCase());
      }
    } catch (IOException ioe) {
      return toExceptionReply(
          Objects.requireNonNull(rpcRequest, "rpcRequest is null"), ioe);
    }
  }

  // ---- RaftServerAsynchronousProtocol ------------------------------------

  private final RaftServerAsynchronousProtocol asyncProtocol =
      new RaftServerAsynchronousProtocol() {
        @Override
        public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(
            AppendEntriesRequestProto request) {
          throw new UnsupportedOperationException(
              CLASS_NAME + " does not support async appendEntries");
        }

        @Override
        public CompletableFuture<ReadIndexReplyProto> readIndexAsync(
            ReadIndexRequestProto request) throws IOException {
          final RaftPeerId target =
              RaftPeerId.valueOf(request.getServerRequest().getReplyId());
          return getProxies().getProxy(target).readIndexAsync(request);
        }
      };

  @Override
  public RaftServerAsynchronousProtocol async() {
    return asyncProtocol;
  }

  private static RaftNettyServerReplyProto toExceptionReply(
      RaftRpcRequestProto request, IOException e) {
    final RaftRpcReplyProto.Builder rpcReply = RaftRpcReplyProto.newBuilder()
        .setRequestorId(request.getRequestorId())
        .setReplyId(request.getReplyId())
        .setCallId(request.getCallId())
        .setSuccess(false);
    final RaftNettyExceptionReplyProto.Builder ioe =
        RaftNettyExceptionReplyProto.newBuilder()
            .setRpcReply(rpcReply)
            .setException(ProtoUtils.writeObject2ByteString(e));
    return RaftNettyServerReplyProto.newBuilder().setExceptionReply(ioe).build();
  }

  // ---- TLS context helper -------------------------------------------------

  private static QuicSslContext buildServerSslContext(RaftServer server) {
    final String certPath = QuicConfigKeys.Server.tlsCert(server.getProperties());
    final String keyPath  = QuicConfigKeys.Server.tlsKey(server.getProperties());
    try {
      if (certPath != null && keyPath != null) {
        LOG.info("{}: building QUIC server TLS context from cert={} key={}",
            server.getId(), certPath, keyPath);
        return QuicSslContextBuilder
            .forServer(new File(keyPath), null, new File(certPath))
            .applicationProtocols(QuicConfigKeys.ALPN)
            .build();
      }
      // No certs configured → generate a temporary self-signed certificate.
      // Useful for local development and integration tests.
      LOG.warn("{}: no TLS cert/key configured; using SelfSignedCertificate "
          + "(NOT suitable for production)", server.getId());
      final SelfSignedCertificate ssc = new SelfSignedCertificate();
      return QuicSslContextBuilder
          .forServer(ssc.privateKey(), null, ssc.certificate())
          .applicationProtocols(QuicConfigKeys.ALPN)
          .build();
    } catch (CertificateException e) {
      throw new IllegalStateException(
          server.getId() + ": failed to build QUIC server SSL context", e);
    }
  }
}
