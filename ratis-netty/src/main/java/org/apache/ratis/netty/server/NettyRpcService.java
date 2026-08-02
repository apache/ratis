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

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.NettyRpcProxy;
import org.apache.ratis.util.NettyUtils;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyExceptionReplyProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerRequestProto;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A netty server endpoint that acts as the communication layer.
 */
public final class NettyRpcService extends RaftServerRpcWithProxy<NettyRpcProxy, NettyRpcProxy.PeerMap> {
  public static final Logger LOG = LoggerFactory.getLogger(NettyRpcService.class);
  static final String CLASS_NAME = JavaUtils.getClassSimpleName(NettyRpcService.class);
  public static final String SEND_SERVER_REQUEST = CLASS_NAME + ".sendServerRequest";

  public static final class Builder {
    private RaftServer server;

    private Builder() {}

    public Builder setServer(RaftServer raftServer) {
      this.server = raftServer;
      return this;
    }

    public NettyRpcService build() {
      return new NettyRpcService(server);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final RaftServer server;

  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final MemoizedSupplier<ChannelFuture> channel;
  private final InetSocketAddress socketAddress;

  private final ExecutorService requestExecutor;

  class InboundHandler extends SimpleChannelInboundHandler<RaftNettyServerRequestProto> {
    /**
     * Tail of this channel's chain of request-handling tasks.
     * Requests on a channel must be handled in arrival order.
     */
    private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RaftNettyServerRequestProto proto) {
      tail = tail.handleAsync((prev, prevError) -> {
        final CompletableFuture<RaftNettyServerReplyProto> replyFuture;
        try {
          replyFuture = handleAsync(proto);
        } catch (Exception e) {
          // No request context to build a reply; fail fast by closing the channel.
          LOG.warn("{}: Failed to handle request; closing the channel.", getId(), e);
          ctx.close();
          return null;
        }
        replyFuture.whenComplete((reply, e) -> {
          if (e != null) {
            LOG.warn("{}: Failed to handle request; closing the channel.", getId(), e);
            ctx.close();
          } else {
            ctx.writeAndFlush(reply);
          }
        });
        return null;
      }, requestExecutor);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.warn("{}: exceptionCaught on channel {}; closing it.", getId(), ctx.channel(), cause);
      ctx.close();
    }
  }

  /** Constructs a netty server with the given port. */
  private NettyRpcService(RaftServer server) {
    super(server::getId, id -> new NettyRpcProxy.PeerMap(id.toString(), server.getProperties()));
    this.server = server;

    final ChannelInitializer<SocketChannel> initializer
        = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) {
        final ChannelPipeline p = ch.pipeline();

        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(RaftNettyServerRequestProto.getDefaultInstance()));
        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());

        p.addLast(new InboundHandler());
      }
    };

    this.requestExecutor = ConcurrentUtils.newThreadPoolWithMax(
        NettyConfigKeys.Server.asyncRequestThreadPoolCached(server.getProperties()),
        NettyConfigKeys.Server.asyncRequestThreadPoolSize(server.getProperties()),
        server.getId() + "-request-");

    final boolean useEpoll = NettyConfigKeys.Server.useEpoll(server.getProperties());
    this.bossGroup = NettyUtils.newEventLoopGroup(CLASS_NAME + "-bossGroup", 0, useEpoll);
    this.workerGroup = NettyUtils.newEventLoopGroup(CLASS_NAME + "-workerGroup",0, useEpoll);

    final String host = NettyConfigKeys.Server.host(server.getProperties());
    final int port = NettyConfigKeys.Server.port(server.getProperties());
    socketAddress =
            host == null || host.isEmpty() ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
    this.channel = JavaUtils.memoize(() -> new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NettyUtils.getServerChannelClass(bossGroup))
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(initializer)
        .bind(socketAddress));
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.NETTY;
  }

  private Channel getChannel() {
    if (!channel.isInitialized()) {
      throw new IllegalStateException(getId() + ": Failed to getChannel since the service is not yet started");
    }
    return channel.get().awaitUninterruptibly().channel();
  }

  @Override
  public void startImpl() throws IOException {
    try {
      channel.get().syncUninterruptibly();
    } catch(Exception t) {
      throw new IOException(getId() + ": Failed to start " + JavaUtils.getClassSimpleName(getClass()), t);
    }
  }

  @Override
  public void closeImpl() throws IOException {
    ConcurrentUtils.shutdownAndWait(requestExecutor);
    final ChannelFuture f = getChannel().close();
    f.syncUninterruptibly();
    bossGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    workerGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    try {
      bossGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
      workerGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.error("Interrupt EventLoopGroup terminate", e);
      Thread.currentThread().interrupt();
    }
    super.closeImpl();
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    try {
      return (InetSocketAddress) getChannel().localAddress();
    } catch (IllegalStateException e) {
      if (socketAddress.getPort() != NettyConfigKeys.Server.PORT_DEFAULT) {
        return socketAddress;
      }
      throw e;
    }
  }

  CompletableFuture<RaftNettyServerReplyProto> handleAsync(RaftNettyServerRequestProto proto) {
    RaftRpcRequestProto rpcRequest = null;
    try {
      final CompletableFuture<RaftNettyServerReplyProto> replyFuture;
      switch (proto.getRaftNettyServerRequestCase()) {
        case REQUESTVOTEREQUEST: {
          // requestVote has no async variant; it is fast and does not block on commit.
          final RequestVoteRequestProto request = proto.getRequestVoteRequest();
          rpcRequest = request.getServerRequest();
          replyFuture = CompletableFuture.completedFuture(RaftNettyServerReplyProto.newBuilder()
              .setRequestVoteReply(server.requestVote(request))
              .build());
          break;
        }
        case TRANSFERLEADERSHIPREQUEST: {
          final TransferLeadershipRequestProto request = proto.getTransferLeadershipRequest();
          rpcRequest = request.getRpcRequest();
          replyFuture = server.transferLeadershipAsync(ClientProtoUtils.toTransferLeadershipRequest(request))
              .thenApply(NettyRpcService::toRaftClientReply);
          break;
        }
        case STARTLEADERELECTIONREQUEST: {
          // startLeaderElection has no async variant; it is fast and does not block on commit.
          final StartLeaderElectionRequestProto request = proto.getStartLeaderElectionRequest();
          rpcRequest = request.getServerRequest();
          replyFuture = CompletableFuture.completedFuture(RaftNettyServerReplyProto.newBuilder()
              .setStartLeaderElectionReply(server.startLeaderElection(request))
              .build());
          break;
        }
        case SNAPSHOTMANAGEMENTREQUEST: {
          final SnapshotManagementRequestProto request = proto.getSnapshotManagementRequest();
          rpcRequest = request.getRpcRequest();
          replyFuture = server.snapshotManagementAsync(ClientProtoUtils.toSnapshotManagementRequest(request))
              .thenApply(NettyRpcService::toRaftClientReply);
          break;
        }
        case LEADERELECTIONMANAGEMENTREQUEST: {
          final LeaderElectionManagementRequestProto request = proto.getLeaderElectionManagementRequest();
          rpcRequest = request.getRpcRequest();
          replyFuture = server.leaderElectionManagementAsync(
                  ClientProtoUtils.toLeaderElectionManagementRequest(request))
              .thenApply(NettyRpcService::toRaftClientReply);
          break;
        }
        case APPENDENTRIESREQUEST: {
          final AppendEntriesRequestProto request = proto.getAppendEntriesRequest();
          rpcRequest = request.getServerRequest();
          replyFuture = server.appendEntriesAsync(request)
              .thenApply(reply -> RaftNettyServerReplyProto.newBuilder()
                  .setAppendEntriesReply(reply)
                  .build());
          break;
        }
        case INSTALLSNAPSHOTREQUEST: {
          // installSnapshot has no async variant; it runs on this per-channel serialized path.
          final InstallSnapshotRequestProto request = proto.getInstallSnapshotRequest();
          rpcRequest = request.getServerRequest();
          replyFuture = CompletableFuture.completedFuture(RaftNettyServerReplyProto.newBuilder()
              .setInstallSnapshotReply(server.installSnapshot(request))
              .build());
          break;
        }
        case RAFTCLIENTREQUEST: {
          final RaftClientRequestProto request = proto.getRaftClientRequest();
          rpcRequest = request.getRpcRequest();
          replyFuture = server.submitClientRequestAsync(ClientProtoUtils.toRaftClientRequest(request))
              .thenApply(NettyRpcService::toRaftClientReply);
          break;
        }
        case SETCONFIGURATIONREQUEST: {
          final SetConfigurationRequestProto request = proto.getSetConfigurationRequest();
          rpcRequest = request.getRpcRequest();
          replyFuture = server.setConfigurationAsync(ClientProtoUtils.toSetConfigurationRequest(request))
              .thenApply(NettyRpcService::toRaftClientReply);
          break;
        }
        case GROUPMANAGEMENTREQUEST: {
          final GroupManagementRequestProto request = proto.getGroupManagementRequest();
          rpcRequest = request.getRpcRequest();
          replyFuture = server.groupManagementAsync(ClientProtoUtils.toGroupManagementRequest(request))
              .thenApply(NettyRpcService::toRaftClientReply);
          break;
        }
        case GROUPLISTREQUEST: {
          final GroupListRequestProto request = proto.getGroupListRequest();
          rpcRequest = request.getRpcRequest();
          replyFuture = server.getGroupListAsync(ClientProtoUtils.toGroupListRequest(request))
              .thenApply(reply -> RaftNettyServerReplyProto.newBuilder()
                  .setGroupListReply(ClientProtoUtils.toGroupListReplyProto(reply))
                  .build());
          break;
        }
        case GROUPINFOREQUEST: {
          final GroupInfoRequestProto request = proto.getGroupInfoRequest();
          rpcRequest = request.getRpcRequest();
          replyFuture = server.getGroupInfoAsync(ClientProtoUtils.toGroupInfoRequest(request))
              .thenApply(reply -> RaftNettyServerReplyProto.newBuilder()
                  .setGroupInfoReply(ClientProtoUtils.toGroupInfoReplyProto(reply))
                  .build());
          break;
        }
        case RAFTNETTYSERVERREQUEST_NOT_SET:
          throw new IllegalArgumentException("Request case not set in proto: "
              + proto.getRaftNettyServerRequestCase());
        default:
          throw new UnsupportedOperationException("Request case not supported: "
              + proto.getRaftNettyServerRequestCase());
      }

      final RaftRpcRequestProto request = rpcRequest;
      // Convert an asynchronous failure into an error reply (the client casts it to IOException).
      return replyFuture.exceptionally(e -> toRaftNettyServerReplyProto(request, toIOException(e)));
    } catch (Exception e) {
      // A synchronous failure before the reply future was created.
      if (rpcRequest == null) {
        // No request context to build a targeted reply; let InboundHandler close the channel.
        throw new IllegalStateException(getId() + ": Failed to handle request " + proto, e);
      }
      return CompletableFuture.completedFuture(toRaftNettyServerReplyProto(rpcRequest, toIOException(e)));
    }
  }

  private static RaftNettyServerReplyProto toRaftClientReply(RaftClientReply reply) {
    return RaftNettyServerReplyProto.newBuilder()
        .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(reply))
        .build();
  }

  private static IOException toIOException(Throwable t) {
    final Throwable cause = t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    return cause instanceof IOException ? (IOException) cause : new IOException(cause);
  }

  private static RaftNettyServerReplyProto toRaftNettyServerReplyProto(
      RaftRpcRequestProto request, IOException e) {
    final RaftRpcReplyProto.Builder rpcReply = RaftRpcReplyProto.newBuilder()
        .setRequestorId(request.getRequestorId())
        .setReplyId(request.getReplyId())
        .setCallId(request.getCallId())
        .setSuccess(false);
    final RaftNettyExceptionReplyProto.Builder ioe = RaftNettyExceptionReplyProto.newBuilder()
        .setRpcReply(rpcReply)
        .setException(ProtoUtils.writeObject2ByteString(e));
    return RaftNettyServerReplyProto.newBuilder().setExceptionReply(ioe).build();
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) throws IOException {
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, getId(), null, request);

    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setRequestVoteRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftNettyServerRequestProto(serverRequest, proto).getRequestVoteReply();
  }


  @Override
  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, getId(), null, request);

    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setStartLeaderElectionRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftNettyServerRequestProto(serverRequest, proto).getStartLeaderElectionReply();
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) throws IOException {
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, getId(), null, request);

    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setAppendEntriesRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftNettyServerRequestProto(serverRequest, proto).getAppendEntriesReply();
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, getId(), null, request);

    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setInstallSnapshotRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftNettyServerRequestProto(serverRequest, proto).getInstallSnapshotReply();
  }

  private RaftNettyServerReplyProto sendRaftNettyServerRequestProto(
      RaftRpcRequestProto request, RaftNettyServerRequestProto proto)
      throws IOException {
    final RaftPeerId id = RaftPeerId.valueOf(request.getReplyId());
    try {
      final NettyRpcProxy p = getProxies().getProxy(id);
      return p.send(request, proto);
    } catch (Exception e) {
      getProxies().handleException(id, e, false);
      throw e;
    }
  }
}
