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
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
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
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.Objects;

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

  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final MemoizedSupplier<ChannelFuture> channel;
  private final InetSocketAddress socketAddress;

  @ChannelHandler.Sharable
  class InboundHandler extends SimpleChannelInboundHandler<RaftNettyServerRequestProto> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RaftNettyServerRequestProto proto) {
      final RaftNettyServerReplyProto reply = handle(proto);
      ctx.writeAndFlush(reply);
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

    final String host = NettyConfigKeys.Server.host(server.getProperties());
    final int port = NettyConfigKeys.Server.port(server.getProperties());
    socketAddress =
            host == null || host.isEmpty() ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
    this.channel = JavaUtils.memoize(() -> new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
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

  RaftNettyServerReplyProto handle(RaftNettyServerRequestProto proto) {
    RaftRpcRequestProto rpcRequest = null;
    try {
      switch (proto.getRaftNettyServerRequestCase()) {
        case REQUESTVOTEREQUEST:
          final RequestVoteRequestProto request = proto.getRequestVoteRequest();
          rpcRequest = request.getServerRequest();
          final RequestVoteReplyProto reply = server.requestVote(request);
          return RaftNettyServerReplyProto.newBuilder()
              .setRequestVoteReply(reply)
              .build();

        case TRANSFERLEADERSHIPREQUEST:
          final TransferLeadershipRequestProto transferLeadershipRequest = proto.getTransferLeadershipRequest();
          rpcRequest = transferLeadershipRequest.getRpcRequest();
          final RaftClientReply transferLeadershipReply = server.transferLeadership(
              ClientProtoUtils.toTransferLeadershipRequest(transferLeadershipRequest));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(transferLeadershipReply))
              .build();

        case STARTLEADERELECTIONREQUEST:
          final StartLeaderElectionRequestProto startLeaderElectionRequest = proto.getStartLeaderElectionRequest();
          rpcRequest = startLeaderElectionRequest.getServerRequest();
          final StartLeaderElectionReplyProto startLeaderElectionReply =
              server.startLeaderElection(startLeaderElectionRequest);
          return RaftNettyServerReplyProto.newBuilder().setStartLeaderElectionReply(startLeaderElectionReply).build();

        case SNAPSHOTMANAGEMENTREQUEST:
          final SnapshotManagementRequestProto snapshotManagementRequest = proto.getSnapshotManagementRequest();
          rpcRequest = snapshotManagementRequest.getRpcRequest();
          final RaftClientReply snapshotManagementReply = server.snapshotManagement(
              ClientProtoUtils.toSnapshotManagementRequest(snapshotManagementRequest));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(snapshotManagementReply))
              .build();

        case LEADERELECTIONMANAGEMENTREQUEST:
          final LeaderElectionManagementRequestProto leaderElectionManagementRequest =
              proto.getLeaderElectionManagementRequest();
          rpcRequest = leaderElectionManagementRequest.getRpcRequest();
          final RaftClientReply leaderElectionManagementReply = server.leaderElectionManagement(
              ClientProtoUtils.toLeaderElectionManagementRequest(leaderElectionManagementRequest));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(leaderElectionManagementReply))
              .build();

        case APPENDENTRIESREQUEST:
          final AppendEntriesRequestProto appendEntriesRequest = proto.getAppendEntriesRequest();
          rpcRequest = appendEntriesRequest.getServerRequest();
          final AppendEntriesReplyProto appendEntriesReply = server.appendEntries(appendEntriesRequest);
          return RaftNettyServerReplyProto.newBuilder()
              .setAppendEntriesReply(appendEntriesReply)
              .build();

        case INSTALLSNAPSHOTREQUEST:
          final InstallSnapshotRequestProto installSnapshotRequest = proto.getInstallSnapshotRequest();
          rpcRequest = installSnapshotRequest.getServerRequest();
          final InstallSnapshotReplyProto installSnapshotReply = server.installSnapshot(installSnapshotRequest);
          return RaftNettyServerReplyProto.newBuilder()
              .setInstallSnapshotReply(installSnapshotReply)
              .build();

        case RAFTCLIENTREQUEST:
          final RaftClientRequestProto raftClientRequest = proto.getRaftClientRequest();
          rpcRequest = raftClientRequest.getRpcRequest();
          final RaftClientReply raftClientReply = server.submitClientRequest(
              ClientProtoUtils.toRaftClientRequest(raftClientRequest));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(raftClientReply))
              .build();

        case SETCONFIGURATIONREQUEST:
          final SetConfigurationRequestProto configurationRequest = proto.getSetConfigurationRequest();
          rpcRequest = configurationRequest.getRpcRequest();
          final RaftClientReply configurationReply = server.setConfiguration(
              ClientProtoUtils.toSetConfigurationRequest(configurationRequest));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(configurationReply))
              .build();

        case GROUPMANAGEMENTREQUEST:
          final GroupManagementRequestProto groupManagementRequest = proto.getGroupManagementRequest();
          rpcRequest = groupManagementRequest.getRpcRequest();
          final RaftClientReply groupManagementReply = server.groupManagement(
              ClientProtoUtils.toGroupManagementRequest(groupManagementRequest));
          return RaftNettyServerReplyProto.newBuilder()
              .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(groupManagementReply))
              .build();

        case GROUPLISTREQUEST:
          final GroupListRequestProto groupListRequest = proto.getGroupListRequest();
          rpcRequest = groupListRequest.getRpcRequest();
          final GroupListReply groupListReply = server.getGroupList(
              ClientProtoUtils.toGroupListRequest(groupListRequest));
          return RaftNettyServerReplyProto.newBuilder()
              .setGroupListReply(ClientProtoUtils.toGroupListReplyProto(groupListReply))
              .build();

        case GROUPINFOREQUEST:
          final GroupInfoRequestProto groupInfoRequest = proto.getGroupInfoRequest();
          rpcRequest = groupInfoRequest.getRpcRequest();
          final GroupInfoReply groupInfoReply = server.getGroupInfo(
              ClientProtoUtils.toGroupInfoRequest(groupInfoRequest));
          return RaftNettyServerReplyProto.newBuilder()
              .setGroupInfoReply(ClientProtoUtils.toGroupInfoReplyProto(groupInfoReply))
              .build();

        case RAFTNETTYSERVERREQUEST_NOT_SET:
          throw new IllegalArgumentException("Request case not set in proto: "
              + proto.getRaftNettyServerRequestCase());
        default:
          throw new UnsupportedOperationException("Request case not supported: "
              + proto.getRaftNettyServerRequestCase());
      }
    } catch (IOException ioe) {
      return toRaftNettyServerReplyProto(
          Objects.requireNonNull(rpcRequest, "rpcRequest = null"), ioe);
    }
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
