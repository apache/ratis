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
package org.apache.raft.netty.server;

import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.raft.client.ClientProtoUtils;
import org.apache.raft.netty.NettyRpcProxy;
import org.apache.raft.netty.proto.NettyProtos.RaftNettyServerReplyProto;
import org.apache.raft.netty.proto.NettyProtos.RaftNettyServerRequestProto;
import org.apache.raft.proto.RaftProtos.*;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerRpc;
import org.apache.raft.server.RaftServerRpcService;
import org.apache.raft.server.RequestDispatcher;
import org.apache.raft.util.CodeInjectionForTesting;
import org.apache.raft.util.LifeCycle;
import org.apache.raft.util.PeerProxyMap;
import org.apache.raft.util.RaftUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A netty server endpoint that acts as the communication layer.
 */
public final class NettyRpcService implements RaftServerRpc {
  static final String CLASS_NAME = NettyRpcService.class.getSimpleName();
  public static final String SEND_SERVER_REQUEST = CLASS_NAME + ".sendServerRequest";

  private final LifeCycle lifeCycle = new LifeCycle(getClass().getSimpleName());
  private final RaftServerRpcService raftService;
  private final String id;

  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final ChannelFuture channelFuture;

  private final PeerProxyMap<NettyRpcProxy> proxies
      = new PeerProxyMap<NettyRpcProxy>() {
    @Override
    public NettyRpcProxy createProxy(RaftPeer peer)
        throws IOException {
      final NettyRpcProxy proxy = new NettyRpcProxy(peer);
      try {
        proxy.connect();
      } catch (InterruptedException e) {
        throw RaftUtils.toInterruptedIOException("Failed connecting to " + peer, e);
      }
      return proxy;
    }
  };

  @ChannelHandler.Sharable
  class InboundHandler extends SimpleChannelInboundHandler<RaftNettyServerRequestProto> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RaftNettyServerRequestProto proto)
        throws IOException {
      final RaftNettyServerReplyProto reply = handle(proto);
      ctx.writeAndFlush(reply);
    }
  }

  /** Constructs a netty server with the given port. */
  public NettyRpcService(int port, RaftServer server) {
    this.raftService = new RaftServerRpcService(new RequestDispatcher(server));
    this.id = server.getId();

    final ChannelInitializer<SocketChannel> initializer
        = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        final ChannelPipeline p = ch.pipeline();

        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(RaftNettyServerRequestProto.getDefaultInstance()));
        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());

        p.addLast(new InboundHandler());
      }
    };

    channelFuture = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(initializer)
        .bind(port)
        .syncUninterruptibly();
  }

  @Override
  public void start() {
    lifeCycle.transition(LifeCycle.State.STARTING);
    channelFuture.awaitUninterruptibly();
    lifeCycle.transition(LifeCycle.State.RUNNING);
  }

  @Override
  public void shutdown() {
    for(;;) {
      final LifeCycle.State current = lifeCycle.getCurrentState();
      if (current == LifeCycle.State.CLOSING
          || current == LifeCycle.State.CLOSED) {
        return; //already closing or closed.
      }
      if (lifeCycle.compareAndTransition(current, LifeCycle.State.CLOSING)) {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        channelFuture.channel().close().awaitUninterruptibly();
        lifeCycle.transition(LifeCycle.State.CLOSED);
        proxies.close();
        return;
      }

      // lifecycle state is changed, retry.
    }
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return (InetSocketAddress) channelFuture.channel().localAddress();
  }

  RaftNettyServerReplyProto handle(RaftNettyServerRequestProto proto)
      throws IOException {
    switch (proto.getRaftNettyServerRequestCase()) {
      case REQUESTVOTEREQUEST: {
        final RequestVoteReplyProto reply = raftService.requestVote(
            proto.getRequestVoteRequest());
        return RaftNettyServerReplyProto.newBuilder()
            .setRequestVoteReply(reply)
            .build();
      }
      case APPENDENTRIESREQUEST: {
        final AppendEntriesReplyProto reply = raftService.appendEntries(
            proto.getAppendEntriesRequest());
        return RaftNettyServerReplyProto.newBuilder()
            .setAppendEntriesReply(reply)
            .build();
      }
      case INSTALLSNAPSHOTREQUEST: {
        final InstallSnapshotReplyProto reply = raftService.installSnapshot(
            proto.getInstallSnapshotRequest());
        return RaftNettyServerReplyProto.newBuilder()
            .setInstallSnapshotReply(reply)
            .build();
      }
      case RAFTCLIENTREQUEST: {
        final RaftClientRequest request = ClientProtoUtils.toRaftClientRequest(
            proto.getRaftClientRequest());
        final RaftClientReply reply = raftService.submitClientRequest(request);
        return RaftNettyServerReplyProto.newBuilder()
            .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(reply))
            .build();
      }
      case SETCONFIGURATIONREQUEST: {
        final SetConfigurationRequest request = ClientProtoUtils.toSetConfigurationRequest(
            proto.getSetConfigurationRequest());
        final RaftClientReply reply = raftService.setConfiguration(request);
        return RaftNettyServerReplyProto.newBuilder()
            .setRaftClientReply(ClientProtoUtils.toRaftClientReplyProto(reply))
            .build();
      }
      case RAFTNETTYSERVERREQUEST_NOT_SET:
        throw new IllegalArgumentException("Request case not set in proto: "
            + proto.getRaftNettyServerRequestCase());
      default:
        throw new UnsupportedOperationException("Request case not supported: "
            + proto.getRaftNettyServerRequestCase());
    }
  }

  @Override
  public RequestVoteReplyProto sendRequestVote(RequestVoteRequestProto request) throws IOException {
    Preconditions.checkArgument(id.equals(request.getServerRequest().getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, id, null, request);

    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setRequestVoteRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftNettyServerRequestProto(serverRequest, proto).getRequestVoteReply();
  }

  @Override
  public AppendEntriesReplyProto sendAppendEntries(AppendEntriesRequestProto request) throws IOException {
    Preconditions.checkArgument(id.equals(request.getServerRequest().getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, id, null, request);

    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setAppendEntriesRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftNettyServerRequestProto(serverRequest, proto).getAppendEntriesReply();
  }

  @Override
  public InstallSnapshotReplyProto sendInstallSnapshot(InstallSnapshotRequestProto request) throws IOException {
    Preconditions.checkArgument(id.equals(request.getServerRequest().getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, id, null, request);

    final RaftNettyServerRequestProto proto = RaftNettyServerRequestProto.newBuilder()
        .setInstallSnapshotRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftNettyServerRequestProto(serverRequest, proto).getInstallSnapshotReply();
  }

  private RaftNettyServerReplyProto sendRaftNettyServerRequestProto(
      RaftRpcRequestProto request, RaftNettyServerRequestProto proto)
      throws IOException {
    final NettyRpcProxy p = proxies.getProxy(request.getReplyId());
    return p.send(request, proto);
  }

  @Override
  public void addPeers(Iterable<RaftPeer> peers) {
    proxies.addPeers(peers);
  }
}
