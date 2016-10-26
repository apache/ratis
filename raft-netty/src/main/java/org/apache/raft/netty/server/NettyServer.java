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
import org.apache.raft.netty.proto.NettyProtos.RaftServerReplyProto;
import org.apache.raft.netty.proto.NettyProtos.RaftServerRequestProto;
import org.apache.raft.proto.RaftProtos.*;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerRpc;
import org.apache.raft.server.RaftServerRpcService;
import org.apache.raft.server.RequestDispatcher;
import org.apache.raft.util.LifeCycle;
import org.apache.raft.util.PeerProxyMap;
import org.apache.raft.util.RaftUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A netty server endpoint that acts as the communication layer.
 */
public final class NettyServer implements RaftServerRpc {
  private final int port;
  private final LifeCycle lifeCycle = new LifeCycle(getClass().getSimpleName());
  private final RaftServer server;
  private final RaftServerRpcService raftService;

  private final EventLoopGroup bossGroup = new NioEventLoopGroup();
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private Channel channel;

  private final PeerProxyMap<RaftServerProtocolProxy> proxies
      = new PeerProxyMap<RaftServerProtocolProxy>() {
    @Override
    public RaftServerProtocolProxy createProxy(RaftPeer peer)
        throws IOException {
      final RaftServerProtocolProxy proxy = new RaftServerProtocolProxy(peer);
      try {
        proxy.connect();
      } catch (InterruptedException e) {
        throw RaftUtils.toInterruptedIOException("Failed connecting to " + peer, e);
      }
      return proxy;
    }
  };


  /** Constructs a netty server with the given port. */
  public NettyServer(int port, RaftServer server) {
    this.port = port;
    this.server = server;
    this.raftService = new RaftServerRpcService(new RequestDispatcher(server));
  }

  @Override
  public void start() {
    lifeCycle.transition(LifeCycle.State.STARTING);
    final SimpleChannelInboundHandler<RaftServerRequestProto> inboundHandler
        = new SimpleChannelInboundHandler<RaftServerRequestProto>() {
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, RaftServerRequestProto proto)
          throws IOException {
        final RaftServerReplyProto reply = handleRaftServerRequestProto(proto);
        ctx.writeAndFlush(reply);
      }
    };

    final ChannelInitializer<SocketChannel> initializer
        = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        final ChannelPipeline p = ch.pipeline();

        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(RaftServerRequestProto.getDefaultInstance()));
        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());

        p.addLast(inboundHandler);
      }
    };

    channel = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(initializer)
        .bind(port)
        .syncUninterruptibly()
        .channel();
    lifeCycle.transition(LifeCycle.State.RUNNING);
  }

  @Override
  public void shutdown() {
    lifeCycle.transition(LifeCycle.State.CLOSING);
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    if (channel != null) {
      channel.close().awaitUninterruptibly();
    }
    lifeCycle.transition(LifeCycle.State.CLOSED);
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return (InetSocketAddress) channel.localAddress();
  }

  RaftServerReplyProto handleRaftServerRequestProto(RaftServerRequestProto proto)
      throws IOException {
    switch (proto.getRaftServerRequestCase()) {
      case REQUESTVOTEREQUEST: {
        final RequestVoteReplyProto reply = raftService.requestVote(
            proto.getRequestVoteRequest());
        return RaftServerReplyProto.newBuilder()
            .setRequestVoteReply(reply)
            .build();
      }
      case APPENDENTRIESREQUEST: {
        final AppendEntriesReplyProto reply = raftService.appendEntries(
            proto.getAppendEntriesRequest());
        return RaftServerReplyProto.newBuilder()
            .setAppendEntriesReply(reply)
            .build();
      }
      case INSTALLSNAPSHOTREQUEST: {
        final InstallSnapshotReplyProto reply = raftService.installSnapshot(
            proto.getInstallSnapshotRequest());
        return RaftServerReplyProto.newBuilder()
            .setInstallSnapshotReply(reply)
            .build();
      }
      case RAFTSERVERREQUEST_NOT_SET:
        throw new IllegalArgumentException("Request case not set in proto: "
            + proto.getRaftServerRequestCase());
      default:
        throw new UnsupportedOperationException("Request case not supported: "
            + proto.getRaftServerRequestCase());
    }
  }

  @Override
  public RequestVoteReplyProto sendRequestVote(RequestVoteRequestProto request) throws IOException {
    final RaftServerRequestProto proto = RaftServerRequestProto.newBuilder()
        .setRequestVoteRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftServerRequestProto(serverRequest, proto).getRequestVoteReply();
  }

  @Override
  public AppendEntriesReplyProto sendAppendEntries(AppendEntriesRequestProto request) throws IOException {
    final RaftServerRequestProto proto = RaftServerRequestProto.newBuilder()
        .setAppendEntriesRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftServerRequestProto(serverRequest, proto).getAppendEntriesReply();
  }

  @Override
  public InstallSnapshotReplyProto sendInstallSnapshot(InstallSnapshotRequestProto request) throws IOException {
    final RaftServerRequestProto proto = RaftServerRequestProto.newBuilder()
        .setInstallSnapshotRequest(request)
        .build();
    final RaftRpcRequestProto serverRequest = request.getServerRequest();
    return sendRaftServerRequestProto(serverRequest, proto).getInstallSnapshotReply();
  }

  private RaftServerReplyProto sendRaftServerRequestProto(
      RaftRpcRequestProto request, RaftServerRequestProto proto)
      throws IOException {
    final RaftServerProtocolProxy p = proxies.getProxy(request.getReplyId());
    return p.sendRaftServerRequestProto(request, proto);
  }

  @Override
  public void addPeers(Iterable<RaftPeer> peers) {
    proxies.addPeers(peers);
  }
}
