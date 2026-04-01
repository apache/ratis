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
package org.apache.ratis.netty;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.thirdparty.io.netty.channel.*;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerRequestProto;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LogLevel;
import org.apache.ratis.thirdparty.io.netty.handler.logging.LoggingHandler;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto.RaftNettyServerReplyCase.EXCEPTIONREPLY;

public class NettyRpcProxy implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(NettyRpcProxy.class);
  public static class PeerMap extends PeerProxyMap<NettyRpcProxy> {
    private final EventLoopGroup group;

    public PeerMap(String name, RaftProperties properties) {
      this(name, properties, NettyUtils.newEventLoopGroup(name, 0,
          NettyConfigKeys.Client.useEpoll(properties)));
    }

    private PeerMap(String name, RaftProperties properties, EventLoopGroup group) {
      super(name, peer -> {
        try {
          return new NettyRpcProxy(peer, properties, group);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw IOUtils.toInterruptedIOException("Failed connecting to " + peer, e);
        }
      });
      this.group = group;
    }

    @Override
    public void close() {
      super.close();
      group.shutdownGracefully();
    }
  }

  static RaftRpcRequestProto getRequest(RaftNettyServerRequestProto proto) {
    final RaftNettyServerRequestProto.RaftNettyServerRequestCase requestCase = proto.getRaftNettyServerRequestCase();
    switch (requestCase) {
      case REQUESTVOTEREQUEST:
        return proto.getRequestVoteRequest().getServerRequest();
      case APPENDENTRIESREQUEST:
        return proto.getAppendEntriesRequest().getServerRequest();
      case INSTALLSNAPSHOTREQUEST:
        return proto.getInstallSnapshotRequest().getServerRequest();
      case RAFTCLIENTREQUEST:
        return proto.getRaftClientRequest().getRpcRequest();
      case SETCONFIGURATIONREQUEST:
        return proto.getSetConfigurationRequest().getRpcRequest();
      case GROUPMANAGEMENTREQUEST:
        return proto.getGroupManagementRequest().getRpcRequest();
      case GROUPLISTREQUEST:
        return proto.getGroupListRequest().getRpcRequest();
      case GROUPINFOREQUEST:
        return proto.getGroupInfoRequest().getRpcRequest();
      case TRANSFERLEADERSHIPREQUEST:
        return proto.getTransferLeadershipRequest().getRpcRequest();
      case STARTLEADERELECTIONREQUEST:
        return proto.getStartLeaderElectionRequest().getServerRequest();
      case SNAPSHOTMANAGEMENTREQUEST:
        return proto.getSnapshotManagementRequest().getRpcRequest();
      case LEADERELECTIONMANAGEMENTREQUEST:
        return proto.getLeaderElectionManagementRequest().getRpcRequest();

      case RAFTNETTYSERVERREQUEST_NOT_SET:
        throw new IllegalArgumentException("Request case not set in proto: " + requestCase);
      default:
        throw new UnsupportedOperationException("Request case not supported: " + requestCase);
    }
  }

  public static long getCallId(RaftNettyServerReplyProto proto) {
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
      case RAFTNETTYSERVERREPLY_NOT_SET:
        throw new IllegalArgumentException("Reply case not set in proto: "
            + proto.getRaftNettyServerReplyCase());
      default:
        throw new UnsupportedOperationException("Reply case not supported: "
            + proto.getRaftNettyServerReplyCase());
    }
  }


  class Connection implements Closeable {
    private final NettyClient client = new NettyClient(peer.getAddress());
    private final Map<Long, CompletableFuture<RaftNettyServerReplyProto>> replies = new ConcurrentHashMap<>();

    Connection(EventLoopGroup group) throws InterruptedException {
      final ChannelInboundHandler inboundHandler
          = new SimpleChannelInboundHandler<RaftNettyServerReplyProto>() {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx,
                                    RaftNettyServerReplyProto proto) {
          final long callId = getCallId(proto);
          final CompletableFuture<RaftNettyServerReplyProto> future = getReplyFuture(callId, null, "reply");
          if (future == null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Ignoring reply for callId={} from {} (no outstanding request, outstanding={})",
                  callId, peer, replies.size());
            }
            return;
          }
          if (proto.getRaftNettyServerReplyCase() == EXCEPTIONREPLY) {
            final Object ioe = ProtoUtils.toObject(proto.getExceptionReply().getException());
            future.completeExceptionally((IOException)ioe);
          } else {
            future.complete(proto);
          }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
          client.close();
          failOutstandingRequests(new IOException("Caught an exception for the connection to " + peer, cause));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
          failOutstandingRequests(new AlreadyClosedException("Channel to " + peer + " is inactive."));
          super.channelInactive(ctx);
        }
      };
      final ChannelInitializer<SocketChannel> initializer
          = new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          final ChannelPipeline p = ch.pipeline();

          // LoggingHandler emits all events at the chosen level; use DEBUG to reduce noise by default.
          p.addLast(new LoggingHandler(LogLevel.DEBUG));
          p.addLast(new ProtobufVarint32FrameDecoder());
          p.addLast(new ProtobufDecoder(RaftNettyServerReplyProto.getDefaultInstance()));
          p.addLast(new ProtobufVarint32LengthFieldPrepender());
          p.addLast(new ProtobufEncoder());

          p.addLast(inboundHandler);
        }
      };

      client.connect(group, initializer);
    }

    private CompletableFuture<RaftNettyServerReplyProto> getReplyFuture(long callId,
        CompletableFuture<RaftNettyServerReplyProto> expected, String reason) {
      final CompletableFuture<RaftNettyServerReplyProto> removed = replies.remove(callId);
      if (removed == null && LOG.isDebugEnabled()) {
        LOG.debug("Request {} not found for callId={} from {} (reason={}, outstanding={})",
            expected == null ? "future" : "reply", callId, peer, reason, replies.size());
      }
      if (expected != null) {
        Preconditions.assertSame(expected, removed, "removed");
      }
      return removed;
    }

    synchronized CompletableFuture<RaftNettyServerReplyProto> offer(RaftNettyServerRequestProto request) {
      final CompletableFuture<RaftNettyServerReplyProto> reply = new CompletableFuture<>();
      final long callId = getRequest(request).getCallId();
      final CompletableFuture<RaftNettyServerReplyProto> previous = replies.put(callId, reply);
      Preconditions.assertNull(previous, "previous");

      final ChannelFuture future;
      try {
        future = client.writeAndFlush(request);
      } catch (AlreadyClosedException e) {
        replies.remove(callId, reply);
        return JavaUtils.completeExceptionally(e);
      }

      future.addListener(cf -> {
        if (!cf.isSuccess()) {
          // Remove from queue on async write failure to prevent reply mismatch.
          // Only complete exceptionally if removal succeeds (not already polled).
          final CompletableFuture<RaftNettyServerReplyProto> removed =
              getReplyFuture(callId, reply, "write-failure");
          if (removed != null) {
            removed.completeExceptionally(cf.cause());
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("Write failed for callId={} to {} after request removed", callId, peer, cf.cause());
          }
          client.close();
        }
      });
      return reply;
    }

    @Override
    public synchronized void close() {
      client.close();
      failOutstandingRequests(new AlreadyClosedException("Closing connection to " + peer));
    }

    private synchronized void failOutstandingRequests(Throwable cause) {
      if (!replies.isEmpty()) {
        LOG.warn("Still have {} requests outstanding from {} connection: {}",
            replies.size(), peer, cause.toString());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Outstanding request ids from {}: {}", peer, replies.keySet());
        }
        replies.values().forEach(f -> f.completeExceptionally(cause));
        replies.clear();
      }
    }
  }

  private final RaftPeer peer;
  private final Connection connection;
  private final TimeDuration requestTimeoutDuration;

  public NettyRpcProxy(RaftPeer peer, RaftProperties properties, EventLoopGroup group) throws InterruptedException {
    this.peer = peer;
    this.connection = new Connection(group);
    this.requestTimeoutDuration = RaftClientConfigKeys.Rpc.requestTimeout(properties);
  }

  @Override
  public void close() {
    connection.close();
  }

  public CompletableFuture<RaftNettyServerReplyProto> sendAsync(RaftNettyServerRequestProto proto) {
    return connection.offer(proto);
  }

  public RaftNettyServerReplyProto send(
      RaftRpcRequestProto request, RaftNettyServerRequestProto proto)
      throws IOException {
    final CompletableFuture<RaftNettyServerReplyProto> reply = sendAsync(proto);
    try {
      TimeDuration newDuration = requestTimeoutDuration.add(request.getTimeoutMs(), TimeUnit.MILLISECONDS);
      return reply.get(newDuration.getDuration(), newDuration.getUnit());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(ProtoUtils.toString(request)
          + " sending from " + peer + " is interrupted.", e);
    } catch (ExecutionException e) {
      throw IOUtils.toIOException(e);
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e.getMessage(), e);
    }
  }
}
