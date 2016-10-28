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

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.raft.netty.NettyClient;
import org.apache.raft.netty.proto.NettyProtos.*;
import org.apache.raft.proto.RaftProtos.*;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.util.RaftUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class RaftServerProtocolProxy implements Closeable {
  static long getSeqNum(RaftNettyServerReplyProto proto) {
    switch (proto.getRaftNettyServerReplyCase()) {
      case REQUESTVOTEREPLY:
        return proto.getRequestVoteReply().getServerReply().getSeqNum();
      case APPENDENTRIESREPLY:
        return proto.getAppendEntriesReply().getServerReply().getSeqNum();
      case INSTALLSNAPSHOTREPLY:
        return proto.getInstallSnapshotReply().getServerReply().getSeqNum();
      case RAFTNETTYSERVERREPLY_NOT_SET:
        throw new IllegalArgumentException("Reply case not set in proto: "
            + proto.getRaftNettyServerReplyCase());
      default:
        throw new UnsupportedOperationException("Reply case not supported: "
            + proto.getRaftNettyServerReplyCase());
    }
  }

  private final RaftPeer peer;

  private final ConcurrentHashMap<Long, CompletableFuture<RaftNettyServerReplyProto>> replyMap
      = new ConcurrentHashMap<>();

  private final NettyClient client;

  RaftServerProtocolProxy(RaftPeer peer) {
    this.peer = peer;

    final ChannelInboundHandler inboundHandler
        = new SimpleChannelInboundHandler<RaftNettyServerReplyProto>() {
      @Override
      protected void channelRead0(ChannelHandlerContext ctx,
                                  RaftNettyServerReplyProto proto) {
        final long seq = getSeqNum(proto);
        replyMap.remove(seq).complete(proto);
      }
    };
    final ChannelInitializer<SocketChannel> initializer
        = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        final ChannelPipeline p = ch.pipeline();

        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(RaftNettyServerReplyProto.getDefaultInstance()));
        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());

        p.addLast(inboundHandler);
      }
    };
    this.client = new NettyClient(initializer);
  }

  public void connect() throws InterruptedException {
    client.connect(peer.getAddress());
  }

  @Override
  public void close() {
    client.close();
  }

  RaftNettyServerReplyProto sendRaftNettyServerRequestProto(
      RaftRpcRequestProto request, RaftNettyServerRequestProto proto)
      throws IOException {
    final CompletableFuture<RaftNettyServerReplyProto> replyFuture = new CompletableFuture<>();
    replyMap.put(request.getSeqNum(), replyFuture);
    final ChannelFuture channelFuture = client.writeAndFlush(proto);

    try {
      channelFuture.await();
      return replyFuture.get();
    } catch (InterruptedException e) {
      throw RaftUtils.toInterruptedIOException(request + " interrupted.", e);
    } catch (ExecutionException e) {
      throw new IOException(request + " failed.", e);
    }
  }
}
