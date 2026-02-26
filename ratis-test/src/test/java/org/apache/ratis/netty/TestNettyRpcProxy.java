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
package org.apache.ratis.netty;

import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.proto.RaftProtos.GroupListRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerReplyProto;
import org.apache.ratis.proto.netty.NettyProtos.RaftNettyServerRequestProto;
import org.apache.ratis.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.Channel;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.util.JavaUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNettyRpcProxy extends BaseTest {
  @Test
  public void testOfferRollbackOnAlreadyClosed() throws Exception {
    // Minimal netty server to allow client connect; we don't need to process requests.
    final EventLoopGroup bossGroup = NettyUtils.newEventLoopGroup("test-netty-boss", 1, false);
    final EventLoopGroup workerGroup = NettyUtils.newEventLoopGroup("test-netty-worker", 1, false);
    final EventLoopGroup clientGroup = NettyUtils.newEventLoopGroup("test-netty-client", 1, false);
    Channel serverChannel = null;
    NettyRpcProxy proxy = null;
    try {
      final ChannelFuture bindFuture = new ServerBootstrap()
          .group(bossGroup, workerGroup)
          .channel(NettyUtils.getServerChannelClass(workerGroup))
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                }
              });
            }
          })
          .bind(0)
          .sync();
      serverChannel = bindFuture.channel();

      final InetSocketAddress address = (InetSocketAddress) serverChannel.localAddress();
      final String peerAddress = address.getHostString() + ":" + address.getPort();
      final RaftPeer peer = RaftPeer.newBuilder().setId("s0").setAddress(peerAddress).build();
      proxy = new NettyRpcProxy(peer, new RaftProperties(), clientGroup);

      // Close to force AlreadyClosedException on write and trigger rollback logic.
      proxy.close();
      final RaftRpcRequestProto rpcRequest = RaftRpcRequestProto.newBuilder()
          .setCallId(1)
          .build();
      final GroupListRequestProto groupListRequest = GroupListRequestProto.newBuilder()
          .setRpcRequest(rpcRequest)
          .build();
      final RaftNettyServerRequestProto request = RaftNettyServerRequestProto.newBuilder()
          .setGroupListRequest(groupListRequest)
          .build();
      final CompletableFuture<RaftNettyServerReplyProto> reply =
          proxy.sendAsync(request);

      // Ensure the future completes exceptionally with AlreadyClosedException.
      final Throwable thrown = assertThrows(CompletionException.class, reply::join);
      final Throwable unwrapped = JavaUtils.unwrapCompletionException(thrown);
      assertInstanceOf(AlreadyClosedException.class, unwrapped);

      // The replies queue must be empty after rollback; use reflection to reach it.
      final Object connection = getField(proxy, "connection");
      final Map<?, ?> replies = getField(connection, "replies");
      assertTrue(replies.isEmpty());
    } finally {
      if (proxy != null) {
        proxy.close();
      }
      if (serverChannel != null) {
        serverChannel.close().sync();
      }
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
      clientGroup.shutdownGracefully();
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T getField(Object target, String name) throws Exception {
    final Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return (T) field.get(target);
  }
}
