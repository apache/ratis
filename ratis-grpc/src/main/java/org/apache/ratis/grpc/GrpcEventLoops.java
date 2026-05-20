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
package org.apache.ratis.grpc;

import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.netty.channel.Channel;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.ServerChannel;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.Epoll;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ratis.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Helper for creating bounded Netty {@link EventLoopGroup} instances for gRPC,
 * along with matching {@link Channel} / {@link ServerChannel} types.
 *
 * <p>By default, gRPC uses a shared {@link EventLoopGroup} sized to
 * {@code Runtime.getRuntime().availableProcessors() * 2}, and Netty threads
 * in that group never exit once started. A traffic burst (e.g. a follower
 * catch-up after restart) can permanently inflate the active thread count.
 * This helper lets callers construct a smaller, dedicated group and wire it
 * into {@link NettyServerBuilder} / {@link NettyChannelBuilder} consistently
 * with the matching channel type.
 */
public final class GrpcEventLoops {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcEventLoops.class);

  private GrpcEventLoops() {}

  public static boolean isEpollAvailable() {
    return Epoll.isAvailable();
  }

  /** Create a new {@link EventLoopGroup} with the given number of threads. */
  public static EventLoopGroup newEventLoopGroup(int threads, String name) {
    if (threads <= 0) {
      throw new IllegalArgumentException("threads must be > 0, but got " + threads);
    }
    final ThreadFactory threadFactory = new DefaultThreadFactory(name);
    if (isEpollAvailable()) {
      LOG.info("Creating EpollEventLoopGroup({}) for {}", threads, name);
      return new EpollEventLoopGroup(threads, threadFactory);
    }
    LOG.info("Creating NioEventLoopGroup({}) for {} (Epoll not available)", threads, name);
    return new NioEventLoopGroup(threads, threadFactory);
  }

  public static Class<? extends ServerChannel> getServerChannelType() {
    return isEpollAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
  }

  public static Class<? extends Channel> getClientChannelType() {
    return isEpollAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;
  }

  /**
   * Configure the {@link NettyServerBuilder} to use the given boss and worker groups.
   *
   * <p>gRPC requires that the boss group, worker group, and channel type are
   * all provided together or all omitted. The caller owns the lifecycle of
   * both groups and must shut them down.
   */
  public static void configure(NettyServerBuilder serverBuilder,
      EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
    if (workerGroup == null) {
      return;
    }
    serverBuilder.channelType(getServerChannelType())
        .bossEventLoopGroup(bossGroup)
        .workerEventLoopGroup(workerGroup);
  }

  /** Configure the {@link NettyChannelBuilder} to use the given event-loop group. */
  public static void configure(NettyChannelBuilder channelBuilder, EventLoopGroup group) {
    if (group == null) {
      return;
    }
    channelBuilder.channelType(getClientChannelType())
        .eventLoopGroup(group);
  }

  /** Gracefully shut down the group, swallowing interruptions. */
  public static void shutdownGracefully(EventLoopGroup group) {
    if (group == null) {
      return;
    }
    try {
      group.shutdownGracefully(0, 3, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while shutting down EventLoopGroup", e);
    } catch (Exception e) {
      LOG.warn("Failed to shut down EventLoopGroup", e);
    }
  }
}
