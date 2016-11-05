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
package org.apache.raft.netty;

import com.google.common.base.Preconditions;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.raft.util.LifeCycle;
import org.apache.raft.util.RaftUtils;

import java.io.Closeable;
import java.net.InetSocketAddress;

public class NettyClient implements Closeable {
  private final LifeCycle lifeCycle = new LifeCycle(getClass().getSimpleName());

  private Channel channel;

  /** Connects to the given server address. */
  public void connect(String serverAddress, EventLoopGroup group,
                      ChannelInitializer<SocketChannel> initializer)
      throws InterruptedException {
    final InetSocketAddress address = RaftUtils.newInetSocketAddress(serverAddress);
    Preconditions.checkNotNull(address,
        "Failed to create InetSocketAddress from %s.", serverAddress);

    lifeCycle.startAndTransition(
        () -> channel = new Bootstrap()
            .group(group)
            .channel(NioSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .handler(initializer)
            .connect(address)
            .sync()
            .channel(),
        InterruptedException.class);
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> {
      channel.close().syncUninterruptibly();
    });
  }

  public ChannelFuture writeAndFlush(Object msg) {
    lifeCycle.assertCurrentState(LifeCycle.State.RUNNING);
    return channel.writeAndFlush(msg);
  }
}
