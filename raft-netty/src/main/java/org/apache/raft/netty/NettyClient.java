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

import io.netty.bootstrap.Bootstrap;
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

  private ChannelFuture channelFuture;

  /** Connects to the given server address. */
  public void connect(String serverAddress, EventLoopGroup group,
                      ChannelInitializer<SocketChannel> initializer)
      throws InterruptedException {
    lifeCycle.transition(LifeCycle.State.STARTING);
    final InetSocketAddress address = RaftUtils.newInetSocketAddress(serverAddress);
    channelFuture  = new Bootstrap()
        .group(group)
        .channel(NioSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .handler(initializer)
        .connect(address)
        .sync();
    lifeCycle.transition(LifeCycle.State.RUNNING);
  }

  @Override
  public void close() {
    for(;;) {
      final LifeCycle.State current = lifeCycle.getCurrentState();
      if (current == LifeCycle.State.CLOSING
          || current == LifeCycle.State.CLOSED) {
        return; //already closing or closed.
      }
      if (lifeCycle.compareAndTransition(current, LifeCycle.State.CLOSING)) {
        channelFuture.channel().close();
        lifeCycle.transition(LifeCycle.State.CLOSED);
        return;
      }

      // lifecycle state is changed, retry.
    }
  }

  public ChannelFuture writeAndFlush(Object msg) {
    return channelFuture.channel().writeAndFlush(msg);
  }
}
