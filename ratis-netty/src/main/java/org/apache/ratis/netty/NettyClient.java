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

import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.channel.Channel;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;

import java.io.Closeable;
import java.net.InetSocketAddress;

public class NettyClient implements Closeable {
  private final LifeCycle lifeCycle;
  private final String serverAddress;
  private Channel channel;

  NettyClient(String serverAddress) {
    this.lifeCycle = new LifeCycle(JavaUtils.getClassSimpleName(getClass()) + "-" + serverAddress);
    this.serverAddress = serverAddress;
  }

  /** Connects to the given server address. */
  public void connect(EventLoopGroup group, ChannelInitializer<SocketChannel> initializer)
      throws InterruptedException {
    final InetSocketAddress address = NetUtils.createSocketAddr(serverAddress);
    lifeCycle.startAndTransition(
        () -> channel = new Bootstrap()
            .group(group)
            .channel(NettyUtils.getSocketChannelClass(group))
            .handler(initializer)
            .connect(address)
            .sync()
            .channel(),
        InterruptedException.class);
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> NettyUtils.closeChannel(channel, serverAddress));
  }

  public ChannelFuture writeAndFlush(Object msg) throws AlreadyClosedException {
    final LifeCycle.State state = lifeCycle.getCurrentState();
    if (state.isRunning()) {
      return channel.writeAndFlush(msg);
    }
    // For CLOSING, CLOSED, and EXCEPTION states, throw AlreadyClosedException to trigger reconnection
    if (state.isClosingOrClosed() || state == LifeCycle.State.EXCEPTION) {
      throw new AlreadyClosedException(
        "Client is closed or failed: state=" + state + ", channel=" + channel);
    }
    // For other states (NEW, STARTING, PAUSING, PAUSED), this is a programming error
    throw new IllegalStateException("Client is in unexpected state for writeAndFlush: " +
      "state=" + state + ", channel=" + channel);
  }

  @Override
  public String toString() {
    return lifeCycle.toString();
  }
}
