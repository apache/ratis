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
package org.apache.ratis.grpc.server;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NegotiationType;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.AbstractStub;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.thirdparty.io.netty.channel.WriteBufferWaterMark;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class GrpcStubPool<S extends AbstractStub<S>> {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcStubPool.class);

  static final class PooledStub<S extends AbstractStub<S>> {
    private final ManagedChannel ch;
    private final S stub;
    private final Semaphore permits;

    PooledStub(ManagedChannel ch, S stub, int maxInflight) {
      this.ch = ch;
      this.stub = stub;
      this.permits = new Semaphore(maxInflight);
    }

    S getStub() {
      return stub;
    }

    void release() {
      permits.release();
    }
  }

  private final List<PooledStub<S>> pool;
  private final NioEventLoopGroup elg;
  private final int size;

  GrpcStubPool(RaftPeer target, int n, Function<ManagedChannel, S> stubFactory, SslContext sslContext) {
    this(target, n, stubFactory, sslContext, Math.max(2, Runtime.getRuntime().availableProcessors() / 2), 16);
  }

  GrpcStubPool(RaftPeer target, int n, Function<ManagedChannel, S> stubFactory, SslContext sslContext,
               int elgThreads, int maxInflightPerConn) {
    this.elg = new NioEventLoopGroup(elgThreads);
    ArrayList<PooledStub<S>> tmp = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      NettyChannelBuilder channelBuilder = NettyChannelBuilder.forTarget(target.getAddress())
          .eventLoopGroup(elg)
          .channelType(NioSocketChannel.class)
          .keepAliveTime(30, TimeUnit.SECONDS)
          .keepAliveWithoutCalls(true)
          .idleTimeout(24, TimeUnit.HOURS)
          .withOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(64 << 10, 128 << 10));
      if (sslContext != null) {
        LOG.debug("Setting TLS for {}", target.getAddress());
        channelBuilder.useTransportSecurity().sslContext(sslContext);
      } else {
        channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
      }
      ManagedChannel ch = channelBuilder.build();
      tmp.add(new PooledStub<>(ch, stubFactory.apply(ch), maxInflightPerConn));
      ch.getState(true);
    }
    this.pool = Collections.unmodifiableList(tmp);
    this.size = n;
  }

  PooledStub<S> acquire() throws InterruptedException {
    final int start = ThreadLocalRandom.current().nextInt(size);
    for (int k = 0; k < size; k++) {
      PooledStub<S> p = pool.get((start + k) % size);
      if (p.permits.tryAcquire()) {
        return p;
      }
    }
    final PooledStub<S> p = pool.get(start);
    p.permits.acquire();
    return p;
  }

  public void close() {
    for (PooledStub<S> p : pool) {
      p.ch.shutdown();
    }
    elg.shutdownGracefully();
  }
}
