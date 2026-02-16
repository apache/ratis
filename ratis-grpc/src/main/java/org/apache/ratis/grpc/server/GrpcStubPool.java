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

import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NegotiationType;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.AbstractStub;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.thirdparty.io.netty.channel.WriteBufferWaterMark;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;
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

  static ManagedChannel buildManagedChannel(String address, SslContext sslContext) {
    NettyChannelBuilder channelBuilder = NettyChannelBuilder.forTarget(address)
        .keepAliveTime(10, TimeUnit.MINUTES)
        .keepAliveWithoutCalls(false)
        .idleTimeout(30, TimeUnit.MINUTES)
        .withOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(64 << 10, 128 << 10));
    if (sslContext != null) {
      LOG.debug("Setting TLS for {}", address);
      channelBuilder.useTransportSecurity().sslContext(sslContext);
    } else {
      channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
    }
    ManagedChannel ch = channelBuilder.build();
    ch.getState(true);
    return ch;
  }

  static final class Stub<S extends AbstractStub<S>> {
    private final ManagedChannel ch;
    private final S stub;
    private final Semaphore permits;

    Stub(String address, SslContext sslContext, Function<ManagedChannel, S> stubFactory, int maxInflight) {
      this.ch = buildManagedChannel(address, sslContext);
      this.stub = stubFactory.apply(ch);
      this.permits = new Semaphore(maxInflight);
    }

    S getStub() {
      return stub;
    }

    void release() {
      permits.release();
    }

    void shutdown() {
      GrpcUtil.shutdownManagedChannel(ch);
    }
  }

  private final List<MemoizedSupplier<Stub<S>>> pool;

  GrpcStubPool(int connections, String address, SslContext sslContext, Function<ManagedChannel, S> stubFactory,
               int maxInflightPerConn) {
    Preconditions.assertTrue(connections > 1, "connections must be > 1");
    final List<MemoizedSupplier<Stub<S>>> tmpPool = new ArrayList<>(connections);
    for (int i = 0; i < connections; i++) {
      tmpPool.add(MemoizedSupplier.valueOf(() -> new Stub<>(address, sslContext, stubFactory, maxInflightPerConn)));
    }
    this.pool = Collections.unmodifiableList(tmpPool);
  }

  Stub<S> getStub(int i) {
    return pool.get(i).get();
  }

  Stub<S> acquire() throws InterruptedException {
    final int size = pool.size();
    final int start = ThreadLocalRandom.current().nextInt(size);
    for (int k = 0; k < size; k++) {
      Stub<S> p = getStub((start + k) % size);
      if (p.permits.tryAcquire()) {
        return p;
      }
    }
    final Stub<S> p = getStub(start);
    p.permits.acquire();
    return p;
  }

  public void close() {
    for (MemoizedSupplier<Stub<S>> p : pool) {
      if (p.isInitialized()) {
        p.get().shutdown();
      }
    }
  }
}
