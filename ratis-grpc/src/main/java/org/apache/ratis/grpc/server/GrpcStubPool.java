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
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.AbstractStub;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.thirdparty.io.netty.channel.WriteBufferWaterMark;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

final class GrpcStubPool<S extends AbstractStub<S>> implements AutoCloseable {

    static final class PooledStub<S extends AbstractStub<S>> {
        final ManagedChannel ch;
        final S stub;
        final Semaphore permits;
        PooledStub(ManagedChannel ch, S stub, int maxInflight) {
            this.ch = ch;
            this.stub = stub;
            this.permits = new Semaphore(maxInflight);
        }
    }

    private final List<PooledStub<S>> pool;
    private final AtomicInteger rr = new AtomicInteger();
    private final NioEventLoopGroup elg;
    private final int size;

    GrpcStubPool(RaftPeer target, int n, java.util.function.Function<ManagedChannel, S> stubFactory) {
        this(target, n, stubFactory, Math.max(2, Runtime.getRuntime().availableProcessors()/2), 16);
    }

    GrpcStubPool(RaftPeer target, int n,
                 java.util.function.Function<ManagedChannel, S> stubFactory,
                 int elgThreads, int maxInflightPerConn) {
        this.elg = new NioEventLoopGroup(elgThreads);
        java.util.ArrayList<PooledStub<S>> tmp = new java.util.ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ManagedChannel ch = NettyChannelBuilder.forTarget(target.getAddress())
                    .eventLoopGroup(elg)
                    .channelType(NioSocketChannel.class)
                    .keepAliveTime(30, java.util.concurrent.TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .idleTimeout(24, java.util.concurrent.TimeUnit.HOURS)
                    .withOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(64<<10, 128<<10))
                    .usePlaintext()
                    .build();
            tmp.add(new PooledStub<>(ch, stubFactory.apply(ch), maxInflightPerConn));
            ch.getState(true);
        }
        this.pool = java.util.Collections.unmodifiableList(tmp);
        this.size = n;
    }

    PooledStub<S> acquire() throws InterruptedException {
        int start = rr.getAndIncrement();
        for (int k = 0; k < size; k++) {
            PooledStub<S> p = pool.get((start + k) % size);
            if (p.permits.tryAcquire()) return p;
        }
        PooledStub<S> p = pool.get(Math.floorMod(start, size));
        p.permits.acquire();
        return p;
    }

    void release(PooledStub<S> p) { p.permits.release(); }

    @Override public void close() {
        for (PooledStub p: pool) p.ch.shutdown();
        elg.shutdownGracefully();
    }

}
