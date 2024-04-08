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
package org.apache.ratis.grpc.util;

import org.apache.ratis.test.proto.GreeterGrpc;
import org.apache.ratis.test.proto.HelloReply;
import org.apache.ratis.test.proto.HelloRequest;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.util.concurrent.ThreadPerTaskExecutor;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** gRPC server for testing */
class GrpcTestServer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcTestServer.class);
  private static final AtomicLong COUNTER = new AtomicLong();

  private final Server server;

  GrpcTestServer(int port, int warmup, int slow, TimeDuration timeout) {
    this.server = ServerBuilder.forPort(port)
        .executor(new ThreadPerTaskExecutor(r -> Daemon.newBuilder()
            .setName("test-server-" + COUNTER.getAndIncrement())
            .setRunnable(r)
            .build()))
        .addService(new GreeterImpl(warmup, slow, timeout))
        .build();
  }

  int start() throws IOException {
    server.start();
    return server.getPort();
  }

  @Override
  public void close() throws IOException {
    try {
      server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException("Failed to close", e);
    }
  }

  static class GreeterImpl extends GreeterGrpc.GreeterImplBase {
    static String toReplySuffix(String request) {
      return ") Hello " + request;
    }

    private final int warmup;
    private final int slow;
    private final TimeDuration shortSleepTime;
    private final TimeDuration longSleepTime;
    private int count = 0;

    GreeterImpl(int warmup, int slow, TimeDuration timeout) {
      this.warmup = warmup;
      this.slow = slow;
      this.shortSleepTime = timeout.multiply(0.25);
      this.longSleepTime = timeout.multiply(2);
    }

    @Override
    public StreamObserver<HelloRequest> hello(StreamObserver<HelloReply> responseObserver) {
      return new StreamObserver<HelloRequest>() {
        @Override
        public void onNext(HelloRequest helloRequest) {
          final String reply = count + toReplySuffix(helloRequest.getName());
          final TimeDuration sleepTime = count < warmup ? TimeDuration.ZERO :
              count < (warmup + slow) ? shortSleepTime : longSleepTime;
          LOG.info("count = {}, slow = {}, sleep {}", reply, slow, sleepTime);
          try {
            sleepTime.sleep();
          } catch (InterruptedException e) {
            responseObserver.onError(e);
            return;
          }
          responseObserver.onNext(HelloReply.newBuilder().setMessage(reply).build());
          count++;
        }

        @Override
        public void onError(Throwable throwable) {
          LOG.error("onError", throwable);
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }
  }
}