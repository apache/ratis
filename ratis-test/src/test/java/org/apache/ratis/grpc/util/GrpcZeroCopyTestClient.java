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

import org.apache.ratis.test.proto.BinaryReply;
import org.apache.ratis.test.proto.BinaryRequest;
import org.apache.ratis.test.proto.GreeterGrpc;
import org.apache.ratis.test.proto.GreeterGrpc.GreeterStub;
import org.apache.ratis.test.proto.HelloReply;
import org.apache.ratis.test.proto.HelloRequest;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** gRPC client for testing */
class GrpcZeroCopyTestClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcZeroCopyTestClient.class);

  private final ManagedChannel channel;

  private final StreamObserver<HelloRequest> helloRequestHandler;
  private final Queue<CompletableFuture<String>> helloReplies = new ConcurrentLinkedQueue<>();

  private final StreamObserver<BinaryRequest> binaryRequestHandler;
  private final Queue<CompletableFuture<ByteString>> binaryReplies = new ConcurrentLinkedQueue<>();

  GrpcZeroCopyTestClient(String host, int port) {
    this.channel = ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext()
        .build();
    final GreeterStub asyncStub = GreeterGrpc.newStub(channel);

    final StreamObserver<HelloReply> helloResponseHandler = new StreamObserver<HelloReply>() {
      @Override
      public void onNext(HelloReply helloReply) {
        final CompletableFuture<String> polled = helloReplies.poll();
        Objects.requireNonNull(polled, "polled");
        polled.complete(helloReply.getMessage());
      }

      @Override
      public void onError(Throwable throwable) {
        LOG.info("onError", throwable);
        completeExceptionally(throwable);
      }

      @Override
      public void onCompleted() {
        LOG.info("onCompleted");
        completeExceptionally(new IllegalStateException("onCompleted"));
      }

      void completeExceptionally(Throwable throwable) {
        helloReplies.forEach(f -> f.completeExceptionally(throwable));
        helloReplies.clear();
      }
    };

    this.helloRequestHandler = asyncStub.hello(helloResponseHandler);

    final StreamObserver<BinaryReply> binaryResponseHandler = new StreamObserver<BinaryReply>() {
      @Override
      public void onNext(BinaryReply binaryReply) {
        final CompletableFuture<ByteString> polled = binaryReplies.poll();
        Objects.requireNonNull(polled, "polled");
        polled.complete(binaryReply.getData());
      }

      @Override
      public void onError(Throwable throwable) {
        LOG.info("onError", throwable);
        completeExceptionally(throwable);
      }

      @Override
      public void onCompleted() {
        LOG.info("onCompleted");
        completeExceptionally(new IllegalStateException("onCompleted"));
      }

      void completeExceptionally(Throwable throwable) {
        binaryReplies.forEach(f -> f.completeExceptionally(throwable));
        binaryReplies.clear();
      }
    };
    this.binaryRequestHandler = asyncStub.binary(binaryResponseHandler);
  }

  @Override
  public void close() throws IOException {
    try {
      /* After the request handler is cancelled, no more life-cycle hooks are allowed,
       * see {@link org.apache.ratis.thirdparty.io.grpc.ClientCall.Listener#cancel(String, Throwable)} */
      // requestHandler.onCompleted();
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException("Failed to close", e);
    }
  }

  CompletableFuture<String> send(String name) {
    LOG.info("send message {}", name);
    final HelloRequest request = HelloRequest.newBuilder().setName(name).build();
    return send(request, helloReplies, helloRequestHandler::onNext);
  }

  CompletableFuture<ByteString> send(ByteBuffer data) {
    LOG.info("send data: size={}, direct? {}", data.remaining(), data.isDirect());
    final BinaryRequest request = BinaryRequest.newBuilder().setData(UnsafeByteOperations.unsafeWrap(data)).build();
    return send(request, binaryReplies, binaryRequestHandler::onNext);
  }

  static <REQUEST, REPLY> CompletableFuture<REPLY> send(REQUEST request,
      Queue<CompletableFuture<REPLY>> queue, Consumer<REQUEST> onNext) {
    final CompletableFuture<REPLY> f = new CompletableFuture<>();
    queue.offer(f);
    try {
      onNext.accept(request);
    } catch (Exception e) {
      // already closed
      f.completeExceptionally(e);
      final CompletableFuture<REPLY> polled = queue.poll();
      Preconditions.assertSame(f, polled, "future");
    }
    return f;
  }
}