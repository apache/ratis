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
import org.apache.ratis.test.proto.HelloReply;
import org.apache.ratis.test.proto.HelloRequest;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.ServerMethodDefinition;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** gRPC server for testing */
class GrpcZeroCopyTestServer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcZeroCopyTestServer.class);

  static class Count {
    private int numElements;
    private long numBytes;

    synchronized int getNumElements() {
      return numElements;
    }

    synchronized long getNumBytes() {
      return numBytes;
    }

    synchronized void inc(ByteString data) {
      numElements++;
      numBytes += data.size();
    }

    void inc(BinaryRequest request) {
      inc(request.getData());
    }

    @Override
    public synchronized String toString() {
      return numElements + ", " + TraditionalBinaryPrefix.long2String(numBytes) + "B";
    }
  }

  private final Count zeroCopyCount = new Count();
  private final Count nonZeroCopyCount = new Count();
  private final Count releasedCount = new Count();

  private final Server server;
  private final ZeroCopyMessageMarshaller<BinaryRequest> marshaller = new ZeroCopyMessageMarshaller<>(
      BinaryRequest.getDefaultInstance(),
      zeroCopyCount::inc,
      nonZeroCopyCount::inc,
      releasedCount::inc);

  GrpcZeroCopyTestServer(int port) {
    final GreeterImpl greeter = new GreeterImpl();
    final MethodDescriptor<BinaryRequest, BinaryReply> binary = GreeterGrpc.getBinaryMethod();
    final String binaryFullMethodName = binary.getFullMethodName();
    final ServerServiceDefinition service = greeter.bindService();
    @SuppressWarnings("unchecked")
    final ServerMethodDefinition<BinaryRequest, BinaryReply> method
        = (ServerMethodDefinition<BinaryRequest, BinaryReply>) service.getMethod(binaryFullMethodName);
    final ServerServiceDefinition.Builder builder = ServerServiceDefinition.builder(
        service.getServiceDescriptor().getName());
    builder.addMethod(binary.toBuilder().setRequestMarshaller(marshaller).build(), method.getServerCallHandler());

    service.getMethods().stream()
        .filter(m -> !m.getMethodDescriptor().getFullMethodName().equals(binaryFullMethodName))
        .forEach(builder::addMethod);

    this.server = ServerBuilder.forPort(port)
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .addService(builder.build())
        .build();
  }

  Count getZeroCopyCount() {
    return zeroCopyCount;
  }

  Count getNonZeroCopyCount() {
    return nonZeroCopyCount;
  }

  void assertCounts(int expectNumElements, long expectNumBytes) {
    LOG.info("ZeroCopyCount    = {}", zeroCopyCount);
    LOG.info("nonZeroCopyCount = {}", nonZeroCopyCount);
    Assert.assertEquals("zeroCopyCount.getNumElements()", expectNumElements, zeroCopyCount.getNumElements());
    Assert.assertEquals("zeroCopyCount.getNumBytes()", expectNumBytes, zeroCopyCount.getNumBytes());
    Assert.assertEquals("nonZeroCopyCount.getNumElements()", 0, nonZeroCopyCount.getNumElements());
    Assert.assertEquals("nonZeroCopyCount.getNumBytes()", 0, nonZeroCopyCount.getNumBytes());
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

  static String toReply(int i, String request) {
    return i + ") hi " + request;
  }

  class GreeterImpl extends GreeterGrpc.GreeterImplBase {
    @Override
    public StreamObserver<HelloRequest> hello(StreamObserver<HelloReply> responseObserver) {
      final AtomicInteger count = new AtomicInteger();
      return new StreamObserver<HelloRequest>() {
        @Override
        public void onNext(HelloRequest request) {
          final String reply = toReply(count.getAndIncrement(), request.getName());
          LOG.info("reply {}", reply);
          responseObserver.onNext(HelloReply.newBuilder().setMessage(reply).build());
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

    @Override
    public StreamObserver<BinaryRequest> binary(StreamObserver<BinaryReply> responseObserver) {
      final AtomicInteger count = new AtomicInteger();
      return new StreamObserver<BinaryRequest>() {
        @Override
        public void onNext(BinaryRequest request) {
          try {
            final ByteString data = request.getData();
            int i = count.getAndIncrement();
            LOG.info("Received {}) data.size() = {}", i, data.size());
            TestGrpcZeroCopy.RandomData.verify(i, data);
            final byte[] bytes = new byte[4];
            ByteBuffer.wrap(bytes).putInt(data.size());
            responseObserver.onNext(BinaryReply.newBuilder().setData(UnsafeByteOperations.unsafeWrap(bytes)).build());
          } finally {
            marshaller.release(request);
          }
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
