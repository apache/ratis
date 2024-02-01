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

import org.apache.ratis.BaseTest;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.thirdparty.io.grpc.KnownLength;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Test gRPC zero-copy feature.
 */
public final class TestGrpcZeroCopy extends BaseTest {
  static class RandomData {
    private static final Random random = new Random();
    private static final byte[] array = new byte[4096];

    static void fill(long seed, int size, ByteBuf buf) {
      random.setSeed(seed);
      for(int offset = 0; offset < size; ) {
        final int remaining = Math.min(size - offset, array.length);
        random.nextBytes(array);
        buf.writeBytes(array, 0, remaining);
        offset += remaining;
      }
    }

    static void verify(long seed, ByteString b) {
      random.setSeed(seed);
      final int size = b.size();
      for(int offset = 0; offset < size; ) {
        final int remaining = Math.min(size - offset, array.length);
        random.nextBytes(array);
        final ByteString expected = UnsafeByteOperations.unsafeWrap(array, 0, remaining);
        final ByteString computed = b.substring(offset, offset + remaining);
        Assertions.assertEquals(expected.size(), computed.size());
        Assertions.assertEquals(expected, computed);
        offset += remaining;
      }
    }
  }

  private static final boolean IS_ZERO_COPY_READY;

  static {
    // Check whether the Detachable class exists.
    boolean detachableClassExists = false;
    final String detachableClassName = KnownLength.class.getPackage().getName() + ".Detachable";
    try {
      Class.forName(detachableClassName);
      detachableClassExists = true;
    } catch (ClassNotFoundException e) {
      e.printStackTrace(System.out);
    }

    // Check whether the UnsafeByteOperations exists.
    boolean unsafeByteOperationsClassExists = false;
    final String unsafeByteOperationsClassName = MessageLite.class.getPackage().getName() + ".UnsafeByteOperations";
    try {
      Class.forName(unsafeByteOperationsClassName);
      unsafeByteOperationsClassExists = true;
    } catch (ClassNotFoundException e) {
      e.printStackTrace(System.out);
    }
    IS_ZERO_COPY_READY = detachableClassExists && unsafeByteOperationsClassExists;
  }

  public static boolean isReady() {
    return IS_ZERO_COPY_READY;
  }

  /** Test a zero-copy marshaller is available from the versions of gRPC and Protobuf. */
  @Test
  public void testReadiness() {
    Assertions.assertTrue(isReady());
  }


  @Test
  public void testZeroCopy() throws Exception {
    runTestZeroCopy();
  }

  void runTestZeroCopy() throws Exception {
    try (GrpcZeroCopyTestServer server = new GrpcZeroCopyTestServer(NetUtils.getFreePort())) {
      final int port = server.start();
      try (GrpcZeroCopyTestClient client = new GrpcZeroCopyTestClient(NetUtils.LOCALHOST, port)) {
        sendMessages(5, client, server);
        sendBinaries(11, client, server);
      }
    }
  }

  void sendMessages(int n, GrpcZeroCopyTestClient client, GrpcZeroCopyTestServer server) throws Exception {
    final List<String> messages = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      messages.add("m" + i);
    }

    final List<CompletableFuture<String>> futures = new ArrayList<>();
    for (String m : messages) {
      futures.add(client.send(m));
    }

    final int numElements = server.getZeroCopyCount().getNumElements();
    final long numBytes = server.getZeroCopyCount().getNumBytes();
    for (int i = 0; i < futures.size(); i++) {
      final String expected = GrpcZeroCopyTestServer.toReply(i, messages.get(i));
      final String reply = futures.get(i).get();
      Assertions.assertEquals(expected, reply, "expected = " + expected + " != reply = " + reply);
      server.assertCounts(numElements, numBytes);
    }
  }

  void sendBinaries(int n, GrpcZeroCopyTestClient client, GrpcZeroCopyTestServer server) throws Exception {
    final PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    int numElements = server.getZeroCopyCount().getNumElements();
    long numBytes = server.getZeroCopyCount().getNumBytes();

    for (int i = 0; i < n; i++) {
      final int size = 16 << (2 * i);
      LOG.info("buf {}: {}B", i, TraditionalBinaryPrefix.long2String(size));

      final CompletableFuture<ByteString> future;
      final ByteBuf buf = allocator.directBuffer(size, size);
      try {
        RandomData.fill(i, size, buf);
        future = client.send(buf.nioBuffer(0, buf.capacity()));
      } finally {
        buf.release();
      }

      final ByteString reply = future.get();
      Assertions.assertEquals(4, reply.size());
      Assertions.assertEquals(size, reply.asReadOnlyByteBuffer().getInt());

      numElements++;
      numBytes += size;
      server.assertCounts(numElements, numBytes);
    }
  }
}
