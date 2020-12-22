/**
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
package org.apache.ratis.examples.filestore;

import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.datastream.DataStreamTestUtils;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.util.internal.ThreadLocalRandom;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

class FileStoreWriter implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(FileStoreWriter.class);

  final long seed = ThreadLocalRandom.current().nextLong();
  final byte[] buffer = new byte[4 << 10];

  final String fileName;
  final SizeInBytes fileSize;
  final FileStoreClient client;
  final Executor asyncExecutor;
  final int bufferSize;

  static Builder newBuilder() {
    return new Builder();
  }

  static class Builder {
    private String fileName;
    private SizeInBytes fileSize;
    private CheckedSupplier<FileStoreClient, IOException> clientSupplier;
    private Executor asyncExecutor;
    private int bufferSize;

    public Builder setFileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    public Builder setFileSize(SizeInBytes size) {
      this.fileSize = size;
      return this;
    }

    public Builder setFileStoreClientSupplier(CheckedSupplier<FileStoreClient, IOException> supplier) {
      this.clientSupplier = supplier;
      return this;
    }

    public Builder setAsyncExecutor(Executor asyncExecutor) {
      this.asyncExecutor = asyncExecutor;
      return this;
    }

    public Builder setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public FileStoreWriter build() throws IOException {
      return new FileStoreWriter(fileName, fileSize, asyncExecutor, clientSupplier, bufferSize);
    }
  }

  private FileStoreWriter(String fileName, SizeInBytes fileSize, Executor asyncExecutor,
         CheckedSupplier<FileStoreClient, IOException> clientSupplier, int bufferSize)
      throws IOException {
    this.fileName = fileName;
    this.fileSize = fileSize;
    this.client = clientSupplier.get();
    this.asyncExecutor = asyncExecutor;
    this.bufferSize = bufferSize;
  }

  ByteBuffer randomBytes(int length, Random random) {
    Preconditions.assertTrue(length <= buffer.length);
    random.nextBytes(buffer);
    final ByteBuffer b = ByteBuffer.wrap(buffer);
    b.limit(length);
    return b;
  }

  FileStoreWriter write(boolean sync) throws IOException {
    final Random r = new Random(seed);
    final int size = fileSize.getSizeInt();

    for(int offset = 0; offset < size; ) {
      final int remaining = size - offset;
      final int length = Math.min(remaining, buffer.length);
      final boolean close = length == remaining;

      final ByteBuffer b = randomBytes(length, r);

      LOG.trace("write {}, offset={}, length={}, close? {}",
          fileName, offset, length, close);
      final long written = client.write(fileName, offset, close, b, sync);
      Assert.assertEquals(length, written);
      offset += written;
    }
    return this;
  }

  public FileStoreWriter streamWriteAndVerify(RoutingTable routingTable) {
    final int size = fileSize.getSizeInt();
    final DataStreamOutput dataStreamOutput = client.getStreamOutput(fileName, size, routingTable);
    final List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
    final List<Integer> sizes = new ArrayList<>();

    for(int offset = 0; offset < size; ) {
      final int remaining = size - offset;
      final int length = Math.min(remaining, bufferSize);
      final boolean close = length == remaining;

      LOG.trace("write {}, offset={}, length={}, close? {}",
          fileName, offset, length, close);
      final ByteBuffer bf = DataStreamTestUtils.initBuffer(0, length);
      futures.add(close ?
          dataStreamOutput.writeAsync(bf, StandardWriteOption.CLOSE) : dataStreamOutput.writeAsync(bf));
      sizes.add(length);
      offset += length;
    }

    DataStreamReply reply = dataStreamOutput.closeAsync().join();
    Assert.assertTrue(reply.isSuccess());

    // TODO: handle when any of the writeAsync has failed.
    // check writeAsync requests
    for (int i = 0; i < futures.size(); i++) {
      reply = futures.get(i).join();
      Assert.assertTrue(reply.isSuccess());
      Assert.assertEquals(sizes.get(i).longValue(), reply.getBytesWritten());
      Assert.assertEquals(reply.getType(), RaftProtos.DataStreamPacketHeaderProto.Type.STREAM_DATA);
    }

    return this;
  }

  CompletableFuture<FileStoreWriter> writeAsync(boolean sync) {
    Objects.requireNonNull(asyncExecutor, "asyncExecutor == null");
    final Random r = new Random(seed);
    final int size = fileSize.getSizeInt();

    final CompletableFuture<FileStoreWriter> returnFuture = new CompletableFuture<>();
    final AtomicInteger callCount = new AtomicInteger();
    final AtomicInteger n = new AtomicInteger();
    for(; n.get() < size; ) {
      final int offset = n.get();
      final int remaining = size - offset;
      final int length = Math.min(remaining, buffer.length);
      final boolean close = length == remaining;

      final ByteBuffer b = randomBytes(length, r);

      callCount.incrementAndGet();
      n.addAndGet(length);

      LOG.trace("writeAsync {}, offset={}, length={}, close? {}",
          fileName, offset, length, close);
      client.writeAsync(fileName, offset, close, b, sync)
          .thenAcceptAsync(written -> Assert.assertEquals(length, (long)written), asyncExecutor)
          .thenRun(() -> {
            final int count = callCount.decrementAndGet();
            LOG.trace("writeAsync {}, offset={}, length={}, close? {}: n={}, callCount={}",
                fileName, offset, length, close, n.get(), count);
            if (n.get() == size && count == 0) {
              returnFuture.complete(this);
            }
          })
          .exceptionally(e -> {
            returnFuture.completeExceptionally(e);
            return null;
          });
    }
    return returnFuture;
  }

  FileStoreWriter verify() throws IOException {
    final Random r = new Random(seed);
    final int size = fileSize.getSizeInt();

    for(int offset = 0; offset < size; ) {
      final int remaining = size - offset;
      final int n = Math.min(remaining, buffer.length);
      final ByteString read = client.read(fileName, offset, n);
      final ByteBuffer expected = randomBytes(n, r);
      verify(read, offset, n, expected);
      offset += n;
    }
    return this;
  }

  CompletableFuture<FileStoreWriter> verifyAsync() {
    Objects.requireNonNull(asyncExecutor, "asyncExecutor == null");
    final Random r = new Random(seed);
    final int size = fileSize.getSizeInt();

    final CompletableFuture<FileStoreWriter> returnFuture = new CompletableFuture<>();
    final AtomicInteger callCount = new AtomicInteger();
    final AtomicInteger n = new AtomicInteger();
    for(; n.get() < size; ) {
      final int offset = n.get();
      final int remaining = size - offset;
      final int length = Math.min(remaining, buffer.length);

      callCount.incrementAndGet();
      n.addAndGet(length);
      final ByteBuffer expected = ByteString.copyFrom(randomBytes(length, r)).asReadOnlyByteBuffer();

      client.readAsync(fileName, offset, length)
          .thenAcceptAsync(read -> verify(read, offset, length, expected), asyncExecutor)
          .thenRun(() -> {
            final int count = callCount.decrementAndGet();
            LOG.trace("verifyAsync {}, offset={}, length={}: n={}, callCount={}",
                fileName, offset, length, n.get(), count);
            if (n.get() == size && count == 0) {
              returnFuture.complete(this);
            }
          })
          .exceptionally(e -> {
            returnFuture.completeExceptionally(e);
            return null;
          });
    }
    Assert.assertEquals(size, n.get());
    return returnFuture;
  }

  void verify(ByteString read, int offset, int length, ByteBuffer expected) {
    Assert.assertEquals(length, read.size());
    assertBuffers(offset, length, expected, read.asReadOnlyByteBuffer());
  }

  CompletableFuture<FileStoreWriter> deleteAsync() {
    Objects.requireNonNull(asyncExecutor, "asyncExecutor == null");
    return client.deleteAsync(fileName).thenApplyAsync(reply -> this, asyncExecutor);
  }

  FileStoreWriter delete() throws IOException {
    client.delete(fileName);
    return this;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  static void assertBuffers(int offset, int length, ByteBuffer expected, ByteBuffer computed) {
    try {
      Assert.assertEquals(expected, computed);
    } catch(AssertionError e) {
      LOG.error("Buffer mismatched at offset=" + offset + ", length=" + length
          + "\n  expected = " + StringUtils.bytes2HexString(expected)
          + "\n  computed = " + StringUtils.bytes2HexString(computed), e);
      throw e;
    }
  }
}
