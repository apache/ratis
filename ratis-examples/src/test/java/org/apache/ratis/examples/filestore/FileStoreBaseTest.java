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
package org.apache.ratis.examples.filestore;

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.util.internal.ThreadLocalRandom;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FileStoreBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  public static final Logger LOG = LoggerFactory.getLogger(FileStoreBaseTest.class);

  {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        FileStoreStateMachine.class, StateMachine.class);
    ConfUtils.setFile(p::setFile, FileStoreCommon.STATEMACHINE_DIR_KEY,
        new File(getClassTestDir(), "filestore"));
  }

  static final int NUM_PEERS = 3;

  @Test
  public void testFileStore() throws Exception {
    final CLUSTER cluster = newCluster(NUM_PEERS);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    final CheckedSupplier<FileStoreClient, IOException> newClient =
        () -> new FileStoreClient(cluster.getGroup(), getProperties());

    testSingleFile("foo", SizeInBytes.valueOf("10M"), newClient);
    testMultipleFiles("file", 100, SizeInBytes.valueOf("1M"), newClient);

    cluster.shutdown();
  }

  private static void testSingleFile(
      String path, SizeInBytes fileLength, CheckedSupplier<FileStoreClient, IOException> newClient)
      throws Exception {
    LOG.info("runTestSingleFile with path={}, fileLength={}", path, fileLength);

    try (final Writer w = new Writer(path, fileLength, null, newClient)) {
      w.write().verify().delete();
    }
  }

  private static void testMultipleFiles(
      String pathPrefix, int numFile, SizeInBytes fileLength,
      CheckedSupplier<FileStoreClient, IOException> newClient) throws Exception {
    LOG.info("runTestMultipleFile with pathPrefix={}, numFile={}, fileLength={}",
        pathPrefix, numFile, fileLength);

    final ExecutorService executor = Executors.newFixedThreadPool(20);

    final List<Future<Writer>> writerFutures = new ArrayList<>();
    for (int i = 0; i < numFile; i++) {
      final String path = String.format("%s%02d", pathPrefix, i);
      final Callable<Writer> callable = LogUtils.newCallable(LOG,
          () -> new Writer(path, fileLength, null, newClient).write(),
          () -> path + ":" + fileLength);
      writerFutures.add(executor.submit(callable));
    }

    final List<Writer> writers = new ArrayList<>();
    for(Future<Writer> f : writerFutures) {
      writers.add(f.get());
    }

    writerFutures.clear();
    for (Writer w : writers) {
      writerFutures.add(executor.submit(() -> w.verify().delete()));
    }
    for(Future<Writer> f : writerFutures) {
      f.get().close();
    }

    executor.shutdown();
  }

  static class Writer implements Closeable {
    final long seed = ThreadLocalRandom.current().nextLong();
    final byte[] buffer = new byte[4 << 10];

    final String fileName;
    final SizeInBytes fileSize;
    final FileStoreClient client;
    final Executor asyncExecutor;

    Writer(String fileName, SizeInBytes fileSize, Executor asyncExecutor,
        CheckedSupplier<FileStoreClient, IOException> clientSupplier)
        throws IOException {
      this.fileName = fileName;
      this.fileSize = fileSize;
      this.client = clientSupplier.get();
      this.asyncExecutor = asyncExecutor;
    }

    ByteBuffer randomBytes(int length, Random random) {
      Preconditions.assertTrue(length <= buffer.length);
      random.nextBytes(buffer);
      final ByteBuffer b = ByteBuffer.wrap(buffer);
      b.limit(length);
      return b;
    }

    Writer write() throws IOException {
      final Random r = new Random(seed);
      final int size = fileSize.getSizeInt();

      for(int offset = 0; offset < size; ) {
        final int remaining = size - offset;
        final int length = Math.min(remaining, buffer.length);
        final boolean close = length == remaining;

        final ByteBuffer b = randomBytes(length, r);

        LOG.trace("write {}, offset={}, length={}, close? {}",
            fileName, offset, length, close);
        final long written = client.write(fileName, offset, close, b);
        Assert.assertEquals(length, written);
        offset += written;
      }
      return this;
    }

    CompletableFuture<Writer> writeAsync() {
      Objects.requireNonNull(asyncExecutor, "asyncExecutor == null");
      final Random r = new Random(seed);
      final int size = fileSize.getSizeInt();

      final CompletableFuture<Writer> returnFuture = new CompletableFuture<>();
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
        client.writeAsync(fileName, offset, close, b)
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

    Writer verify() throws IOException {
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

    CompletableFuture<Writer> verifyAsync() {
      Objects.requireNonNull(asyncExecutor, "asyncExecutor == null");
      final Random r = new Random(seed);
      final int size = fileSize.getSizeInt();

      final CompletableFuture<Writer> returnFuture = new CompletableFuture<>();
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

    CompletableFuture<Writer> deleteAsync() {
      Objects.requireNonNull(asyncExecutor, "asyncExecutor == null");
      return client.deleteAsync(fileName).thenApplyAsync(reply -> this, asyncExecutor);
    }

    Writer delete() throws IOException {
      client.delete(fileName);
      return this;
    }

    @Override
    public void close() throws IOException {
      client.close();
    }
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
