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

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.io.netty.util.internal.ThreadLocalRandom;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.*;
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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    try (final Writer w = new Writer(path, fileLength, newClient)) {
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
          () -> new Writer(path, fileLength, newClient).write(),
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

    Writer(String fileName, SizeInBytes fileSize, CheckedSupplier<FileStoreClient, IOException> newClient)
        throws IOException {
      this.fileName = fileName;
      this.fileSize = fileSize;
      this.client = newClient.get();
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
        final int n = Math.min(remaining, buffer.length);
        final boolean close = n == remaining;

        final ByteBuffer b = randomBytes(n, r);

        LOG.trace("client write {}, offset={}", fileName, offset);
        final long written = client.write(fileName, offset, close, b);
        Assert.assertEquals(n, written);
        offset += written;
      }
      return this;
    }

    Writer verify() throws IOException {
      final Random r = new Random(seed);
      final int size = fileSize.getSizeInt();

      for(int offset = 0; offset < size; ) {
        final int remaining = size - offset;
        final int n = Math.min(remaining, buffer.length);

        final ByteString read = client.read(fileName, offset, n);
        Assert.assertEquals(n, read.size());

        final ByteBuffer b = randomBytes(n, r);

        assertBuffers(offset, n, b, read.asReadOnlyByteBuffer());
        offset += n;
      }
      return this;
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
          + "expected = " + StringUtils.bytes2HexString(expected) + "\n"
          + "computed = " + StringUtils.bytes2HexString(computed) + "\n", e);
      throw e;
    }
  }
}
