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
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.ReadReplyProto;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

  FileStoreClient newFileStoreClient(CLUSTER cluster) throws IOException {
    return new FileStoreClient(cluster.getGroup(), getProperties());
  }

  @Test
  public void testWatch() throws Exception {
    runWithNewCluster(NUM_PEERS, cluster -> runTestWatch(10, cluster));
  }

  void runTestWatch(int n, CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);

    final AtomicBoolean isStarted = new AtomicBoolean();
    final List<Integer> randomIndices = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      randomIndices.add(i);
    }
    Collections.shuffle(randomIndices);
    LOG.info("randomIndices {}", randomIndices);
    final List<Integer> completionOrder = new ArrayList<>();

    final String pathFirst = "first";
    final String pathSecond = "second";
    final List<CompletableFuture<ReadReplyProto>> firstList = new ArrayList<>(n);
    final List<CompletableFuture<ReadReplyProto>> watchSecond = new ArrayList<>(n);
    try (FileStoreClient client = new FileStoreClient(cluster.getGroup(), getProperties())) {
      for (int i = 0; i < n; i++) {
        LOG.info("watchAsync {}", i);
        final int index = i;
        final CompletableFuture<ReadReplyProto> f = client.watchAsync(pathFirst + i).whenComplete((reply, e) -> {
          throw new IllegalStateException(pathFirst + index + " should never be completed.");
        });
        firstList.add(f);
        final CompletableFuture<ReadReplyProto> s = client.watchAsync(pathSecond + i).whenComplete((reply, e) -> {
          Assert.assertNotNull(reply);
          Assert.assertNull(e);
          Assert.assertTrue(isStarted.get());
          completionOrder.add(index);
        });
        watchSecond.add(s);
        Assert.assertFalse(f.isDone());
        Assert.assertFalse(s.isDone());
        Assert.assertFalse(isStarted.get());
      }

      TimeDuration.valueOf(ThreadLocalRandom.current().nextLong(500) + 100, TimeUnit.MILLISECONDS)
          .sleep(s -> LOG.info("{}", s));
      firstList.stream().map(CompletableFuture::isDone).forEach(Assert::assertFalse);
      watchSecond.stream().map(CompletableFuture::isDone).forEach(Assert::assertFalse);
      Assert.assertFalse(isStarted.get());
      isStarted.set(true);

      for (int i : randomIndices) {
        writeSingleFile(pathSecond + i, SizeInBytes.ONE_KB, () -> client);
      }

      for (int i = 0; i < n; i++) {
        final ReadReplyProto reply = watchSecond.get(i).get(100, TimeUnit.MILLISECONDS);
        LOG.info("reply {}: {}", i, reply);
        Assert.assertNotNull(reply);
        Assert.assertEquals(pathSecond + i, reply.getResolvedPath().toStringUtf8());
      }
      LOG.info("completionOrder {}", completionOrder);
      Assert.assertEquals(randomIndices, completionOrder);
      firstList.stream().map(CompletableFuture::isDone).forEach(Assert::assertFalse);
    }
  }

  @Test
  public void testFileStore() throws Exception {
    final CLUSTER cluster = newCluster(NUM_PEERS);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    final CheckedSupplier<FileStoreClient, IOException> newClient = () -> newFileStoreClient(cluster);

    testSingleFile("foo", SizeInBytes.valueOf("2M"), newClient);
    testMultipleFiles("file", 20, SizeInBytes.valueOf("1M"), newClient);

    cluster.shutdown();
  }

  private static FileStoreWriter writeSingleFile(
      String path, SizeInBytes fileLength, CheckedSupplier<FileStoreClient, IOException> newClient)
      throws Exception {
    return FileStoreWriter.newBuilder()
        .setFileName(path)
        .setFileSize(fileLength)
        .setFileStoreClientSupplier(newClient)
        .build()
        .write(false)
        .verify()
        .delete();
  }

  private static void testSingleFile(
      String path, SizeInBytes fileLength, CheckedSupplier<FileStoreClient, IOException> newClient)
      throws Exception {
    LOG.info("runTestSingleFile with path={}, fileLength={}", path, fileLength);
    writeSingleFile(path, fileLength, newClient).close();
  }

  private static void testMultipleFiles(
      String pathPrefix, int numFile, SizeInBytes fileLength,
      CheckedSupplier<FileStoreClient, IOException> newClient) throws Exception {
    LOG.info("runTestMultipleFile with pathPrefix={}, numFile={}, fileLength={}",
        pathPrefix, numFile, fileLength);

    final ExecutorService executor = Executors.newFixedThreadPool(20);

    final List<Future<FileStoreWriter>> writerFutures = new ArrayList<>();
    for (int i = 0; i < numFile; i++) {
      final String path = String.format("%s%02d", pathPrefix, i);
      final Callable<FileStoreWriter> callable = LogUtils.newCallable(LOG,
          () -> FileStoreWriter.newBuilder()
              .setFileName(path)
              .setFileSize(fileLength)
              .setFileStoreClientSupplier(newClient)
              .build().write(false),
          () -> path + ":" + fileLength);
      writerFutures.add(executor.submit(callable));
    }

    final List<FileStoreWriter> writers = new ArrayList<>();
    for(Future<FileStoreWriter> f : writerFutures) {
      writers.add(f.get());
    }

    writerFutures.clear();
    for (FileStoreWriter w : writers) {
      writerFutures.add(executor.submit(() -> w.verify().delete()));
    }
    for(Future<FileStoreWriter> f : writerFutures) {
      f.get().close();
    }

    executor.shutdown();
  }
}
