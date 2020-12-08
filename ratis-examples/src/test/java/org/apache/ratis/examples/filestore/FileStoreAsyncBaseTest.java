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
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public abstract class FileStoreAsyncBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  public static final Logger LOG = LoggerFactory.getLogger(FileStoreAsyncBaseTest.class);

  {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        FileStoreStateMachine.class, StateMachine.class);
    ConfUtils.setFile(p::setFile, FileStoreCommon.STATEMACHINE_DIR_KEY,
        new File(getClassTestDir(), "filestore"));
  }

  static final int NUM_PEERS = 3;

  @Test
  public void testFileStoreAsync() throws Exception {
    final CLUSTER cluster = newCluster(NUM_PEERS);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    final FileStoreClient client = new FileStoreClient(cluster.getGroup(), getProperties());
    final ExecutorService executor = Executors.newFixedThreadPool(20);

    testSingleFile("foo", SizeInBytes.valueOf("2M"), executor, client);
    testMultipleFiles("file", 20, SizeInBytes.valueOf("1M"), executor, client);

    executor.shutdown();
    client.close();
    cluster.shutdown();
  }

  private static void testSingleFile(
      String path, SizeInBytes fileLength, Executor executor, FileStoreClient client)
      throws Exception {
    LOG.info("runTestSingleFile with path={}, fileLength={}", path, fileLength);

    FileStoreWriter.newBuilder()
        .setFileName(path)
        .setFileSize(fileLength)
        .setAsyncExecutor(executor)
        .setFileStoreClientSupplier(() -> client)
        .build()
        .writeAsync(false)
        .thenCompose(FileStoreWriter::verifyAsync)
        .thenCompose(FileStoreWriter::deleteAsync)
        .get();
  }

  private static void testMultipleFiles(
      String pathPrefix, int numFile, SizeInBytes fileLength, Executor executor,
      FileStoreClient client) throws Exception {
    LOG.info("runTestMultipleFile with pathPrefix={}, numFile={}, fileLength={}",
        pathPrefix, numFile, fileLength);

    final List<CompletableFuture<FileStoreWriter>> writerFutures = new ArrayList<>();
    for (int i = 0; i < numFile; i++) {
      final String path = String.format("%s%02d", pathPrefix, i);
      final Callable<CompletableFuture<FileStoreWriter>> callable = LogUtils.newCallable(LOG,
          () -> FileStoreWriter.newBuilder()
              .setFileName(path)
              .setFileSize(fileLength)
              .setAsyncExecutor(executor)
              .setFileStoreClientSupplier(() -> client)
              .build()
              .writeAsync(false),
          () -> path + ":" + fileLength);
      writerFutures.add(callable.call());
    }

    final List<FileStoreWriter> writers = new ArrayList<>();
    for(CompletableFuture<FileStoreWriter> f : writerFutures) {
      writers.add(f.get());
    }

    writerFutures.clear();
    for (FileStoreWriter w : writers) {
      writerFutures.add(w.verifyAsync().thenCompose(FileStoreWriter::deleteAsync));
    }
    for(CompletableFuture<FileStoreWriter> f : writerFutures) {
      f.get();
    }
  }
}
