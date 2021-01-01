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
import org.apache.ratis.datastream.DataStreamTestUtils;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class FileStoreStreamingBaseTest <CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  public static final Logger LOG = LoggerFactory.getLogger(FileStoreStreamingBaseTest.class);

  {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        FileStoreStateMachine.class, StateMachine.class);
    ConfUtils.setFile(p::setFile, FileStoreCommon.STATEMACHINE_DIR_KEY,
        new File(getClassTestDir(), "filestore"));
  }

  static final int NUM_PEERS = 3;

  @Test
  public void testFileStoreStreamSingleFile() throws Exception {
    final CLUSTER cluster = newCluster(NUM_PEERS);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    final RaftGroup raftGroup = cluster.getGroup();
    final Collection<RaftPeer> peers = raftGroup.getPeers();
    Assert.assertEquals(NUM_PEERS, peers.size());
    RaftPeer primary = peers.iterator().next();

    final CheckedSupplier<FileStoreClient, IOException> newClient =
        () -> new FileStoreClient(cluster.getGroup(), getProperties(), primary);

    RoutingTable routingTable = DataStreamTestUtils.getRoutingTableChainTopology(peers, primary);
    testSingleFile("foo", SizeInBytes.valueOf("2M"), 10_000, newClient, routingTable);
    testSingleFile("bar", SizeInBytes.valueOf("2M"), 1000, newClient, routingTable);
    testSingleFile("sar", SizeInBytes.valueOf("20M"), 100_000, newClient, routingTable);

    cluster.shutdown();
  }

  @Test
  public void testFileStoreStreamMultipleFiles() throws Exception {
    final CLUSTER cluster = newCluster(NUM_PEERS);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    final RaftGroup raftGroup = cluster.getGroup();
    final Collection<RaftPeer> peers = raftGroup.getPeers();
    Assert.assertEquals(NUM_PEERS, peers.size());
    RaftPeer primary = peers.iterator().next();

    final CheckedSupplier<FileStoreClient, IOException> newClient =
        () -> new FileStoreClient(cluster.getGroup(), getProperties(), primary);

    RoutingTable routingTable = DataStreamTestUtils.getRoutingTableChainTopology(peers, primary);
    testMultipleFiles("foo", 5, SizeInBytes.valueOf("2M"), 10_000, newClient, routingTable);
    testMultipleFiles("bar", 10, SizeInBytes.valueOf("2M"), 1000, newClient, routingTable);

    cluster.shutdown();
  }

  private void testSingleFile(
      String path, SizeInBytes fileLength, int bufferSize, CheckedSupplier<FileStoreClient, IOException> newClient,
      RoutingTable routingTable)
      throws Exception {
    LOG.info("runTestSingleFile with path={}, fileLength={}", path, fileLength);
    FileStoreWriter.newBuilder()
        .setFileName(path)
        .setFileSize(fileLength)
        .setBufferSize(bufferSize)
        .setFileStoreClientSupplier(newClient)
        .build().streamWriteAndVerify(routingTable);
  }

  private void testMultipleFiles(String pathBase, int numFile, SizeInBytes fileLength,
      int bufferSize, CheckedSupplier<FileStoreClient, IOException> newClient,
      RoutingTable routingTable) throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(numFile);

    final List<Future<FileStoreWriter>> writerFutures = new ArrayList<>();
    for (int i = 0; i < numFile; i++) {
      String path =  pathBase + "-" + i;
      final Callable<FileStoreWriter> callable = LogUtils.newCallable(LOG,
          () -> FileStoreWriter.newBuilder()
              .setFileName(path)
              .setFileSize(fileLength)
              .setBufferSize(bufferSize)
              .setFileStoreClientSupplier(newClient)
              .build().streamWriteAndVerify(routingTable),
          () -> path);
      writerFutures.add(executor.submit(callable));
    }
    for (Future<FileStoreWriter> future : writerFutures) {
      future.get();
    }
  }
}
