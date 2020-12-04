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
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.DataStreamTestUtils;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
  public void testFileStoreStream() throws Exception {
    final CLUSTER cluster = newCluster(NUM_PEERS);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    final RaftGroup raftGroup = cluster.getGroup();
    final Collection<RaftPeer> peers = raftGroup.getPeers();
    Assert.assertEquals(NUM_PEERS, peers.size());
    RaftPeer raftPeer = peers.iterator().next();

    final CheckedSupplier<FileStoreClient, IOException> newClient =
        () -> new FileStoreClient(cluster.getGroup(), getProperties(), raftPeer);
    // TODO: configurable buffer size
    final int bufferSize = 10_000;
    testSingleFile("foo", SizeInBytes.valueOf("2M"), bufferSize, newClient);

    cluster.shutdown();
  }

  private void testSingleFile(
      String path, SizeInBytes fileLength, int bufferSize, CheckedSupplier<FileStoreClient,
      IOException> newClient)
      throws Exception {
    LOG.info("runTestSingleFile with path={}, fileLength={}", path, fileLength);
    final int size = fileLength.getSizeInt();
    try (FileStoreClient client = newClient.get()) {
      final DataStreamOutput dataStreamOutput = client.getStreamOutput(path, size);
      final List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
      final List<Integer> sizes = new ArrayList<>();

      for(int offset = 0; offset < size; ) {
        final int remaining = size - offset;
        final int length = Math.min(remaining, bufferSize);
        final boolean close = length == remaining;

        LOG.trace("write {}, offset={}, length={}, close? {}",
            path, offset, length, close);
        final ByteBuffer bf = DataStreamTestUtils.initBuffer(0, length);
        futures.add(dataStreamOutput.writeAsync(bf, close));
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
        Assert.assertEquals(reply.getType(), i == futures.size() - 1 ? DataStreamPacketHeaderProto.Type.STREAM_DATA_SYNC : DataStreamPacketHeaderProto.Type.STREAM_DATA);
      }
    }
  }
}
