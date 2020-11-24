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
package org.apache.ratis.datastream;

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.datastream.DataStreamTestUtils.MultiDataStreamStateMachine;
import org.apache.ratis.datastream.DataStreamTestUtils.SingleDataStream;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public abstract class DataStreamClusterTests<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    setStateMachine(MultiDataStreamStateMachine.class);
  }

  public static final int NUM_SERVERS = 3;

  @Test
  public void testStreamWrites() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::testStreamWrites);
  }

  void testStreamWrites(CLUSTER cluster) throws Exception {
    waitForLeader(cluster);
    runTestDataStreamOutput(cluster);
    runTestTransferTo(cluster);
  }

  void runTestDataStreamOutput(CLUSTER cluster) throws Exception {
    final RaftClientRequest request;
    final CompletableFuture<RaftClientReply> reply;
    try (RaftClient client = cluster.createClient()) {
      try(final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi().stream()) {
        request = out.getHeader();
        reply = out.getRaftClientReplyFuture();

        // write using DataStreamOutput
        DataStreamTestUtils.writeAndAssertReplies(out, 1000, 10);
      }
    }

    watchOrSleep(cluster, reply.join().getLogIndex());
    assertLogEntry(cluster, request);
  }

  void runTestTransferTo(CLUSTER cluster) throws Exception {
    final int size = 4_000_000 + ThreadLocalRandom.current().nextInt(1_000_000);

    // create data file
    final File f = new File(getTestDir(), "a.txt");
    DataStreamTestUtils.createFile(f, size);

    final RaftClientRequest request;
    final CompletableFuture<RaftClientReply> reply;
    try (RaftClient client = cluster.createClient()) {
      try(final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi().stream()) {
        request = out.getHeader();
        reply = out.getRaftClientReplyFuture();

        // write using transferTo WritableByteChannel
        try(FileInputStream in = new FileInputStream(f)) {
          final long transferred = in.getChannel().transferTo(0, size, out.getWritableByteChannel());
          Assert.assertEquals(size, transferred);
        }
      }
    }

    watchOrSleep(cluster, reply.join().getLogIndex());
    assertLogEntry(cluster, request);
  }

  void watchOrSleep(CLUSTER cluster, long index) throws Exception {
    try (RaftClient client = cluster.createClient()) {
      client.async().watch(index, ReplicationLevel.ALL).join();
    } catch (UnsupportedOperationException e) {
      // the cluster does not support watch
      ONE_SECOND.sleep();
    }
  }

  void assertLogEntry(CLUSTER cluster, RaftClientRequest request) throws Exception {
    for (RaftServerProxy proxy : cluster.getServers()) {
      final RaftServerImpl impl = proxy.getImpl(cluster.getGroupId());
      final MultiDataStreamStateMachine stateMachine = (MultiDataStreamStateMachine) impl.getStateMachine();
      final SingleDataStream s = stateMachine.getSingleDataStream(request);
      Assert.assertFalse(s.getDataChannel().isOpen());
      DataStreamTestUtils.assertLogEntry(impl, s);
    }
  }
}
