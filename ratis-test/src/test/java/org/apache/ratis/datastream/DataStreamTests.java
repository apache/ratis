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

import static org.apache.ratis.RaftTestUtil.waitForLeader;

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.DataStreamOutputRpc;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.datastream.DataStreamBaseTest.MultiDataStreamStateMachine;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

public abstract class DataStreamTests <CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    RaftConfigKeys.DataStream.setType(getProperties(), SupportedDataStreamType.NETTY);
    setStateMachine(MultiDataStreamStateMachine.class);
  }

  public static final int NUM_SERVERS = 3;
  // TODO: change bufferSize and bufferNum configurable
  private static int bufferSize = 1_000_000;
  private static int bufferNum =  10;

  @Test
  public void testStreamWrites() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::testStreamWrites);
  }

  void testStreamWrites(CLUSTER cluster) throws Exception {
    final RaftServerImpl leader = waitForLeader(cluster);

    final RaftGroup raftGroup = cluster.getGroup();
    final Collection<RaftPeer> peers = raftGroup.getPeers();
    Assert.assertEquals(NUM_SERVERS, peers.size());
    RaftPeer raftPeer = peers.iterator().next();

    try (RaftClient client = cluster.createClient(raftPeer)) {
      // send header
      DataStreamOutputRpc dataStreamOutputRpc = (DataStreamOutputRpc) client.getDataStreamApi().stream();

      // send data
      final int halfBufferSize = bufferSize / 2;
      int dataSize = 0;
      for(int i = 0; i < bufferNum; i++) {
        final int size = halfBufferSize + ThreadLocalRandom.current().nextInt(halfBufferSize);

        final ByteBuffer bf = DataStreamBaseTest.initBuffer(dataSize, size);
        dataStreamOutputRpc.writeAsync(bf);
        dataSize += size;
      }

      // send close
      dataStreamOutputRpc.closeAsync().join();

      // get request call id
      long callId = dataStreamOutputRpc.getHeaderFuture().get().getStreamId();

      // verify the write request is in the Raft log.
      RaftLog log = leader.getState().getLog();
      boolean transactionFound = false;
      for (TermIndex termIndex : log.getEntries(0, Long.MAX_VALUE)) {
        RaftProtos.LogEntryProto entryProto = log.get(termIndex.getIndex());
        if (entryProto.hasStateMachineLogEntry()) {
            StateMachineLogEntryProto stateMachineEntryProto = entryProto.getStateMachineLogEntry();
            if (stateMachineEntryProto.getCallId() == callId) {
              transactionFound = true;
              break;
            }
        }
      }
      Assert.assertTrue(transactionFound);
    }
  }
}
