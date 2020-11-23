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
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.DataStreamTestUtils.MultiDataStreamStateMachine;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public abstract class DataStreamClusterTests<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    setStateMachine(MultiDataStreamStateMachine.class);

    // Avoid changing leader
    RaftServerConfigKeys.Rpc.setTimeoutMin(getProperties(), TimeDuration.valueOf(2, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(getProperties(), TimeDuration.valueOf(3, TimeUnit.SECONDS));
  }

  public static final int NUM_SERVERS = 3;

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

    final ClientId clientId;
    final long callId;
    try (RaftClient client = cluster.createClient(raftPeer)) {
      clientId = client.getId();

      // send a stream request
      try(final DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi().stream()) {
        DataStreamTestUtils.writeAndAssertReplies(out, 1000, 10);
        callId = out.getHeader().getCallId();
      }
    }

    // verify the write request is in the Raft log.
    final RaftLog log = leader.getState().getLog();
    final LogEntryProto entry = findLogEntry(clientId, callId, log);
    LOG.info("entry={}", entry);
    Assert.assertNotNull(entry);
  }

  static LogEntryProto findLogEntry(ClientId clientId, long callId, RaftLog log) throws Exception {
    for (TermIndex termIndex : log.getEntries(0, Long.MAX_VALUE)) {
      final LogEntryProto entry = log.get(termIndex.getIndex());
      if (entry.hasStateMachineLogEntry()) {
        final StateMachineLogEntryProto stateMachineEntry = entry.getStateMachineLogEntry();
        if (stateMachineEntry.getCallId() == callId) {
          if (clientId.toByteString().equals(stateMachineEntry.getClientId())) {
            return entry;
          }
        }
      }
    }
    return null;
  }

}
