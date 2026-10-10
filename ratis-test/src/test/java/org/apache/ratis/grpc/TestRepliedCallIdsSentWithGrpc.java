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
package org.apache.ratis.grpc;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.RaftClientImpl;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;

/**
 * Regression test for RATIS-2729: read-only {@code watch} RPCs must not leave permanent entries in
 * {@link RaftClientImpl.RepliedCallIds#sent}.
 */
public class TestRepliedCallIdsSentWithGrpc extends BaseTest
    implements MiniRaftClusterWithGrpc.FactoryGet {

  private static final int ITERATIONS = 100;
  private static final int NUM_SERVERS = 3;

  {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  @Test
  public void testReadOnlyWatchDoesNotGrowRepliedCallIdsSent() throws Exception {
    try (MiniRaftCluster cluster = newCluster(NUM_SERVERS)) {
      cluster.start();
      try (RaftClient client = cluster.createClient()) {
        RaftClientReply first = client.io().send(new SimpleMessage("bootstrap"));
        client.async().watch(first.getLogIndex(), ReplicationLevel.MAJORITY_COMMITTED).get();
        int baseline = repliedCallIdsSentSize(client);

        for (int i = 0; i < ITERATIONS; i++) {
          RaftClientReply write = client.io().send(new SimpleMessage("payload-" + i));
          RaftClientReply watch = client.async()
              .watch(write.getLogIndex(), ReplicationLevel.MAJORITY_COMMITTED).get();
          Assertions.assertTrue(watch.isSuccess(), "watch at iteration " + i);
        }

        int after = repliedCallIdsSentSize(client);
        Assertions.assertTrue(after <= baseline + 2,
            () -> String.format(
                "RepliedCallIds#sent should stay near baseline (%d) after read-only watches, but was %d",
                baseline, after));
      }
    }
  }

  private static int repliedCallIdsSentSize(RaftClient raftClient) throws Exception {
    RaftClientImpl impl = (RaftClientImpl) raftClient;
    Field repliedCallIdsField = RaftClientImpl.class.getDeclaredField("repliedCallIds");
    repliedCallIdsField.setAccessible(true);
    Object repliedCallIds = repliedCallIdsField.get(impl);
    Field sentField = repliedCallIds.getClass().getDeclaredField("sent");
    sentField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentMap<Long, ?> sent = (ConcurrentMap<Long, ?>) sentField.get(repliedCallIds);
    return sent.size();
  }
}
