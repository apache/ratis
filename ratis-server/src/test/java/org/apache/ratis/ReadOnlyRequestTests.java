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
package org.apache.ratis;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ReadOnlyRequestTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {


  static final int NUM_SERVERS = 3;

  @Before
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        VersionedStateMachine.class, StateMachine.class);
  }

  @Test
  public void testReadIndex() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::testReadIndexImpl);
  }

  private void testReadIndexImpl(CLUSTER cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();
      try (final RaftClient client = cluster.createClient(leaderId, RetryPolicies.noRetry())) {
        for (int i = 0; i < 10; i++) {
          RaftClientReply reply =
              client.io().send(new RaftTestUtil.SimpleMessage("a=" + i));
          Assert.assertTrue(reply.isSuccess());
        }
        RaftClientReply reply = client.io().sendReadOnly(new RaftTestUtil.SimpleMessage("a"));
        Assert.assertEquals(reply.getMessage().getContent().toString(StandardCharsets.UTF_8), "9");
      }
    } finally {
      cluster.shutdown();
    }
  }

  static class VersionedStateMachine extends BaseStateMachine {
    private Map<String, Integer> data = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Message> query(Message request) {
      return CompletableFuture.completedFuture(
          Message.valueOf(Integer.toString(
              data.get(request.getContent().toString(StandardCharsets.UTF_8)))));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      System.out.println("index is " + trx.getLogEntry().getIndex());
      updateLastAppliedTermIndex(trx.getLogEntry().getTerm(), trx.getLogEntry().getIndex());

      String content = trx.getLogEntry().getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
      String[] kv = content.split("=");
      data.put(kv[0], Integer.parseInt(kv[1]));
      return CompletableFuture.completedFuture(
          Message.valueOf(trx.getLogEntry().getIndex() + " OK"));
    }
  }
}

