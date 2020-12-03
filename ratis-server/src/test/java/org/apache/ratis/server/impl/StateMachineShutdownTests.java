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

package org.apache.ratis.server.impl;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;


public abstract class StateMachineShutdownTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  protected static class StateMachineWithConditionalWait extends
      SimpleStateMachine4Testing {

    private final Long objectToWait = 0L;
    volatile boolean blockOnApply = true;

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      if (blockOnApply) {
        synchronized (objectToWait) {
          try {
            objectToWait.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException();
          }
        }
      }
      RaftProtos.LogEntryProto entry = trx.getLogEntry();
      updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      return CompletableFuture.completedFuture(new RaftTestUtil.SimpleMessage("done"));
    }

    public void unBlockApplyTxn() {
      blockOnApply = false;
      synchronized (objectToWait) {
        objectToWait.notifyAll();
      }
    }
  }

  @Test
  public void testStateMachineShutdownWaitsForApplyTxn() throws Exception {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        StateMachineWithConditionalWait.class, StateMachine.class);
    final MiniRaftCluster cluster = newCluster(3);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
    final RaftServer.Division leader = cluster.getLeader();
    RaftPeerId leaderId = leader.getId();

    //Unblock leader and one follower
    ((StateMachineWithConditionalWait)leader.getStateMachine())
        .unBlockApplyTxn();
    ((StateMachineWithConditionalWait)cluster.
        getFollowers().get(0).getStateMachine()).unBlockApplyTxn();

    cluster.getLeaderAndSendFirstMessage(true);

    try (final RaftClient client = cluster.createClient(leaderId)) {
      client.io().send(new RaftTestUtil.SimpleMessage("message"));
      RaftClientReply reply = client.io().send(
              new RaftTestUtil.SimpleMessage("message2"));

      long logIndex = reply.getLogIndex();
      //Confirm that followers have committed
      RaftClientReply watchReply = client.io().watch(
              logIndex, RaftProtos.ReplicationLevel.ALL_COMMITTED);
      watchReply.getCommitInfos().forEach(
              val -> Assert.assertTrue(val.getCommitIndex() >= logIndex));
      final RaftServer.Division secondFollower = cluster.getFollowers().get(1);
      // Second follower is blocked in apply transaction
      Assert.assertTrue(secondFollower.getInfo().getLastAppliedIndex() < logIndex);

      // Now shutdown the follower in a separate thread
      final Thread t = new Thread(secondFollower::close);
      t.start();

      // The second follower should still be blocked in apply transaction
      Assert.assertTrue(secondFollower.getInfo().getLastAppliedIndex() < logIndex);

      // Now unblock the second follower
      ((StateMachineWithConditionalWait) secondFollower.getStateMachine())
              .unBlockApplyTxn();

      // Now wait for the thread
      t.join(5000);
      Assert.assertEquals(logIndex, secondFollower.getInfo().getLastAppliedIndex());

      cluster.shutdown();
    }
  }
}
