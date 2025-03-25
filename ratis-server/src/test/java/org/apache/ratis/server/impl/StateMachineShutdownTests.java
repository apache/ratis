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
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public abstract class StateMachineShutdownTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  public static Logger LOG = LoggerFactory.getLogger(StateMachineUpdater.class);
  private static MockedStatic<CompletableFuture> mocked;
  protected static class StateMachineWithConditionalWait extends
      SimpleStateMachine4Testing {
    boolean unblockAllTxns = false;
    final Set<Long> blockTxns = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    public static Map<Long, Set<CompletableFuture<Message>>> futures = new ConcurrentHashMap<>();
    public static Map<RaftPeerId, AtomicLong> numTxns = new ConcurrentHashMap<>();
    private final Map<Long, Long> appliedTxns = new ConcurrentHashMap<>();

    private synchronized void updateTxns() {
      long appliedIndex = this.getLastAppliedTermIndex().getIndex() + 1;
      Long appliedTerm = null;
      while (appliedTxns.containsKey(appliedIndex)) {
        appliedTerm = appliedTxns.remove(appliedIndex);
        appliedIndex += 1;
      }
      if (appliedTerm != null) {
        updateLastAppliedTermIndex(appliedTerm, appliedIndex - 1);
      }
    }

    @Override
    public void notifyTermIndexUpdated(long term, long index) {
      appliedTxns.put(index, term);
      updateTxns();
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      final RaftProtos.LogEntryProto entry = trx.getLogEntry();

      CompletableFuture<Message> future = new CompletableFuture<>();
      futures.computeIfAbsent(Thread.currentThread().getId(), k -> new HashSet<>()).add(future);
      executor.submit(() -> {
        synchronized (blockTxns) {
          if (!unblockAllTxns) {
            blockTxns.add(entry.getIndex());
          }
          while (!unblockAllTxns && blockTxns.contains(entry.getIndex())) {
            try {
              blockTxns.wait(10000);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
        numTxns.computeIfAbsent(getId(), (k) -> new AtomicLong()).incrementAndGet();
        appliedTxns.put(entry.getIndex(), entry.getTerm());
        updateTxns();
        future.complete(new RaftTestUtil.SimpleMessage("done"));
      });
      return future;
    }

    public void unBlockApplyTxn(long txnId) {
      synchronized (blockTxns) {
        blockTxns.remove(txnId);
        blockTxns.notifyAll();
      }
    }

    public void unblockAllTxns() {
      unblockAllTxns = true;
      synchronized (blockTxns) {
        for (Long txnId : blockTxns) {
          blockTxns.remove(txnId);
        }
        blockTxns.notifyAll();
      }
    }
  }

  @Before
  public void setup() {
    mocked = Mockito.mockStatic(CompletableFuture.class, Mockito.CALLS_REAL_METHODS);
  }

  @After
  public void tearDownClass() {
    if (mocked != null) {
      mocked.close();
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
            .unblockAllTxns();
    ((StateMachineWithConditionalWait)cluster.
            getFollowers().get(0).getStateMachine()).unblockAllTxns();
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



      // Now unblock the second follower
      long minIndex = ((StateMachineWithConditionalWait) secondFollower.getStateMachine()).blockTxns.stream()
              .min(Comparator.naturalOrder()).get();
      Assert.assertEquals(2, StateMachineWithConditionalWait.numTxns.values().stream()
                      .filter(val -> val.get() == 3).count());
      // The second follower should still be blocked in apply transaction
      Assert.assertTrue(secondFollower.getInfo().getLastAppliedIndex() < minIndex);
      for (long index : ((StateMachineWithConditionalWait) secondFollower.getStateMachine()).blockTxns) {
        if (minIndex != index) {
          ((StateMachineWithConditionalWait) secondFollower.getStateMachine()).unBlockApplyTxn(index);
        }
      }
      Assert.assertEquals(2, StateMachineWithConditionalWait.numTxns.values().stream()
              .filter(val -> val.get() == 3).count());
      Assert.assertTrue(secondFollower.getInfo().getLastAppliedIndex() < minIndex);
      ((StateMachineWithConditionalWait) secondFollower.getStateMachine()).unBlockApplyTxn(minIndex);

      // Now wait for the thread
      t.join(5000);
      Assert.assertEquals(logIndex, secondFollower.getInfo().getLastAppliedIndex());
      Assert.assertEquals(3, StateMachineWithConditionalWait.numTxns.values().stream()
              .filter(val -> val.get() == 3).count());

      cluster.shutdown();
    }
  }
}
