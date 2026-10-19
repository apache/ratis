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
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class ReadOnlyRequestWithLongTimeoutTests<CLUSTER extends MiniRaftCluster>
  extends BaseTest
  implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
  }

  static final int NUM_SERVERS = 3;

  static final String INCREMENT = "INCREMENT";
  static final String WAIT_AND_INCREMENT = "WAIT_AND_INCREMENT";
  static final String TIMEOUT_INCREMENT = "TIMEOUT_INCREMENT";
  static final String QUERY = "QUERY";
  final Message incrementMessage = new RaftTestUtil.SimpleMessage(INCREMENT);
  final Message waitAndIncrementMessage = new RaftTestUtil.SimpleMessage(WAIT_AND_INCREMENT);
  final Message timeoutMessage = new RaftTestUtil.SimpleMessage(TIMEOUT_INCREMENT);
  final Message queryMessage = new RaftTestUtil.SimpleMessage(QUERY);

  @BeforeEach
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        ReadOnlyRequestTests.CounterStateMachine.class, StateMachine.class);

    RaftServerConfigKeys.Read.setOption(p, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    RaftServerConfigKeys.Read.setTimeout(p, TimeDuration.ONE_SECOND);
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(p, TimeDuration.valueOf(150, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(p, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMin(p, TimeDuration.valueOf(3, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(p, TimeDuration.valueOf(6, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setRequestTimeout(p, TimeDuration.valueOf(10, TimeUnit.SECONDS));
    RaftClientConfigKeys.Rpc.setRequestTimeout(p, TimeDuration.valueOf(10, TimeUnit.SECONDS));
  }

  @Test
  public void testLinearizableReadParallel() throws Exception {
    // Configure the state machine before creating the cluster so this test can control write application.
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        BlockingCounterStateMachine.class, StateMachine.class);
    runWithNewCluster(NUM_SERVERS, this::testLinearizableReadParallelImpl);
  }

  /** Holds a committed write before updating the counter, without relying on thread scheduling delays. */
  public static class BlockingCounterStateMachine extends ReadOnlyRequestTests.CounterStateMachine {
    // Signals that WAIT_AND_INCREMENT has reached state machine application.
    private final CountDownLatch writeStarted = new CountDownLatch(1);
    // Released by the test after checking the stale value and initiating the linearizable read.
    private final CountDownLatch allowWrite = new CountDownLatch(1);

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      if (WAIT_AND_INCREMENT.equals(trx.getLogEntry().getStateMachineLogEntry().getLogData().toStringUtf8())) {
        // Notify the test before blocking; the counter still contains the previous value.
        writeStarted.countDown();
        try {
          Assertions.assertTrue(allowWrite.await(10, TimeUnit.SECONDS), "Timed out waiting to release write");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      return super.applyTransaction(trx);
    }
  }

  private void testLinearizableReadParallelImpl(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();

    try (RaftClient client = cluster.createClient(leaderId, RetryPolicies.noRetry())) {
      final RaftClientReply reply = client.io().send(incrementMessage);
      Assertions.assertTrue(reply.isSuccess());

      final BlockingCounterStateMachine stateMachine =
          (BlockingCounterStateMachine) cluster.getLeader().getStateMachine();
      final CompletableFuture<RaftClientReply> write = client.async().send(waitAndIncrementMessage);
      final CompletableFuture<RaftClientReply> read;
      try {
        // Wait until the write is committed, but keep it unapplied while reading the stale value.
        Assertions.assertTrue(stateMachine.writeStarted.await(10, TimeUnit.SECONDS), "Write did not start");
        final RaftClientReply staleValueBefore = client.io().sendStaleRead(queryMessage, 0, leaderId);
        Assertions.assertEquals(1, ReadOnlyRequestTests.retrieve(staleValueBefore));
        // The write is already committed, so this read must include it even though it is not yet applied.
        read = client.async().sendReadOnly(queryMessage);
      } finally {
        // Also unblock application on assertion failure so cluster shutdown cannot be held up by this latch.
        stateMachine.allowWrite.countDown();
      }

      final RaftClientReply linearizableReadValue = read.get(10, TimeUnit.SECONDS);
      Assertions.assertEquals(2, ReadOnlyRequestTests.retrieve(linearizableReadValue));
      Assertions.assertTrue(write.get(10, TimeUnit.SECONDS).isSuccess());
    }
  }

  @Test
  public void testLinearizableReadTimeout() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::testLinearizableReadTimeoutImpl);
  }

  private void testLinearizableReadTimeoutImpl(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();

    try (RaftClient client = cluster.createClient(leaderId, RetryPolicies.noRetry())) {
      final RaftClientReply reply = client.io().send(incrementMessage);
      Assertions.assertTrue(reply.isSuccess());

      final CompletableFuture<RaftClientReply> asyncTimeoutReply = client.async().send(timeoutMessage);
      Thread.sleep(100);

      Assertions.assertThrows(ReadException.class, () -> {
        final RaftClientReply timeoutReply = client.io().sendReadOnly(queryMessage);
        Assertions.assertTrue(timeoutReply.getException().getCause() instanceof TimeoutIOException);
      });

      asyncTimeoutReply.join();
    }
  }
}
