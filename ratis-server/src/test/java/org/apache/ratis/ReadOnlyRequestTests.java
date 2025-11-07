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
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.Slf4jUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ReadOnlyRequestTests<CLUSTER extends MiniRaftCluster>
  extends BaseTest
  implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
  }

  static final int NUM_SERVERS = 3;

  static final String INCREMENT_STRING = "INCREMENT";
  static final String WAIT_AND_INCREMENT_STRING = "WAIT_AND_INCREMENT";
  static final String QUERY_STRING = "QUERY";

  static final Message INCREMENT = new RaftTestUtil.SimpleMessage(INCREMENT_STRING);
  static final Message WAIT_AND_INCREMENT = new RaftTestUtil.SimpleMessage(WAIT_AND_INCREMENT_STRING);
  static final Message QUERY = new RaftTestUtil.SimpleMessage(QUERY_STRING);

  @BeforeEach
  public void setup() {
    final RaftProperties p = getProperties();
    CounterStateMachine.setProperties(p);
  }

  public static void assertOption(RaftServerConfigKeys.Read.Option expected, RaftProperties properties) {
    final RaftServerConfigKeys.Read.Option computed = RaftServerConfigKeys.Read.option(properties);
    Assertions.assertEquals(expected, computed);
  }

  @Test
  public void testReadOnly() throws Exception {
    assertOption(RaftServerConfigKeys.Read.Option.DEFAULT,  getProperties());
    runWithNewCluster(NUM_SERVERS, ReadOnlyRequestTests::runTestReadOnly);
  }

  static <C extends MiniRaftCluster> void runTestReadOnly(C cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try (final RaftClient client = cluster.createClient(leaderId)) {
        for (int i = 1; i <= 10; i++) {
          assertReplyExact(i, client.io().send(INCREMENT));
          assertReplyExact(i, client.io().sendReadOnly(QUERY));
        }
      }
    } finally {
      cluster.shutdown();
    }
  }
  @Test
  public void testReadTimeout() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster -> runTestReadTimeout(RaftRetryFailureException.class, cluster));
  }

  static <C extends MiniRaftCluster> void runTestReadTimeout(Class<? extends Throwable> exceptionClass, C cluster)
      throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

    try (final RaftClient client = cluster.createClient(leaderId);
         final RaftClient noRetry = cluster.createClient(leaderId, RetryPolicies.noRetry())) {

      assertReplyExact(1, client.io().send(INCREMENT));
      client.admin().transferLeadership(null, 200);

      Assertions.assertThrows(exceptionClass, () -> {
        final RaftClientReply timeoutReply = noRetry.io().sendReadOnly(QUERY);
        Assertions.assertFalse(timeoutReply.isSuccess());
        Assertions.assertNotNull(timeoutReply.getException());
        Assertions.assertInstanceOf(ReadException.class, timeoutReply.getException());
      });
    }
  }

  @Test
  public void testReadOnlyRetryWhenLeaderDown() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster -> runTestReadOnlyRetryWhenLeaderDown(null, cluster));
  }

  static <C extends MiniRaftCluster> void runTestReadOnlyRetryWhenLeaderDown(RetryPolicy retryPolicy, C cluster)
      throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

    try (RaftClient client = cluster.createClient(leaderId, retryPolicy)) {
      assertReplyExact(1, client.io().send(INCREMENT));
      assertReplyExact(1, client.io().sendReadOnly(QUERY));

      // kill the leader
      client.admin().transferLeadership(null, 200);

      // readOnly will success after re-election
      assertReplyExact(1, client.io().sendReadOnly(QUERY));
    }
  }

  static int retrieve(RaftClientReply reply) {
    Assertions.assertTrue(reply.isSuccess());
    return Integer.parseInt(reply.getMessage().getContent().toString(StandardCharsets.UTF_8));
  }

  static void assertReplyExact(int expectedCount, RaftClientReply reply) {
    Assertions.assertTrue(reply.isSuccess());
    final int retrieved = retrieve(reply);
    Assertions.assertEquals(expectedCount, retrieved, () -> "reply=" + reply);
  }

  static void assertReplyAtLeast(int minCount, RaftClientReply reply) {
    Assertions.assertTrue(reply.isSuccess());
    final int retrieved = retrieve(reply);
    Assertions.assertTrue(retrieved >= minCount,
        () -> "retrieved = " + retrieved + " >= minCount = " + minCount + ", reply=" + reply);
  }

  /**
   * CounterStateMachine support 3 operations
   * 1. increment
   * 2. get
   * 3. waitAndIncrement
   */
  static class CounterStateMachine extends BaseStateMachine {
    static void setProperties(RaftProperties properties) {
      properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, CounterStateMachine.class, StateMachine.class);
    }

    private final AtomicLong counter = new AtomicLong(0L);

    @Override
    public CompletableFuture<Message> query(Message request) {
      return toMessageFuture(counter.get());
    }

    static CompletableFuture<Message> toMessageFuture(long count) {
      return CompletableFuture.completedFuture(Message.valueOf(String.valueOf(count)));
    }

    @Override
    public CompletableFuture<Message> queryStale(Message request, long minIndex) {
      return query(request);
    }

    private void sleepQuietly(int millis) {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException e) {
        LOG.debug("{} be interrupted", Thread.currentThread());
        Thread.currentThread().interrupt();
      }
    }

    private long increment() {
      return counter.incrementAndGet();
    }

    private long waitAndIncrement() {
      sleepQuietly(500);
      return increment();
    }

    private long timeoutIncrement() {
      sleepQuietly(5000);
      return increment();
    }


    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      final LogEntryProto logEntry = trx.getLogEntry();
      final TermIndex ti = TermIndex.valueOf(logEntry);
      updateLastAppliedTermIndex(ti);

      final String command = logEntry.getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);

      final long updatedCount;
      if (command.equals(INCREMENT_STRING)) {
        updatedCount = increment();
      } else if (command.equals(WAIT_AND_INCREMENT_STRING)) {
        updatedCount = waitAndIncrement();
      } else {
        updatedCount = timeoutIncrement();
      }
      LOG.info("Applied {} command {}, updatedCount={}", ti, command, updatedCount);

      return toMessageFuture(updatedCount);
    }
  }
}
