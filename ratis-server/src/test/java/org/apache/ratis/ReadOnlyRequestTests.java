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
import org.apache.ratis.proto.RaftProtos;
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
import org.apache.ratis.util.TimeDuration;
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
          RaftClientReply reply = client.io().send(incrementMessage);
          Assertions.assertTrue(reply.isSuccess());
          reply = client.io().sendReadOnly(queryMessage);
          Assertions.assertEquals(i, retrieve(reply));
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

        CompletableFuture<RaftClientReply> result = client.async().send(incrementMessage);
        client.admin().transferLeadership(null, 200);

        Assertions.assertThrows(ReadIndexException.class, () -> {
          RaftClientReply timeoutReply = noRetry.io().sendReadOnly(queryMessage);
          Assertions.assertNotNull(timeoutReply.getException());
          Assertions.assertTrue(timeoutReply.getException() instanceof ReadException);
        });
      }

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testReadOnlyRetryWhenLeaderDown() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster -> runTestReadOnlyRetryWhenLeaderDown(null, cluster));
  }

  static <C extends MiniRaftCluster> void runTestReadOnlyRetryWhenLeaderDown(RetryPolicy retryPolicy, C cluster)
      throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

  private void testFollowerReadOnlyImpl(CLUSTER cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);

      List<RaftServer.Division> followers = cluster.getFollowers();
      Assertions.assertEquals(2, followers.size());

      final RaftPeerId f0 = followers.get(0).getId();
      final RaftPeerId f1 = followers.get(1).getId();
      try (RaftClient client = cluster.createClient(cluster.getLeader().getId())) {
        for (int i = 1; i <= 10; i++) {
          final RaftClientReply reply = client.io().send(incrementMessage);
          Assertions.assertTrue(reply.isSuccess());
          final RaftClientReply read1 = client.io().sendReadOnly(queryMessage, f0);
          Assertions.assertEquals(i, retrieve(read1));
          final CompletableFuture<RaftClientReply> read2 = client.async().sendReadOnly(queryMessage, f1);
          Assertions.assertEquals(i, retrieve(read2.get(1, TimeUnit.SECONDS)));
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testFollowerLinearizableReadParallel() throws Exception {
    getProperties().setEnum(RaftServerConfigKeys.Read.OPTION_KEY, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    runWithNewCluster(NUM_SERVERS, this::testFollowerReadOnlyParallelImpl);
  }

  @Test
  public void testFollowerLeaseReadParallel() throws Exception {
    getProperties().setBoolean(RaftServerConfigKeys.Read.LEADER_LEASE_ENABLED_KEY, true);
    runWithNewCluster(NUM_SERVERS, this::testFollowerReadOnlyParallelImpl);
  }

  private void testFollowerReadOnlyParallelImpl(CLUSTER cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);

      List<RaftServer.Division> followers = cluster.getFollowers();
      Assertions.assertEquals(2, followers.size());

      try (RaftClient leaderClient = cluster.createClient(cluster.getLeader().getId());
           RaftClient followerClient1 = cluster.createClient(followers.get(0).getId())) {

        leaderClient.io().send(incrementMessage);
        leaderClient.async().send(waitAndIncrementMessage);
        Thread.sleep(100);

        RaftClientReply clientReply = followerClient1.io().sendReadOnly(queryMessage, followers.get(0).getId());
        Assertions.assertEquals(2, retrieve(clientReply));
      }

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testFollowerLinearizableReadFailWhenLeaderDown() throws Exception {
    getProperties().setEnum(RaftServerConfigKeys.Read.OPTION_KEY, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    runWithNewCluster(NUM_SERVERS, this::testFollowerReadOnlyFailWhenLeaderDownImpl);
  }

  @Test
  public void testFollowerLeaseReadWhenLeaderDown() throws Exception {
    getProperties().setBoolean(RaftServerConfigKeys.Read.LEADER_LEASE_ENABLED_KEY, true);
    runWithNewCluster(NUM_SERVERS, this::testFollowerReadOnlyFailWhenLeaderDownImpl);
  }

  private void testFollowerReadOnlyFailWhenLeaderDownImpl(CLUSTER cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);

      List<RaftServer.Division> followers = cluster.getFollowers();
      Assertions.assertEquals(2, followers.size());

      try (RaftClient leaderClient = cluster.createClient(cluster.getLeader().getId());
           RaftClient followerClient1 = cluster.createClient(followers.get(0).getId(), RetryPolicies.noRetry())) {
         leaderClient.io().send(incrementMessage);

         RaftClientReply clientReply = followerClient1.io().sendReadOnly(queryMessage);
         Assertions.assertEquals(1, retrieve(clientReply));

         // kill the leader
         // read timeout quicker than election timeout
         leaderClient.admin().transferLeadership(null, 200);

         Assertions.assertThrows(ReadIndexException.class, () -> {
           followerClient1.io().sendReadOnly(queryMessage, followers.get(0).getId());
         });
      }

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testFollowerReadOnlyRetryWhenLeaderDown() throws Exception {
    getProperties().setEnum(RaftServerConfigKeys.Read.OPTION_KEY, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    runWithNewCluster(NUM_SERVERS, this::testFollowerReadOnlyRetryWhenLeaderDown);
  }

  @Test
  public void testFollowerLeaseReadRetryWhenLeaderDown() throws Exception {
    getProperties().setBoolean(RaftServerConfigKeys.Read.LEADER_LEASE_ENABLED_KEY, true);
    runWithNewCluster(NUM_SERVERS, this::testFollowerReadOnlyRetryWhenLeaderDown);
  }

  private void testFollowerReadOnlyRetryWhenLeaderDown(CLUSTER cluster) throws Exception {
    // only retry on readIndexException
    final RetryPolicy retryPolicy = ExceptionDependentRetry
        .newBuilder()
        .setDefaultPolicy(RetryPolicies.noRetry())
        .setExceptionToPolicy(ReadIndexException.class,
            RetryPolicies.retryForeverWithSleep(TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)))
        .build();

    RaftTestUtil.waitForLeader(cluster);

    try (RaftClient client = cluster.createClient(cluster.getLeader().getId(), retryPolicy)) {
      client.io().send(incrementMessage);

      final RaftClientReply clientReply = client.io().sendReadOnly(queryMessage);
      Assertions.assertEquals(1, retrieve(clientReply));

      // kill the leader
      client.admin().transferLeadership(null, 200);

      // readOnly will success after re-election
      final RaftClientReply replySuccess = client.io().sendReadOnly(queryMessage);
      Assertions.assertEquals(1, retrieve(clientReply));
    }
  }

  @Test
  public void testReadAfterWrite() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::testReadAfterWriteImpl);
  }

  private void testReadAfterWriteImpl(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    try (RaftClient client = cluster.createClient()) {
      // test blocking read-after-write
      client.io().send(incrementMessage);
      final RaftClientReply blockReply = client.io().sendReadAfterWrite(queryMessage);
      Assertions.assertEquals(1, retrieve(blockReply));

      // test asynchronous read-after-write
      client.async().send(incrementMessage);
      client.async().sendReadAfterWrite(queryMessage).thenAccept(reply -> {
        Assertions.assertEquals(2, retrieve(reply));
      });

      for (int i = 0; i < 20; i++) {
        client.async().send(incrementMessage);
      }
      final CompletableFuture<RaftClientReply> linearizable = client.async().sendReadOnly(queryMessage);
      final CompletableFuture<RaftClientReply> readAfterWrite = client.async().sendReadAfterWrite(queryMessage);

      CompletableFuture.allOf(linearizable, readAfterWrite).get();
      // read-after-write is more consistent than linearizable read
      Assertions.assertTrue(retrieve(readAfterWrite.get()) >= retrieve(linearizable.get()));
    }
  }

  static int retrieve(RaftClientReply reply) {
    Assertions.assertTrue(reply.isSuccess());
    return Integer.parseInt(reply.getMessage().getContent().toString(StandardCharsets.UTF_8));
  }

  public static void assertReplyExact(int expectedCount, RaftClientReply reply) {
    Assertions.assertTrue(reply.isSuccess());
    final int retrieved = retrieve(reply);
    Assertions.assertEquals(expectedCount, retrieved, () -> "reply=" + reply);
  }

  static void assertReplyAtLeast(int minCount, RaftClientReply reply) {
    Assertions.assertTrue(reply.isSuccess());
    final int retrieved = retrieve(reply);
    Assertions.assertTrue(retrieved >= minCount,
        () -> "retrieved = " + retrieved + " < minCount = " + minCount + ", reply=" + reply);
  }

  /**
   * CounterStateMachine support 3 operations
   * 1. increment
   * 2. get
   * 3. waitAndIncrement
   */
  public static class CounterStateMachine extends BaseStateMachine {
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

    public long getCount() {
      return counter.get();
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
      final RaftProtos.LogEntryProto logEntry = trx.getLogEntryUnsafe();
      LOG.debug("apply trx with index=" + logEntry.getIndex());
      updateLastAppliedTermIndex(logEntry.getTerm(), logEntry.getIndex());

      String command = logEntry.getStateMachineLogEntry()
          .getLogData().toString(StandardCharsets.UTF_8);

      final long updatedCount;
      if (command.equals(INCREMENT_STRING)) {
        updatedCount = increment();
      } else if (command.equals(WAIT_AND_INCREMENT_STRING)) {
        updatedCount = waitAndIncrement();
      } else {
        updatedCount = timeoutIncrement();
      }
      LOG.info("{}: Applied {} command {}, updatedCount={}", getId(), ti, command, updatedCount);

      return toMessageFuture(updatedCount);
    }
  }
}
