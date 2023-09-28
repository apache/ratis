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
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.retry.ExceptionDependentRetry;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ReadOnlyRequestTests<CLUSTER extends MiniRaftCluster>
  extends BaseTest
  implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
  }

  static final int NUM_SERVERS = 3;

  static final String INCREMENT = "INCREMENT";
  static final String WAIT_AND_INCREMENT = "WAIT_AND_INCREMENT";
  static final String QUERY = "QUERY";
  final Message incrementMessage = new RaftTestUtil.SimpleMessage(INCREMENT);
  final Message waitAndIncrementMessage = new RaftTestUtil.SimpleMessage(WAIT_AND_INCREMENT);
  final Message queryMessage = new RaftTestUtil.SimpleMessage(QUERY);

  @Before
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        CounterStateMachine.class, StateMachine.class);
  }

  @Test
  public void testLinearizableRead() throws Exception {
    getProperties().setEnum(RaftServerConfigKeys.Read.OPTION_KEY, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    runWithNewCluster(NUM_SERVERS, this::testReadOnlyImpl);
  }

  @Test
  public void testLeaseRead() throws Exception {
    getProperties().setBoolean(RaftServerConfigKeys.Read.LEADER_LEASE_ENABLED_KEY, true);
    runWithNewCluster(NUM_SERVERS, this::testReadOnlyImpl);
  }

  private void testReadOnlyImpl(CLUSTER cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try (final RaftClient client = cluster.createClient(leaderId)) {
        for (int i = 1; i <= 10; i++) {
          RaftClientReply reply = client.io().send(incrementMessage);
          Assert.assertTrue(reply.isSuccess());
          reply = client.io().sendReadOnly(queryMessage);
          Assert.assertEquals(i, retrieve(reply));
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testLinearizableReadTimeout() throws Exception {
    getProperties().setEnum(RaftServerConfigKeys.Read.OPTION_KEY, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    runWithNewCluster(NUM_SERVERS, this::testReadOnlyTimeoutImpl);
  }

  @Test
  public void testLeaseReadTimeout() throws Exception {
    getProperties().setBoolean(RaftServerConfigKeys.Read.LEADER_LEASE_ENABLED_KEY, true);
    runWithNewCluster(NUM_SERVERS, this::testReadOnlyTimeoutImpl);
  }

  private void testReadOnlyTimeoutImpl(CLUSTER cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try (final RaftClient client = cluster.createClient(leaderId);
           final RaftClient noRetry = cluster.createClient(leaderId, RetryPolicies.noRetry())) {

        CompletableFuture<RaftClientReply> result = client.async().send(incrementMessage);
        client.admin().transferLeadership(null, 200);

        Assert.assertThrows(ReadIndexException.class, () -> {
          RaftClientReply timeoutReply = noRetry.io().sendReadOnly(queryMessage);
          Assert.assertNotNull(timeoutReply.getException());
          Assert.assertTrue(timeoutReply.getException() instanceof ReadException);
        });
      }

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testFollowerLinearizableRead() throws Exception {
    getProperties().setEnum(RaftServerConfigKeys.Read.OPTION_KEY, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    runWithNewCluster(NUM_SERVERS, this::testFollowerReadOnlyImpl);
  }

  @Test
  public void testFollowerLeaseRead() throws Exception {
    getProperties().setBoolean(RaftServerConfigKeys.Read.LEADER_LEASE_ENABLED_KEY, true);
    runWithNewCluster(NUM_SERVERS, this::testFollowerReadOnlyImpl);
  }

  private void testFollowerReadOnlyImpl(CLUSTER cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);

      List<RaftServer.Division> followers = cluster.getFollowers();
      Assert.assertEquals(2, followers.size());

      final RaftPeerId f0 = followers.get(0).getId();
      final RaftPeerId f1 = followers.get(1).getId();
      try (RaftClient client = cluster.createClient(cluster.getLeader().getId())) {
        for (int i = 1; i <= 10; i++) {
          final RaftClientReply reply = client.io().send(incrementMessage);
          Assert.assertTrue(reply.isSuccess());
          final RaftClientReply read1 = client.io().sendReadOnly(queryMessage, f0);
          Assert.assertEquals(i, retrieve(read1));
          final CompletableFuture<RaftClientReply> read2 = client.async().sendReadOnly(queryMessage, f1);
          Assert.assertEquals(i, retrieve(read2.get(1, TimeUnit.SECONDS)));
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
      Assert.assertEquals(2, followers.size());

      try (RaftClient leaderClient = cluster.createClient(cluster.getLeader().getId());
           RaftClient followerClient1 = cluster.createClient(followers.get(0).getId())) {

        leaderClient.io().send(incrementMessage);
        leaderClient.async().send(waitAndIncrementMessage);
        Thread.sleep(100);

        RaftClientReply clientReply = followerClient1.io().sendReadOnly(queryMessage, followers.get(0).getId());
        Assert.assertEquals(2, retrieve(clientReply));
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
      Assert.assertEquals(2, followers.size());

      try (RaftClient leaderClient = cluster.createClient(cluster.getLeader().getId());
           RaftClient followerClient1 = cluster.createClient(followers.get(0).getId(), RetryPolicies.noRetry())) {
         leaderClient.io().send(incrementMessage);

         RaftClientReply clientReply = followerClient1.io().sendReadOnly(queryMessage);
         Assert.assertEquals(1, retrieve(clientReply));

         // kill the leader
         // read timeout quicker than election timeout
         leaderClient.admin().transferLeadership(null, 200);

         Assert.assertThrows(ReadIndexException.class, () -> {
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
      Assert.assertEquals(1, retrieve(clientReply));

      // kill the leader
      client.admin().transferLeadership(null, 200);

      // readOnly will success after re-election
      final RaftClientReply replySuccess = client.io().sendReadOnly(queryMessage);
      Assert.assertEquals(1, retrieve(clientReply));
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
      Assert.assertEquals(1, retrieve(blockReply));

      // test asynchronous read-after-write
      client.async().send(incrementMessage);
      client.async().sendReadAfterWrite(queryMessage).thenAccept(reply -> {
        Assert.assertEquals(2, retrieve(reply));
      });

      for (int i = 0; i < 20; i++) {
        client.async().send(incrementMessage);
      }
      final CompletableFuture<RaftClientReply> linearizable = client.async().sendReadOnly(queryMessage);
      final CompletableFuture<RaftClientReply> readAfterWrite = client.async().sendReadAfterWrite(queryMessage);

      CompletableFuture.allOf(linearizable, readAfterWrite).get();
      // read-after-write is more consistent than linearizable read
      Assert.assertTrue(retrieve(readAfterWrite.get()) >= retrieve(linearizable.get()));
    }
  }

  static int retrieve(RaftClientReply reply) {
    return Integer.parseInt(reply.getMessage().getContent().toString(StandardCharsets.UTF_8));
  }


  /**
   * CounterStateMachine support 3 operations
   * 1. increment
   * 2. get
   * 3. waitAndIncrement
   */
  static class CounterStateMachine extends BaseStateMachine {
    private final AtomicLong counter = new AtomicLong(0L);

    @Override
    public CompletableFuture<Message> query(Message request) {
      return CompletableFuture.completedFuture(
          Message.valueOf(String.valueOf(counter.get())));
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

    private void increment() {
      counter.incrementAndGet();
    }

    private void waitAndIncrement() {
      sleepQuietly(500);
      increment();
    }

    private void timeoutIncrement() {
      sleepQuietly(5000);
      increment();
    }


    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      LOG.debug("apply trx with index=" + trx.getLogEntry().getIndex());
      updateLastAppliedTermIndex(trx.getLogEntry().getTerm(), trx.getLogEntry().getIndex());

      String command = trx.getLogEntry().getStateMachineLogEntry()
          .getLogData().toString(StandardCharsets.UTF_8);

      LOG.info("receive command: {}", command);
      if (command.equals(INCREMENT)) {
        increment();
      } else if (command.equals(WAIT_AND_INCREMENT)) {
        waitAndIncrement();
      } else {
        timeoutIncrement();
      }

      return CompletableFuture.completedFuture(Message.valueOf("OK"));
    }
  }
}
