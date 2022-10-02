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

import org.apache.log4j.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.Log4jUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ReadOnlyRequestTests<CLUSTER extends MiniRaftCluster>
  extends BaseTest
  implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
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

    p.setEnum(RaftServerConfigKeys.Read.OPTION_KEY, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
  }

  @Test
  public void testLinearizableRead() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::testLinearizableReadImpl);
  }

  private void testLinearizableReadImpl(CLUSTER cluster) throws Exception {
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
    runWithNewCluster(NUM_SERVERS, this::testLinearizableReadTimeoutImpl);
  }

  private void testLinearizableReadTimeoutImpl(CLUSTER cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try (final RaftClient client = cluster.createClient(leaderId);
           final RaftClient noRetry = cluster.createClient(leaderId, RetryPolicies.noRetry())) {

        CompletableFuture<RaftClientReply> result = client.async().send(incrementMessage);
        client.admin().transferLeadership(null, 200);

        Assert.assertThrows(ReadException.class, () -> {
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
    runWithNewCluster(NUM_SERVERS, this::testFollowerLinearizableReadImpl);
  }

  private void testFollowerLinearizableReadImpl(CLUSTER cluster) throws Exception {
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
    runWithNewCluster(NUM_SERVERS, this::testFollowerLinearizableReadParallelImpl);
  }

  private void testFollowerLinearizableReadParallelImpl(CLUSTER cluster) throws Exception {
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
    runWithNewCluster(NUM_SERVERS, this::testFollowerLinearizableReadFailWhenLeaderDownImpl);
  }

  private void testFollowerLinearizableReadFailWhenLeaderDownImpl(CLUSTER cluster) throws Exception {
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

         Assert.assertThrows(ReadException.class, () -> {
           followerClient1.io().sendReadOnly(queryMessage, followers.get(0).getId());
         });
      }

    } finally {
      cluster.shutdown();
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
      sleepQuietly(1500);
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
