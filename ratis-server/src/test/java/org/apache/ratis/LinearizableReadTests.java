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
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.retry.ExceptionDependentRetry;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys.Read.ReadIndex.Type;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.ReadOnlyRequestTests.CounterStateMachine;
import static org.apache.ratis.ReadOnlyRequestTests.INCREMENT;
import static org.apache.ratis.ReadOnlyRequestTests.QUERY;
import static org.apache.ratis.ReadOnlyRequestTests.WAIT_AND_INCREMENT;
import static org.apache.ratis.ReadOnlyRequestTests.assertOption;
import static org.apache.ratis.ReadOnlyRequestTests.assertReplyAtLeast;
import static org.apache.ratis.ReadOnlyRequestTests.assertReplyExact;
import static org.apache.ratis.server.RaftServerConfigKeys.Read.Option.LINEARIZABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Test for the {@link RaftServerConfigKeys.Read.Option#LINEARIZABLE} feature. */
public abstract class LinearizableReadTests<CLUSTER extends MiniRaftCluster>
  extends BaseTest
  implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Slf4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public abstract boolean isLeaderLeaseEnabled();

  public abstract Type readIndexType();

  public final void assertRaftProperties(RaftProperties p) {
    assertOption(LINEARIZABLE, p);
    assertEquals(isLeaderLeaseEnabled(), RaftServerConfigKeys.Read.leaderLeaseEnabled(p));
    assertSame(readIndexType(), RaftServerConfigKeys.Read.ReadIndex.type(p));
  }

  protected void runWithNewCluster(CheckedConsumer<CLUSTER, Exception> testCase) throws Exception {
    runWithNewCluster(3, 0, true, cluster -> {
      assertRaftProperties(cluster.getProperties());
      testCase.accept(cluster);
    });
  }

  @BeforeEach
  public void setup() {
    final RaftProperties p = getProperties();
    CounterStateMachine.setProperties(p);
    RaftServerConfigKeys.Read.setOption(p, LINEARIZABLE);
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(p, isLeaderLeaseEnabled());
    RaftServerConfigKeys.Read.ReadIndex.setType(p, readIndexType());

    // Enable dummy request so linearizable-read tests exercise the default ordered-async bootstrap path.
    RaftClientConfigKeys.Async.Experimental.setSendDummyRequest(p, true);
  }

  @Test
  public void testLinearizableRead() throws Exception {
    runWithNewCluster(ReadOnlyRequestTests::runTestReadOnly);
  }

  @Test
  public void testLinearizableReadTimeout() throws Exception {
    runWithNewCluster(cluster -> ReadOnlyRequestTests.runTestReadTimeout(ReadIndexException.class, cluster));
  }

  @Test
  public void testFollowerLinearizableRead() throws Exception {
    runWithNewCluster(LinearizableReadTests::runTestFollowerLinearizableRead);
  }

  public static class Reply {
    private final int count;
    private final CompletableFuture<RaftClientReply> future;

    public Reply(int count, CompletableFuture<RaftClientReply> future) {
      this.count = count;
      this.future = future;
    }

    public boolean isDone() {
      return future.isDone();
    }

    public void assertExact() {
      assertReplyExact(count, future.join());
    }

    public void assertAtLeast() {
      assertReplyAtLeast(count, future.join());
    }

    @Override
    public String toString() {
      return "Reply{" +
          "count=" + count +
          ", reply=" + (isDone() ? future.join() : "pending") +
          '}';
    }
  }

  static <C extends MiniRaftCluster> void runTestFollowerLinearizableRead(C cluster) throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

    final List<RaftServer.Division> followers = cluster.getFollowers();
    Assertions.assertEquals(2, followers.size());

    final RaftPeerId f0 = followers.get(0).getId();
    final RaftPeerId f1 = followers.get(1).getId();

    final int n = 100;
    final List<Reply> f0Replies = new ArrayList<>(n);
    final List<Reply> f1Replies = new ArrayList<>(n);
    try (RaftClient client = cluster.createClient(leaderId);
         RaftClient c0 = cluster.createClient(f0);
         RaftClient c1 = cluster.createClient(f1);
         ) {
      for (int i = 0; i < n; i++) {
        final int count = i + 1;
        assertReplyExact(count, client.io().send(INCREMENT));

        f0Replies.add(new Reply(count, c0.async().sendReadOnly(QUERY, f0)));
        f1Replies.add(new Reply(count, c1.async().sendReadOnly(QUERY, f1)));
      }

      for (int i = 0; i < n; i++) {
        f0Replies.get(i).assertAtLeast();
        f1Replies.get(i).assertAtLeast();
      }
    }
  }

  @Test
  public void testFollowerLinearizableReadFailsWhenInstallingSnapshot() throws Exception {
    RaftServerConfigKeys.Read.setTimeout(getProperties(), TimeDuration.valueOf(200, TimeUnit.MILLISECONDS));
    try {
      runWithNewCluster(ReadOnlyRequestTests::runTestFollowerLinearizableReadFailsWhenInstallingSnapshot);
    } finally {
      RaftServerConfigKeys.Read.setTimeout(getProperties(), RaftServerConfigKeys.Read.TIMEOUT_DEFAULT);
    }
  }

  @Test
  public void testFollowerLinearizableReadParallel() throws Exception {
    runWithNewCluster(LinearizableReadTests::runTestFollowerReadOnlyParallel);
  }

  static <C extends MiniRaftCluster> void runTestFollowerReadOnlyParallel(C cluster) throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

    final List<RaftServer.Division> followers = cluster.getFollowers();
    Assertions.assertEquals(2, followers.size());
    final RaftPeerId f0 = followers.get(0).getId();
    final RaftPeerId f1 = followers.get(1).getId();

    try (RaftClient leaderClient = cluster.createClient(leaderId);
         RaftClient f0Client = cluster.createClient(f0);
         RaftClient f1Client = cluster.createClient(f1)) {

      final int n = 10;
      final List<Reply> writeReplies = new ArrayList<>(n);
      final List<Reply> f1Replies = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        int count = 2*i + 1;
        assertReplyExact(count, leaderClient.io().send(INCREMENT));

        count++;
        writeReplies.add(new Reply(count, leaderClient.async().send(WAIT_AND_INCREMENT)));
        // sleep to let the commitIndex/appliedIndex get updated.
        Thread.sleep(100);
        // WAIT_AND_INCREMENT will delay 500ms to update the count, the read must wait for it.
        assertReplyExact(count, f0Client.io().sendReadOnly(QUERY, f0));
        f1Replies.add(new Reply(count, f1Client.async().sendReadOnly(QUERY, f1)));
      }

      for (int i = 0; i < n; i++) {
        writeReplies.get(i).assertExact();
        f1Replies.get(i).assertAtLeast();
      }
    }
  }

  @Test
  public void testLinearizableReadFailWhenLeaderDown() throws Exception {
    runWithNewCluster(LinearizableReadTests::runTestLinearizableReadFailWhenLeaderDown);
  }

  static <C extends MiniRaftCluster> void runTestLinearizableReadFailWhenLeaderDown(C cluster) throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

    final List<RaftServer.Division> followers = cluster.getFollowers();
    assertEquals(2, followers.size());
    final RaftPeerId f0 = followers.get(0).getId();

    try (RaftClient leaderClient = cluster.createClient(leaderId);
         RaftClient f0Client = cluster.createClient(f0, RetryPolicies.noRetry())) {
      assertReplyExact(1, leaderClient.io().send(INCREMENT));
      assertReplyExact(1, f0Client.io().sendReadOnly(QUERY));

      // kill the leader
      // read timeout quicker than election timeout
      final RaftClientReply reply = leaderClient.admin().transferLeadership(null, 200);
      Assertions.assertTrue(reply.isSuccess());

      // client should fail and won't retry
      Assertions.assertThrows(ReadIndexException.class, () -> f0Client.io().sendReadOnly(QUERY, f0));
    }
  }

  @Test
  public void testFollowerReadOnlyRetryWhenLeaderDown() throws Exception {
    // only retry on ReadIndexException
    final RetryPolicy retryPolicy = ExceptionDependentRetry
        .newBuilder()
        .setDefaultPolicy(RetryPolicies.noRetry())
        .setExceptionToPolicy(ReadIndexException.class,
            RetryPolicies.retryForeverWithSleep(TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)))
        .build();

    runWithNewCluster(cluster -> ReadOnlyRequestTests.runTestReadOnlyRetryWhenLeaderDown(retryPolicy, cluster));
  }


  @Test
  public void testReadAfterWrite() throws Exception {
    runWithNewCluster(LinearizableReadTests::runTestReadAfterWrite);
  }

  static <C extends MiniRaftCluster> void runTestReadAfterWrite(C cluster) throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

    try (RaftClient client = cluster.createClient(leaderId)) {
      // test blocking read-after-write
      assertReplyExact(1, client.io().send(INCREMENT));
      assertReplyExact(1, client.io().sendReadAfterWrite(QUERY));

      // test asynchronous read-after-write
      final CompletableFuture<RaftClientReply> writeReply = client.async().send(INCREMENT);
      final CompletableFuture<RaftClientReply> asyncReply = client.async().sendReadAfterWrite(QUERY);

      final int n = 100;
      final List<Reply> writeReplies = new ArrayList<>(n);
      final List<Reply> readAfterWriteReplies = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        final int count = i + 3;
        writeReplies.add(new Reply(count, client.async().send(INCREMENT)));
        readAfterWriteReplies.add(new Reply(count, client.async().sendReadAfterWrite(QUERY)));
      }

      for (int i = 0; i < n; i++) {
        writeReplies.get(i).assertExact();
        readAfterWriteReplies.get(i).assertAtLeast();
      }

      assertReplyAtLeast(2, writeReply.join());
      assertReplyAtLeast(2, asyncReply.join());
    }
  }
}
