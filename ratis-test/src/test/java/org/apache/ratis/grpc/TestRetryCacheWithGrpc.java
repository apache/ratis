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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.RetryCacheTests;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RetryCacheTestUtil;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TestRetryCacheWithGrpc
    extends RetryCacheTests<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {
  {
    Slf4jUtils.setLogLevel(RetryCache.LOG, Level.TRACE);

    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  @Test
  public void testInvalidateRepliedCalls() throws Exception {
    runWithNewCluster(3, cluster -> new InvalidateRepliedCallsTest(cluster).run());
  }

  static long assertReply(RaftClientReply reply) {
    Assertions.assertTrue(reply.isSuccess());
    return reply.getCallId();
  }

  class InvalidateRepliedCallsTest {
    private final MiniRaftCluster cluster;
    private final RaftServer.Division leader;
    private final AtomicInteger count = new AtomicInteger();

    InvalidateRepliedCallsTest(MiniRaftCluster cluster) throws Exception {
      this.cluster = cluster;
      this.leader = RaftTestUtil.waitForLeader(cluster);
    }

    SimpleMessage nextMessage() {
      return new SimpleMessage("m" + count.incrementAndGet());
    }

    void assertRetryCacheEntry(RaftClient client, long callId, boolean exist) throws InterruptedException {
      assertRetryCacheEntry(client, callId, exist, false);
    }

    void assertRetryCacheEntry(RaftClient client, long callId, boolean exist, boolean eventually) throws InterruptedException {
      Supplier<RetryCache.Entry> lookup = () -> RetryCacheTestUtil.get(leader, client.getId(), callId);
      Consumer<RetryCache.Entry> assertion = exist ? Assertions::assertNotNull : Assertions::assertNull;
      if (eventually) {
        JavaUtils.attempt(() -> assertion.accept(lookup.get()), 100, TimeDuration.ONE_MILLISECOND,
            "retry cache entry", null);
      } else {
        assertion.accept(lookup.get());
      }
    }

    long send(RaftClient client, Long previousCallId) throws Exception {
      final RaftClientReply reply = client.io().send(nextMessage());
      final long callId = assertReply(reply);
      if (previousCallId != null) {
        // the previous should be invalidated.
        assertRetryCacheEntry(client, previousCallId, false);
      }
      // the current should exist.
      assertRetryCacheEntry(client, callId, true);
      return callId;
    }

    CompletableFuture<Long> sendAsync(RaftClient client) {
      return client.async().send(nextMessage())
              .thenApply(TestRetryCacheWithGrpc::assertReply);
    }

    CompletableFuture<Long> watch(long logIndex, RaftClient client) {
      return client.async().watch(logIndex, RaftProtos.ReplicationLevel.MAJORITY)
          .thenApply(TestRetryCacheWithGrpc::assertReply);
    }

    void run() throws Exception {
      try (RaftClient client = cluster.createClient()) {
        // test blocking io
        Long lastBlockingCall = null;
        for (int i = 0; i < 5; i++) {
          lastBlockingCall = send(client, lastBlockingCall);
        }
        final long lastBlockingCallId = lastBlockingCall;

        // test async
        final SimpleStateMachine4Testing stateMachine = SimpleStateMachine4Testing.get(leader);
        stateMachine.blockApplyTransaction();
        final List<CompletableFuture<Long>> asyncCalls = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
          // Since applyTransaction is blocked, the replied call id remains the same.
          asyncCalls.add(sendAsync(client));
        }
        // async call will invalidate blocking calls even if applyTransaction is blocked.
        assertRetryCacheEntry(client, lastBlockingCallId, false, true);

        ONE_SECOND.sleep();
        // No calls can be completed.
        for (CompletableFuture<Long> f : asyncCalls) {
          Assertions.assertFalse(f.isDone());
        }
        stateMachine.unblockApplyTransaction();
        // No calls can be invalidated.
        for (CompletableFuture<Long> f : asyncCalls) {
          assertRetryCacheEntry(client, f.join(), true);
        }

        // one more blocking call will invalidate all async calls
        final long oneMoreBlockingCall = send(client, null);
        LOG.info("oneMoreBlockingCall callId={}", oneMoreBlockingCall);
        assertRetryCacheEntry(client, oneMoreBlockingCall, true);
        for (CompletableFuture<Long> f : asyncCalls) {
          assertRetryCacheEntry(client, f.join(), false);
        }

        // watch call will invalidate blocking calls
        final long watchAsyncCall = watch(1, client).get();
        LOG.info("watchAsyncCall callId={}", watchAsyncCall);
        assertRetryCacheEntry(client, oneMoreBlockingCall, false);
        // retry cache should not contain watch calls
        assertRetryCacheEntry(client, watchAsyncCall, false);
      }
    }
  }

  @Test
  @Timeout(value = 10000)
  public void testRetryOnResourceUnavailableException()
      throws InterruptedException, IOException {
    RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Write.setElementLimit(properties, 1);
    MiniRaftClusterWithGrpc cluster = getFactory().newCluster(NUM_SERVERS, properties);
    cluster.start();

    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftServer leaderProxy = leader.getRaftServer();
    for (RaftServer.Division follower : cluster.getFollowers()) {
      // block followers to trigger ResourceUnavailableException
      ((SimpleStateMachine4Testing) follower.getStateMachine()).blockWriteStateMachineData();
    }
    AtomicBoolean failure = new AtomicBoolean(false);
    long callId = 1;
    ClientId clientId = ClientId.randomId();
    RaftClientRequest r = null;
    while (!failure.get()) {
      long cid = callId;
      r = cluster.newRaftClientRequest(clientId, leaderProxy.getId(), callId++,
          new RaftTestUtil.SimpleMessage("message"));
      CompletableFuture<RaftClientReply> f = leaderProxy.submitClientRequestAsync(r);
      f.exceptionally(e -> {
        if (e.getCause() instanceof ResourceUnavailableException) {
          RetryCacheTestUtil.isFailed(RetryCacheTestUtil.get(leader, clientId, cid));
          failure.set(true);
        }
        return null;
      });
    }
    for (RaftServer.Division follower : cluster.getFollowers()) {
      // unblock followers
      ((SimpleStateMachine4Testing)follower.getStateMachine()).unblockWriteStateMachineData();
    }

    while (failure.get()) {
      try {
        // retry until the request failed with ResourceUnavailableException succeeds.
        RaftClientReply reply = leaderProxy.submitClientRequestAsync(r).get();
        if (reply.isSuccess()) {
          failure.set(false);
        }
      } catch (Exception e) {
        // Ignore the exception
      }
    }
    cluster.shutdown();
  }
}
