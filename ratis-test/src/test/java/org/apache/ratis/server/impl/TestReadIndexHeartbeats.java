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

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.leader.LogAppender;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/** Test that {@link ReadIndexHeartbeats} confirms a read only with requests sent after the read arrived. */
public class TestReadIndexHeartbeats {
  /** A follower's appender: the test sends its requests and delivers their replies. */
  static class Follower {
    private final RaftPeerId id;
    private final AtomicLong nextCallId = new AtomicLong();
    private final AtomicInteger heartbeatTriggers = new AtomicInteger();
    private final LogAppender appender;

    Follower(String name) {
      this.id = RaftPeerId.valueOf(name);
      this.appender = (LogAppender) Proxy.newProxyInstance(LogAppender.class.getClassLoader(),
          new Class<?>[]{LogAppender.class}, (proxy, method, args) -> {
            switch (method.getName()) {
              case "getFollowerId": return id;
              case "getCallId": return nextCallId.get();
              case "getCallIdComparator": return Comparator.<Long>naturalOrder();
              case "triggerHeartbeat": heartbeatTriggers.incrementAndGet(); return null;
              case "hashCode": return System.identityHashCode(proxy);
              case "equals": return proxy == args[0];
              case "toString": return "appender-" + id;
              default: throw new UnsupportedOperationException(method.getName());
            }
          });
    }

    /** @return the call id of a request sent now. */
    long send() {
      return nextCallId.getAndIncrement();
    }

    void reply(ReadIndexHeartbeats heartbeats, long callId, boolean success, Predicate<Predicate<RaftPeerId>> majority) {
      heartbeats.onAppendEntriesReply(appender, AppendEntriesReplyProto.newBuilder()
          .setServerReply(RaftRpcReplyProto.newBuilder().setSuccess(success).setCallId(callId))
          .build(), majority);
    }
  }

  private final Follower f1 = new Follower("f1");
  private final Follower f2 = new Follower("f2");
  private final List<LogAppender> appenders = Arrays.asList(f1.appender, f2.appender);
  /** A group of three: the leader and one follower are a majority. */
  private final Predicate<Predicate<RaftPeerId>> majority = isAcked -> isAcked.test(f1.id) || isAcked.test(f2.id);
  private final ReadIndexHeartbeats heartbeats = new ReadIndexHeartbeats();

  private void reply(Follower follower, long callId) {
    follower.reply(heartbeats, callId, true, majority);
  }

  @Test
  public void testAReadIsConfirmedOnlyByARequestSentAfterItArrived() {
    final long sentBefore = f1.send();
    final CompletableFuture<Void> read = heartbeats.confirm(appenders);
    Assertions.assertEquals(1, f1.heartbeatTriggers.get());
    Assertions.assertEquals(1, f2.heartbeatTriggers.get());

    reply(f1, sentBefore);
    Assertions.assertFalse(read.isDone(), "confirmed by a request sent before the read arrived");

    f1.reply(heartbeats, f1.send(), false, majority);
    Assertions.assertFalse(read.isDone(), "confirmed by a follower that refused the request");

    reply(f1, f1.send());
    Assertions.assertTrue(read.isDone());
    Assertions.assertFalse(read.isCompletedExceptionally());
  }

  /** The reuse of 3.3.1: once a commit index was acknowledged, later reads at that index were answered at once. */
  @Test
  public void testAFinishedRoundConfirmsNoLaterRead() {
    final CompletableFuture<Void> first = heartbeats.confirm(appenders);
    final long callId = f1.send();
    reply(f1, callId);
    Assertions.assertTrue(first.isDone());

    final CompletableFuture<Void> second = heartbeats.confirm(appenders);
    Assertions.assertFalse(second.isDone(), "confirmed without a new round");
    Assertions.assertEquals(2, f1.heartbeatTriggers.get());
    // The reply that confirmed the first read, delivered again, and a late one from the other follower.
    reply(f1, callId);
    Assertions.assertFalse(second.isDone(), "confirmed by the request of the round before");
    reply(f2, f2.send());
    Assertions.assertTrue(second.isDone());
  }

  @Test
  public void testAReadDoesNotJoinARoundThatStarted() {
    final CompletableFuture<Void> r1 = heartbeats.confirm(appenders);
    final long afterR1 = f1.send();
    final CompletableFuture<Void> r2 = heartbeats.confirm(appenders);
    final long afterR2 = f1.send();
    // Two rounds are started: the reads that follow share a third one, which starts when one of them ends.
    final CompletableFuture<Void> r3 = heartbeats.confirm(appenders);
    final CompletableFuture<Void> r4 = heartbeats.confirm(appenders);
    Assertions.assertSame(r3, r4);
    Assertions.assertEquals(2, f1.heartbeatTriggers.get());
    final long afterR3 = f1.send();

    reply(f1, afterR1);
    Assertions.assertTrue(r1.isDone());
    Assertions.assertFalse(r2.isDone(), "confirmed by a request sent before the read arrived");
    Assertions.assertEquals(3, f1.heartbeatTriggers.get(), "the waiting round starts when a round ends");

    reply(f1, afterR2);
    Assertions.assertTrue(r2.isDone());
    // Sent after the third read arrived, but before its round started: the round counts from its start.
    reply(f1, afterR3);
    Assertions.assertFalse(r3.isDone());
    reply(f1, f1.send());
    Assertions.assertTrue(r3.isDone());
  }

  @Test
  public void testAJointMajorityNeedsBothFollowers() {
    final Predicate<Predicate<RaftPeerId>> both = isAcked -> isAcked.test(f1.id) && isAcked.test(f2.id);
    final CompletableFuture<Void> read = heartbeats.confirm(appenders);
    f1.reply(heartbeats, f1.send(), true, both);
    Assertions.assertFalse(read.isDone());
    f2.reply(heartbeats, f2.send(), true, both);
    Assertions.assertTrue(read.isDone());
  }

  @Test
  public void testAStepDownFailsEveryRead() {
    final Set<CompletableFuture<Void>> reads = new HashSet<>();
    for (int i = 0; i < 4; i++) {
      reads.add(heartbeats.confirm(appenders));
    }
    Assertions.assertEquals(3, reads.size());
    final Exception stepDown = new Exception("not leader");
    heartbeats.failListeners(stepDown);
    reads.add(heartbeats.confirm(appenders));
    for (CompletableFuture<Void> read : reads) {
      Assertions.assertTrue(read.isCompletedExceptionally());
    }
    reply(f1, f1.send());
  }
}
