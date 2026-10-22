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

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

/**
 * RATIS-2644: a follower that grants a higher-term vote while an old leader's
 * append is still in flight must not report SUCCESS for that append. It must
 * reply NOT_LEADER with its current term so that the old leader steps down
 * instead of committing an entry the new leader may not have.
 */
public class TestStaleAppendEntriesReplyWithGrpc extends BaseTest
    implements MiniRaftClusterWithGrpc.FactoryGet {
  /** Same value as {@code RaftServerImpl.APPEND_ENTRIES_BEFORE_APPEND_LOG} (package-private). */
  private static final String APPEND_ENTRIES_BEFORE_APPEND_LOG =
      "RaftServerImpl.appendEntries.beforeAppendLog";

  {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), false);
    RaftServerConfigKeys.Rpc.setTimeoutMin(getProperties(), TimeDuration.valueOf(150, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(getProperties(), TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setRequestTimeout(getProperties(), TimeDuration.valueOf(10, TimeUnit.SECONDS));
    RaftServerConfigKeys.LeaderElection.setLeaderStepDownWaitTime(getProperties(),
        TimeDuration.valueOf(60, TimeUnit.SECONDS));
  }

  @Test
  @Timeout(120)
  public void staleAppendAfterHigherTermVoteRepliesNotLeader() throws Exception {
    runWithNewCluster(3, this::runStaleAppendAfterHigherTermVoteRepliesNotLeader);
  }

  private void runStaleAppendAfterHigherTermVoteRepliesNotLeader(MiniRaftClusterWithGrpc cluster) throws Exception {
    BlockRequestHandlingInjection.getInstance().unblockAll();
    final RaftServer.Division oldLeader = waitForLeader(cluster);
    final RaftPeerId oldLeaderId = oldLeader.getId();
    final long oldTerm = oldLeader.getInfo().getCurrentTerm();
    final List<RaftServer.Division> followers = cluster.getFollowers();
    Assertions.assertEquals(2, followers.size(), "expected two followers in a 3-node cluster");

    final RaftServer.Division candidate = followers.get(0);
    final RaftPeerId candidateId = candidate.getId();
    final RaftServer.Division target = followers.get(1);
    final RaftPeerId targetId = target.getId();

    final RaftTestUtil.SimpleMessage message = new RaftTestUtil.SimpleMessage(
        "stale-append-" + oldTerm + "-" + System.nanoTime());
    final BeforeAppendBlocker appendBlocker = new BeforeAppendBlocker(targetId, message.getContent());

    CodeInjectionForTesting.put(APPEND_ENTRIES_BEFORE_APPEND_LOG, appendBlocker);
    try (RaftClient oldLeaderClient = cluster.createClient(oldLeaderId, RetryPolicies.noRetry())) {
      // Keep the candidate from receiving the old leader's entry.
      BlockRequestHandlingInjection.getInstance().blockReplier(candidateId.toString());

      final CompletableFuture<RaftClientReply> replyFuture = oldLeaderClient.async().send(message);
      Assertions.assertTrue(appendBlocker.awaitBlocked(30, TimeUnit.SECONDS),
          "target follower did not reach the accepted-before-append window");
      Assertions.assertFalse(containsMessage(target, message),
          "target should not contain the entry before the blocked append is released");

      // Unfreeze the candidate so it can run an election, and isolate the old
      // leader so that only the candidate and the target can talk to each other.
      BlockRequestHandlingInjection.getInstance().unblockReplier(candidateId.toString());
      BlockRequestHandlingInjection.getInstance().blockRequestor(oldLeaderId.toString());
      BlockRequestHandlingInjection.getInstance().blockReplier(oldLeaderId.toString());

      // The target grants a higher-term vote to the candidate and the candidate
      // becomes the new leader in a term higher than the old leader's.
      JavaUtils.attempt(() -> Assertions.assertTrue(
          candidate.getInfo().isLeader() && candidate.getInfo().getCurrentTerm() > oldTerm,
          "candidate did not become leader in a higher term; role="
              + candidate.getInfo().getCurrentRole() + ", term=" + candidate.getInfo().getCurrentTerm()),
          150, HUNDRED_MILLIS, "candidate becomes new leader", LOG);
      Assertions.assertTrue(target.getInfo().getCurrentTerm() > oldTerm,
          "target did not move to a higher term; term=" + target.getInfo().getCurrentTerm());

      // Release the blocked old-leader append: it must now reply NOT_LEADER,
      // not a stale SUCCESS, so the old leader cannot commit the entry.
      appendBlocker.release();

      // The client write must not be reported as success: either the old leader replies
      // NOT_LEADER, or the request fails because the old leader has stepped down.
      final boolean reportedSuccess = replyFuture
          .thenApply(RaftClientReply::isSuccess)
          .exceptionally(e -> false)
          .get(30, TimeUnit.SECONDS);
      Assertions.assertFalse(reportedSuccess,
          "old leader client write must not be reported as success");
      Assertions.assertEquals(0, oldLeader.getRaftLog().getLastCommittedIndex(),
          "old leader must not commit the stale entry");
    } finally {
      appendBlocker.release();
      CodeInjectionForTesting.remove(APPEND_ENTRIES_BEFORE_APPEND_LOG);
      BlockRequestHandlingInjection.getInstance().unblockAll();
    }
  }

  private static boolean containsMessage(RaftServer.Division server, RaftTestUtil.SimpleMessage message)
      throws Exception {
    final RaftLog log = server.getRaftLog();
    final TermIndex last = log.getLastEntryTermIndex();
    if (last == null) {
      return false;
    }
    final long start = Math.max(RaftLog.LEAST_VALID_LOG_INDEX, log.getStartIndex());
    for (long index = start; index <= last.getIndex(); index++) {
      final LogEntryProto entry = log.get(index);
      if (entry != null && entry.hasStateMachineLogEntry()
          && message.getContent().equals(entry.getStateMachineLogEntry().getLogData())) {
        return true;
      }
    }
    return false;
  }

  /** Blocks the target follower's RaftLog append for the matching entry until released. */
  private static final class BeforeAppendBlocker implements CodeInjectionForTesting.Code {
    private final RaftPeerId targetId;
    private final ByteString payload;
    private final AtomicBoolean blockedOnce = new AtomicBoolean();
    private final CountDownLatch blocked = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    private BeforeAppendBlocker(RaftPeerId targetId, ByteString payload) {
      this.targetId = targetId;
      this.payload = payload;
    }

    @Override
    public boolean execute(Object localId, Object remoteId, Object... args) {
      if (!targetId.toString().equals(String.valueOf(localId)) || !blockedOnce.compareAndSet(false, true)) {
        return false;
      }
      if (args.length < 1 || !(args[0] instanceof List)) {
        blockedOnce.set(false);
        return false;
      }
      @SuppressWarnings("unchecked")
      final List<LogEntryProto> entries = (List<LogEntryProto>) args[0];
      final boolean match = entries.stream().anyMatch(e -> e.hasStateMachineLogEntry()
          && payload.equals(e.getStateMachineLogEntry().getLogData()));
      if (!match) {
        blockedOnce.set(false);
        return false;
      }

      blocked.countDown();
      try {
        if (!release.await(60, TimeUnit.SECONDS)) {
          throw new AssertionError("timed out waiting to release old-leader append");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while blocking old-leader append", e);
      }
      return true;
    }

    boolean awaitBlocked(long timeout, TimeUnit unit) throws InterruptedException {
      return blocked.await(timeout, unit);
    }

    void release() {
      release.countDown();
    }
  }
}
