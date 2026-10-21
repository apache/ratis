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
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.protocol.RaftServerProtocol.Op;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RATIS-2644: a follower that grants a higher-term vote while an old leader's
 * append is still in flight must reply NOT_LEADER (with its current term)
 * instead of a stale SUCCESS when the append eventually completes.
 */
public class TestStaleAppendEntriesReply extends BaseTest {
  private static final RaftPeerId LEADER = RaftPeerId.valueOf("s1");
  private static final RaftPeerId FOLLOWER = RaftPeerId.valueOf("s2");
  private static final RaftPeerId CANDIDATE = RaftPeerId.valueOf("s3");

  @Test
  @Timeout(60)
  public void appendEntriesRepliesNotLeaderAfterHigherTermVote() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(CANDIDATE)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);

    try (RaftServerImpl follower = newServer(group, storageVolume)) {
      follower.start();
      final long oldTerm = 1L;
      // Move the follower to term 1 under LEADER and confirm a normal append succeeds.
      Assertions.assertEquals(AppendResult.SUCCESS,
          follower.appendEntries(appendEntries(group, oldTerm, 0, 0)).getResult());

      // Hold the next (old-leader) append right before it reaches the RaftLog.
      final AppendBlocker blocker = new AppendBlocker();
      CodeInjectionForTesting.put(RaftServerImpl.APPEND_ENTRIES_BEFORE_APPEND_LOG, blocker);
      final CompletableFuture<AppendEntriesReplyProto> replyFuture;
      try {
        replyFuture = follower.appendEntriesAsync(appendEntries(group, oldTerm, 1, 1, TermIndex.valueOf(oldTerm, 0)));
        Assertions.assertTrue(blocker.awaitBlocked(10, TimeUnit.SECONDS),
            "follower did not reach the accepted-before-append window");

        // The follower moves to a higher term before the append completes, as it would
        // after granting a higher-term vote. (A direct requestVote is rejected here because
        // the follower's leader-lease still considers LEADER valid within the RPC timeout.)
        follower.changeToFollowerAndPersistMetadata(oldTerm + 1, true, Op.REQUEST_VOTE).join();
        Assertions.assertEquals(oldTerm + 1, follower.getState().getCurrentTerm());
      } finally {
        blocker.release();
        CodeInjectionForTesting.remove(RaftServerImpl.APPEND_ENTRIES_BEFORE_APPEND_LOG);
      }

      // The in-flight append completes after the vote: it must reply NOT_LEADER
      // with the current (higher) term, not a stale SUCCESS.
      final AppendEntriesReplyProto reply = replyFuture.get(10, TimeUnit.SECONDS);
      Assertions.assertEquals(AppendResult.NOT_LEADER, reply.getResult(),
          "expected NOT_LEADER after the term advanced, got: " + reply.getResult());
      Assertions.assertEquals(oldTerm + 1, reply.getTerm(),
          "the reply must carry the follower's current term so the old leader steps down");
    }
  }

  /** Blocks the follower's RaftLog append for non-heartbeat appends until released. */
  private static final class AppendBlocker implements CodeInjectionForTesting.Code {
    private final AtomicBoolean blockedOnce = new AtomicBoolean();
    private final CountDownLatch blocked = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    @Override
    public boolean execute(Object localId, Object remoteId, Object... args) {
      if (!blockedOnce.compareAndSet(false, true) || args.length < 1 || !(args[0] instanceof List)) {
        return false;
      }
      blocked.countDown();
      try {
        if (!release.await(30, TimeUnit.SECONDS)) {
          throw new AssertionError("timed out waiting to release the blocked append");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while blocking the append", e);
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

  private static RaftPeer peer(RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:0").build();
  }

  private static RaftServerImpl newServer(RaftGroup group, File storageVolume) throws IOException {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageVolume));
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(60, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(61, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties, TimeDuration.valueOf(60, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties, TimeDuration.valueOf(61, TimeUnit.SECONDS));

    final RaftServerProxy proxy = Mockito.mock(RaftServerProxy.class);
    Mockito.when(proxy.getId()).thenReturn(FOLLOWER);
    Mockito.when(proxy.getPeer()).thenReturn(group.getPeer(FOLLOWER));
    Mockito.when(proxy.getProperties()).thenReturn(properties);
    Mockito.when(proxy.getThreadGroup()).thenReturn(new ThreadGroup("stale-append-reply"));
    Mockito.when(proxy.getServerRpc()).thenReturn(Mockito.mock(RaftServerRpc.class));
    return new RaftServerImpl(group, new BaseStateMachine(), proxy, RaftStorage.StartupOption.FORMAT);
  }

  private static AppendEntriesRequestProto appendEntries(RaftGroup group, long term, long callId, long index) {
    return appendEntries(group, term, callId, index, null);
  }

  private static AppendEntriesRequestProto appendEntries(RaftGroup group, long term, long callId, long index,
      TermIndex previous) {
    final AppendEntriesRequestProto.Builder b = AppendEntriesRequestProto.newBuilder()
        .setServerRequest(RaftRpcRequestProto.newBuilder()
            .setRequestorId(LEADER.toByteString())
            .setReplyId(FOLLOWER.toByteString())
            .setRaftGroupId(RaftGroupIdProto.newBuilder().setId(group.getGroupId().toByteString()).build())
            .setCallId(callId)
            .build())
        .setLeaderTerm(term)
        .setLeaderCommit(0)
        .addEntries(LogEntryProto.newBuilder().setTerm(term).setIndex(index).build());
    if (previous != null) {
      b.setPreviousLog(previous.toProto());
    }
    return b.build();
  }
}
