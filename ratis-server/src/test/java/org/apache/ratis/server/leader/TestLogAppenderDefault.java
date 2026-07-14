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
package org.apache.ratis.server.leader;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class TestLogAppenderDefault {
  private static final RaftPeerId FOLLOWER_ID = RaftPeerId.valueOf("follower");

  @Test
  void heartbeatSuccessAdvancesOnlyToRequestPreviousIndex() {
    final LeaderState leaderState = mock(LeaderState.class);
    final TestFollowerInfo follower = new TestFollowerInfo(8, 4);
    final LogAppenderDefault appender = newLogAppender(leaderState, follower);
    final AppendEntriesReplyProto reply = newSuccessReply(20);

    appender.handleReply(reply, RaftLog.INVALID_LOG_INDEX, 9, true);

    Assertions.assertEquals(9, follower.getMatchIndex());
    Assertions.assertEquals(10, follower.getNextIndex());
    Assertions.assertEquals(1, follower.successfulMatchIndexUpdates.get());
    Assertions.assertEquals(1, follower.nextIndexIncreases.get());
    verify(leaderState).onFollowerSuccessAppendEntries(follower);
    verify(leaderState).onAppendEntriesReply(appender, reply);
    verifyNoMoreInteractions(leaderState);
  }

  @Test
  void heartbeatSuccessWithoutPreviousLogDoesNotAdvance() {
    final LeaderState leaderState = mock(LeaderState.class);
    final TestFollowerInfo follower = new TestFollowerInfo(8, 4);
    final LogAppenderDefault appender = newLogAppender(leaderState, follower);
    final AppendEntriesReplyProto reply = newSuccessReply(20);

    appender.handleReply(reply, RaftLog.INVALID_LOG_INDEX, RaftLog.INVALID_LOG_INDEX, true);

    Assertions.assertEquals(4, follower.getMatchIndex());
    Assertions.assertEquals(8, follower.getNextIndex());
    Assertions.assertEquals(0, follower.successfulMatchIndexUpdates.get());
    Assertions.assertEquals(0, follower.nextIndexIncreases.get());
    verify(leaderState).onAppendEntriesReply(appender, reply);
    verifyNoMoreInteractions(leaderState);
  }

  @Test
  void appendSuccessStillUsesReplyNextIndex() {
    final LeaderState leaderState = mock(LeaderState.class);
    final TestFollowerInfo follower = new TestFollowerInfo(8, 4);
    final LogAppenderDefault appender = newLogAppender(leaderState, follower);
    final AppendEntriesReplyProto reply = newSuccessReply(20);

    appender.handleReply(reply, 8, 7, false);

    Assertions.assertEquals(19, follower.getMatchIndex());
    Assertions.assertEquals(20, follower.getNextIndex());
    Assertions.assertEquals(1, follower.successfulMatchIndexUpdates.get());
    Assertions.assertEquals(1, follower.nextIndexIncreases.get());
    verify(leaderState).onFollowerSuccessAppendEntries(follower);
    verify(leaderState).onAppendEntriesReply(appender, reply);
    verifyNoMoreInteractions(leaderState);
  }

  private static LogAppenderDefault newLogAppender(LeaderState leaderState, FollowerInfo follower) {
    final RaftServer.Division division = mock(RaftServer.Division.class);
    final RaftServer raftServer = mock(RaftServer.class);
    when(division.getRaftServer()).thenReturn(raftServer);
    when(division.getThreadGroup()).thenReturn(Thread.currentThread().getThreadGroup());
    when(raftServer.getProperties()).thenReturn(new RaftProperties());
    return new LogAppenderDefault(division, leaderState, follower);
  }

  private static AppendEntriesReplyProto newSuccessReply(long nextIndex) {
    return AppendEntriesReplyProto.newBuilder()
        .setResult(AppendEntriesReplyProto.AppendResult.SUCCESS)
        .setNextIndex(nextIndex)
        .build();
  }

  private static final class TestFollowerInfo implements FollowerInfo {
    private final RaftPeer peer = RaftPeer.newBuilder().setId(FOLLOWER_ID).build();
    private final AtomicInteger successfulMatchIndexUpdates = new AtomicInteger();
    private final AtomicInteger nextIndexIncreases = new AtomicInteger();
    private long matchIndex;
    private long nextIndex;

    private TestFollowerInfo(long nextIndex, long matchIndex) {
      this.nextIndex = nextIndex;
      this.matchIndex = matchIndex;
    }

    @Override
    public String getName() {
      return FOLLOWER_ID.toString();
    }

    @Override
    public RaftPeerId getId() {
      return FOLLOWER_ID;
    }

    @Override
    public RaftPeer getPeer() {
      return peer;
    }

    @Override
    public long getMatchIndex() {
      return matchIndex;
    }

    @Override
    public boolean updateMatchIndex(long newMatchIndex) {
      if (newMatchIndex > matchIndex) {
        matchIndex = newMatchIndex;
        successfulMatchIndexUpdates.incrementAndGet();
        return true;
      }
      return false;
    }

    @Override
    public long getCommitIndex() {
      return 0;
    }

    @Override
    public boolean updateCommitIndex(long newCommitIndex) {
      return false;
    }

    @Override
    public long getSnapshotIndex() {
      return 0;
    }

    @Override
    public void setSnapshotIndex(long newSnapshotIndex) {
    }

    @Override
    public void setAttemptedToInstallSnapshot() {
    }

    @Override
    public boolean hasAttemptedToInstallSnapshot() {
      return false;
    }

    @Override
    public long getNextIndex() {
      return nextIndex;
    }

    @Override
    public void increaseNextIndex(long newNextIndex) {
      if (newNextIndex > nextIndex) {
        nextIndex = newNextIndex;
        nextIndexIncreases.incrementAndGet();
      }
    }

    @Override
    public void decreaseNextIndex(long newNextIndex) {
      nextIndex = Math.min(nextIndex, newNextIndex);
    }

    @Override
    public void setNextIndex(long newNextIndex) {
      nextIndex = newNextIndex;
    }

    @Override
    public void updateNextIndex(long newNextIndex) {
      nextIndex = newNextIndex;
    }

    @Override
    public void computeNextIndex(LongUnaryOperator op) {
      nextIndex = op.applyAsLong(nextIndex);
    }

    @Override
    public Timestamp getLastRpcResponseTime() {
      return Timestamp.currentTime();
    }

    @Override
    public Timestamp getLastRpcSendTime() {
      return Timestamp.currentTime();
    }

    @Override
    public void updateLastRpcResponseTime() {
    }

    @Override
    public void updateLastRpcSendTime(boolean isHeartbeat) {
    }

    @Override
    public Timestamp getLastRpcTime() {
      return Timestamp.currentTime();
    }

    @Override
    public Timestamp getLastHeartbeatSendTime() {
      return Timestamp.currentTime();
    }

    @Override
    public Timestamp getLastRespondedAppendEntriesSendTime() {
      return Timestamp.currentTime();
    }

    @Override
    public void updateLastRespondedAppendEntriesSendTime(Timestamp sendTime) {
    }

    @Override
    public ErrorState getErrorState() {
      return null;
    }
  }
}
