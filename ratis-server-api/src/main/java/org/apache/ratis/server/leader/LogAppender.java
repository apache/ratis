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

import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.AwaitForSignal;
import org.apache.ratis.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A {@link LogAppender} is for the leader to send appendEntries to a particular follower.
 */
public interface LogAppender {
  Logger LOG = LoggerFactory.getLogger(LogAppender.class);

  Class<? extends LogAppender> DEFAULT_CLASS = ReflectionUtils.getClass(
      LogAppender.class.getName() + "Default", LogAppender.class);

  /** Create the default {@link LogAppender}. */
  static LogAppender newLogAppenderDefault(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
    final Class<?>[] argClasses = {RaftServer.Division.class, LeaderState.class, FollowerInfo.class};
    return ReflectionUtils.newInstance(DEFAULT_CLASS, argClasses, server, leaderState, f);
  }

  /** @return the server. */
  RaftServer.Division getServer();

  /** The same as getServer().getRaftServer().getServerRpc(). */
  default RaftServerRpc getServerRpc() {
    return getServer().getRaftServer().getServerRpc();
  }

  /** The same as getServer().getRaftLog(). */
  default RaftLog getRaftLog() {
    return getServer().getRaftLog();
  }

  /** Start this {@link LogAppender}. */
  void start();

  /** Is this {@link LogAppender} running? */
  boolean isRunning();

  /**
   * Stop this {@link LogAppender} asynchronously.
   * @deprecated override {@link #stopAsync()} instead.
   */
  @Deprecated
  default void stop() {
    throw new UnsupportedOperationException();
  }

  /**
   * Stop this {@link LogAppender} asynchronously.
   *
   * @return a future of the final state.
   */
  default CompletableFuture<?> stopAsync() {
    stop();
    return CompletableFuture.supplyAsync(() -> {
      while (isRunning()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CompletionException("stopAsync interrupted", e);
        }
      }
      return null;
    });
  }

  /** @return the leader state. */
  LeaderState getLeaderState();

  /** @return the follower information for this {@link LogAppender}. */
  FollowerInfo getFollower();

  /** The same as getFollower().getPeer().getId(). */
  default RaftPeerId getFollowerId() {
    return getFollower().getId();
  }

  /** @return the call id for the next {@link AppendEntriesRequestProto}. */
  long getCallId();

  /** @return the a {@link Comparator} for comparing call ids. */
  Comparator<Long> getCallIdComparator();

  /**
   * Create a {@link AppendEntriesRequestProto} object using the {@link FollowerInfo} of this {@link LogAppender}.
   * The {@link AppendEntriesRequestProto} object may contain zero or more log entries.
   * When there is zero log entries, the {@link AppendEntriesRequestProto} object is a heartbeat.
   *
   * @param callId The call id of the returned request.
   * @param heartbeat the returned request must be a heartbeat.
   *
   * @return a new {@link AppendEntriesRequestProto} object.
   */
  AppendEntriesRequestProto newAppendEntriesRequest(long callId, boolean heartbeat) throws RaftLogIOException;

  /** @return a new {@link InstallSnapshotRequestProto} object. */
  InstallSnapshotRequestProto newInstallSnapshotNotificationRequest(TermIndex firstAvailableLogTermIndex);

  /** @return an {@link Iterable} of {@link InstallSnapshotRequestProto} for sending the given snapshot. */
  Iterable<InstallSnapshotRequestProto> newInstallSnapshotRequests(String requestId, SnapshotInfo snapshot);

  /**
   * Get the previous {@link TermIndex} for the given next index.
   * This is used to set the previous log entry in AppendEntries requests.
   *
   * @return the previous {@link TermIndex}, or null if unavailable
   *         (e.g. the entry has been purged and the snapshot does not cover it).
   */
  default TermIndex getPrevious(long nextIndex) {
    if (nextIndex == RaftLog.LEAST_VALID_LOG_INDEX) {
      return null;
    }

    final long previousIndex = nextIndex - 1;
    final TermIndex previous = getRaftLog().getTermIndex(previousIndex);
    if (previous != null) {
      return previous;
    }

    final SnapshotInfo snapshot = getServer().getStateMachine().getLatestSnapshot();
    if (snapshot != null) {
      final TermIndex snapshotTermIndex = snapshot.getTermIndex();
      if (snapshotTermIndex.getIndex() == previousIndex) {
        return snapshotTermIndex;
      }
    }

    return null;
  }

  /**
   * Should this {@link LogAppender} send a snapshot to the follower?
   *
   * @return the snapshot if it should install a snapshot; otherwise, return null.
   */
  default SnapshotInfo shouldInstallSnapshot() {
    final SnapshotInfo snapshot = getServer().getStateMachine().getLatestSnapshot();
    return shouldInstallSnapshot(snapshot != null) ? snapshot : null;
  }

  /**
   * Should this {@link LogAppender} send a snapshot notification to the follower?
   *
   * @return the first available log {@link TermIndex} if it should install a snapshot; otherwise, return null.
   */
  default TermIndex shouldNotifyToInstallSnapshot() {
    if (!shouldInstallSnapshot(true)) {
      return null;
    }
    final TermIndex start = getRaftLog().getTermIndex(getRaftLog().getStartIndex());
    if (start != null) {
      return start;
    }
    // No log is currently available; return the next, which will become available in the future.
    return TermIndex.valueOf(getServer().getInfo().getCurrentTerm(), getRaftLog().getNextIndex());
  }

  default boolean shouldInstallSnapshot(boolean hasSnapshot) {
    final FollowerInfo follower = getFollower();
    if (getLeaderState().isFollowerBootstrapping(follower)
        && !follower.hasAttemptedToInstallSnapshot()) {
      if (!hasSnapshot) {
        // Leader cannot send null snapshot to follower. Hence, acknowledge InstallSnapshot attempt (even though it
        // was not attempted) so that follower can come out of staging state after appending log entries.
        follower.setAttemptedToInstallSnapshot();
      }
      return true;
    }

    final long leaderNextIndex = getRaftLog().getNextIndex();
    final long followerNextIndex = getFollower().getNextIndex();
    if (followerNextIndex >= leaderNextIndex) {
      // follower caught up already
      return false;
    }
    final long leaderStartIndex = getRaftLog().getStartIndex();
    if (followerNextIndex < leaderStartIndex || leaderStartIndex == RaftLog.INVALID_LOG_INDEX) {
      // leader does not have follower's next log
      return true;
    }
    // leader does not have the previous log for appendEntries
    return followerNextIndex == leaderStartIndex &&
        followerNextIndex > RaftLog.LEAST_VALID_LOG_INDEX &&
        getPrevious(followerNextIndex) == null;
  }

  /** Define how this {@link LogAppender} should run. */
  void run() throws InterruptedException, IOException;

  /**
   * Get the {@link AwaitForSignal} for events, which can be:
   * (1) new log entries available,
   * (2) log indices changed, or
   * (3) a snapshot installation completed.
   */
  AwaitForSignal getEventAwaitForSignal();

  /** The same as getEventAwaitForSignal().signal(). */
  default void notifyLogAppender() {
    getEventAwaitForSignal().signal();
  }

  /** Should the leader send appendEntries RPC to the follower? */
  default boolean shouldSendAppendEntries() {
    return hasAppendEntries() || getHeartbeatWaitTimeMs() <= 0;
  }

  /** Does it have outstanding appendEntries? */
  default boolean hasAppendEntries() {
    return getFollower().getNextIndex() < getRaftLog().getNextIndex();
  }

  /** Trigger to send a heartbeat AppendEntries. */
  void triggerHeartbeat();

  /** @return the wait time in milliseconds to send the next heartbeat. */
  default long getHeartbeatWaitTimeMs() {
    final int min = getServer().properties().minRpcTimeoutMs();
    // time remaining to send a heartbeat
    final long heartbeatRemainingTimeMs = min/2 - getFollower().getLastRpcResponseTime().elapsedTimeMs();
    // avoid sending heartbeat too frequently
    final long noHeartbeatTimeMs = min/4 - getFollower().getLastHeartbeatSendTime().elapsedTimeMs();
    return Math.max(heartbeatRemainingTimeMs, noHeartbeatTimeMs);
  }

  /** Handle the event that the follower has replied a term. */
  default boolean onFollowerTerm(long followerTerm) {
    synchronized (getServer()) {
      return isRunning() && getLeaderState().onFollowerTerm(getFollower(), followerTerm);
    }
  }
}
