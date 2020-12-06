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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A {@link LogAppender} is for the leader to send appendEntries to a particular follower.
 */
public interface LogAppender {
  Logger LOG = LoggerFactory.getLogger(LogAppender.class);

  RaftServer.Division getServer();

  default RaftServerRpc getServerRpc() {
    return getServer().getRaftServer().getServerRpc();
  }

  default RaftLog getRaftLog() {
    return getServer().getRaftLog();
  }

  void start();

  boolean isRunning();

  void stop();

  LeaderState getLeaderState();

  FollowerInfo getFollower();

  default RaftPeerId getFollowerId() {
    return getFollower().getPeer().getId();
  }

  AppendEntriesRequestProto newAppendEntriesRequest(long callId, boolean heartbeat) throws RaftLogIOException;

  InstallSnapshotRequestProto newInstallSnapshotNotificationRequest(TermIndex firstAvailableLogTermIndex);

  Iterable<InstallSnapshotRequestProto> newInstallSnapshotRequests(String requestId, SnapshotInfo snapshot);

  default SnapshotInfo shouldInstallSnapshot() {
    // we should install snapshot if the follower needs to catch up and:
    // 1. there is no local log entry but there is snapshot
    // 2. or the follower's next index is smaller than the log start index
    final long followerNextIndex = getFollower().getNextIndex();
    if (followerNextIndex < getRaftLog().getNextIndex()) {
      final long logStartIndex = getRaftLog().getStartIndex();
      final SnapshotInfo snapshot = getServer().getStateMachine().getLatestSnapshot();
      if (followerNextIndex < logStartIndex || (logStartIndex == RaftLog.INVALID_LOG_INDEX && snapshot != null)) {
        return snapshot;
      }
    }
    return null;
  }

  void run() throws InterruptedException, IOException;

  default void notifyLogAppender() {
    synchronized (this) {
      notify();
    }
  }

  /** Should the leader send appendEntries RPC to this follower? */
  default boolean shouldSendRequest() {
    return shouldAppendEntries(getFollower().getNextIndex()) || heartbeatTimeout();
  }

  default boolean shouldAppendEntries(long followerIndex) {
    return followerIndex < getRaftLog().getNextIndex();
  }

  default boolean heartbeatTimeout() {
    return getHeartbeatRemainingTime() <= 0;
  }

  /**
   * @return the time in milliseconds that the leader should send a heartbeat.
   */
  default long getHeartbeatRemainingTime() {
    return getServer().properties().minRpcTimeoutMs()/2 - getFollower().getLastRpcTime().elapsedTimeMs();
  }

  default boolean checkResponseTerm(long responseTerm) {
    synchronized (getServer()) {
      return isRunning() && getLeaderState().onFollowerTerm(getFollower(), responseTerm);
    }
  }
}
