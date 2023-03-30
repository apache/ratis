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

import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.util.Timestamp;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

class FollowerInfoImpl implements FollowerInfo {
  private final String name;
  private final Consumer<Object> infoIndexChange;
  private final Consumer<Object> debugIndexChange;

  private final AtomicReference<RaftPeer> peer;
  private final Function<RaftPeerId, RaftPeer> getPeer;
  private final AtomicReference<Timestamp> lastRpcResponseTime;
  private final AtomicReference<Timestamp> lastRpcSendTime;
  private final AtomicReference<Timestamp> lastHeartbeatSendTime;
  private final RaftLogIndex nextIndex;
  private final RaftLogIndex matchIndex = new RaftLogIndex("matchIndex", RaftLog.INVALID_LOG_INDEX);
  private final RaftLogIndex commitIndex = new RaftLogIndex("commitIndex", RaftLog.INVALID_LOG_INDEX);
  private final RaftLogIndex snapshotIndex = new RaftLogIndex("snapshotIndex", 0L);
  private volatile boolean caughtUp;
  private volatile boolean ackInstallSnapshotAttempt = false;

  FollowerInfoImpl(RaftGroupMemberId id, RaftPeer peer, Function<RaftPeerId, RaftPeer> getPeer,
      Timestamp lastRpcTime, long nextIndex, boolean caughtUp) {
    this.name = id + "->" + peer.getId();
    this.infoIndexChange = s -> LOG.info("{}: {}", name, s);
    this.debugIndexChange = s -> LOG.debug("{}: {}", name, s);

    this.peer = new AtomicReference<>(peer);
    this.getPeer = getPeer;
    this.lastRpcResponseTime = new AtomicReference<>(lastRpcTime);
    this.lastRpcSendTime = new AtomicReference<>(lastRpcTime);
    this.lastHeartbeatSendTime = new AtomicReference<>(lastRpcTime);
    this.nextIndex = new RaftLogIndex("nextIndex", nextIndex);
    this.caughtUp = caughtUp;
  }

  @Override
  public long getMatchIndex() {
    return matchIndex.get();
  }

  @Override
  public boolean updateMatchIndex(long newMatchIndex) {
    return matchIndex.updateToMax(newMatchIndex, debugIndexChange);
  }

  @Override
  public long getCommitIndex() {
    return commitIndex.get();
  }

  @Override
  public boolean updateCommitIndex(long newCommitIndex) {
    return commitIndex.updateToMax(newCommitIndex, debugIndexChange);
  }

  @Override
  public long getSnapshotIndex() {
    return snapshotIndex.get();
  }

  @Override
  public long getNextIndex() {
    return nextIndex.get();
  }

  @Override
  public void increaseNextIndex(long newNextIndex) {
    nextIndex.updateIncreasingly(newNextIndex, debugIndexChange);
  }

  @Override
  public void decreaseNextIndex(long newNextIndex) {
    nextIndex.updateUnconditionally(old -> old <= 0L? old: Math.min(old - 1, newNextIndex),
        message -> infoIndexChange.accept("decreaseNextIndex " + message));
  }

  @Override
  public void setNextIndex(long newNextIndex) {
    nextIndex.updateUnconditionally(old -> newNextIndex >= 0 ? newNextIndex : old,
        message -> infoIndexChange.accept("setNextIndex " + message));
  }

  @Override
  public void updateNextIndex(long newNextIndex) {
    nextIndex.updateToMax(newNextIndex,
        message -> infoIndexChange.accept("decreaseNextIndex " + message));
  }

  @Override
  public void setSnapshotIndex(long newSnapshotIndex) {
    snapshotIndex.setUnconditionally(newSnapshotIndex, infoIndexChange);
    matchIndex.setUnconditionally(newSnapshotIndex, infoIndexChange);
    nextIndex.setUnconditionally(newSnapshotIndex + 1, infoIndexChange);
  }

  @Override
  public void setAttemptedToInstallSnapshot() {
    LOG.info("Follower {} acknowledged installing snapshot", name);
    ackInstallSnapshotAttempt = true;
  }

  @Override
  public boolean hasAttemptedToInstallSnapshot() {
    return ackInstallSnapshotAttempt;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return name + "(c" + getCommitIndex() + ",m" + getMatchIndex() + ",n" + getNextIndex()
        + ", caughtUp=" + caughtUp +
        ", lastRpcSendTime=" + lastRpcSendTime.get().elapsedTimeMs() +
        ", lastRpcResponseTime=" + lastRpcResponseTime.get().elapsedTimeMs() + ")";
  }

  void catchUp() {
    caughtUp = true;
  }

  boolean isCaughtUp() {
    return caughtUp;
  }

  @Override
  public RaftPeerId getId() {
    return peer.get().getId();
  }

  @Override
  public RaftPeer getPeer() {
    final RaftPeer newPeer = getPeer.apply(getId());
    if (newPeer != null) {
      peer.set(newPeer);
      return newPeer;
    } else {
      return peer.get();
    }
  }

  @Override
  public void updateLastRpcResponseTime() {
    lastRpcResponseTime.set(Timestamp.currentTime());
  }

  @Override
  public Timestamp getLastRpcResponseTime() {
    return lastRpcResponseTime.get();
  }

  @Override
  public Timestamp getLastRpcSendTime() {
    return lastRpcSendTime.get();
  }

  @Override
  public void updateLastRpcSendTime(boolean isHeartbeat) {
    final Timestamp currentTime = Timestamp.currentTime();
    lastRpcSendTime.set(currentTime);
    if (isHeartbeat) {
      lastHeartbeatSendTime.set(currentTime);
    }
  }

  @Override
  public Timestamp getLastRpcTime() {
    return Timestamp.latest(lastRpcResponseTime.get(), lastRpcSendTime.get());
  }

  @Override
  public Timestamp getLastHeartbeatSendTime() {
    return lastHeartbeatSendTime.get();
  }
}
