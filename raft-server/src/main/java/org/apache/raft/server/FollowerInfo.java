/**
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
package org.apache.raft.server;

import org.apache.raft.protocol.RaftPeer;

import java.util.concurrent.atomic.AtomicLong;

public class FollowerInfo {
  private final RaftPeer peer;
  private final AtomicLong lastRpcResponseTime;
  private final AtomicLong lastRpcSendTime;
  private long nextIndex;
  private final AtomicLong matchIndex;
  private volatile boolean attendVote;

  FollowerInfo(RaftPeer peer, long lastRpcTime, long nextIndex,
      boolean attendVote) {
    this.peer = peer;
    this.lastRpcResponseTime = new AtomicLong(lastRpcTime);
    this.lastRpcSendTime = new AtomicLong(lastRpcTime);
    this.nextIndex = nextIndex;
    this.matchIndex = new AtomicLong(0);
    this.attendVote = attendVote;
  }

  public void updateMatchIndex(final long matchIndex) {
    this.matchIndex.set(matchIndex);
  }

  long getMatchIndex() {
    return matchIndex.get();
  }

  public synchronized long getNextIndex() {
    return nextIndex;
  }

  public synchronized void updateNextIndex(long i) {
    nextIndex = i;
  }

  public synchronized void decreaseNextIndex(long targetIndex) {
    if (nextIndex > 0) {
      nextIndex = Math.min(nextIndex - 1, targetIndex);
    }
  }

  @Override
  public String toString() {
    return peer.getId() + "(next=" + nextIndex + ", match=" + matchIndex + "," +
        " attendVote=" + attendVote +
        ", lastRpcSendTime=" + lastRpcSendTime.get() +
        ", lastRpcResponseTime=" + lastRpcResponseTime.get() + ")";
  }

  void startAttendVote() {
    attendVote = true;
  }

  public boolean isAttendingVote() {
    return attendVote;
  }

  public RaftPeer getPeer() {
    return peer;
  }

  public void updateLastRpcResponseTime(long time) {
    lastRpcResponseTime.set(time);
  }

  public long getLastRpcResponseTime() {
    return lastRpcResponseTime.get();
  }

  public void updateLastRpcSendTime(long time) {
    lastRpcSendTime.set(time);
  }

  public long getLastRpcTime() {
    return Math.max(lastRpcResponseTime.get(), lastRpcSendTime.get());
  }
}
