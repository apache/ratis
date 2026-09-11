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

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.server.RaftServer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * {@link LogAppenderDefault} plus a dedicated heartbeat thread per follower.
 *
 * <p>The default appender is one thread per follower that sends a request (AppendEntries
 * batch, heartbeat or InstallSnapshot chunk) and blocks until the reply arrives, so there is
 * never more than one request in flight to a follower and no heartbeat is ever sent while a
 * large message is still on the wire. This subclass keeps that data loop unchanged and adds a
 * second thread which, whenever nothing has been sent to the follower for one heartbeat
 * interval (half of the minimum RPC timeout), sends an empty AppendEntries and waits for its
 * own reply. A heartbeat can therefore be in flight next to a batch or a snapshot chunk.
 *
 * <p>The heartbeat thread only updates the follower's last-response time and the leader's
 * term/commit bookkeeping from the reply; nextIndex/matchIndex handling stays with the data
 * thread, so an INCONSISTENCY reply to a heartbeat is ignored here. A failed heartbeat is
 * dropped, never retried and never resets the connection.
 *
 * <p>Enabled with {@code raft.server.log.appender.heartbeat.thread=true} through
 * {@code NettyFactory}/{@code QuicFactory}; see HB-THREAD-CHANGES.md in the repository root.
 * Prints {@code HBSTAT <followerId> <count> <sumNs>} (heartbeat RTT, cumulative) to stdout at
 * most once per second, like HOPSTAT in {@link LogAppenderDefault}.
 */
public class LogAppenderWithHeartbeatThread extends LogAppenderDefault {
  private static final long HBSTAT_PRINT_INTERVAL_NS = 1_000_000_000L;

  private volatile Thread heartbeatThread;

  // HBSTAT counters: touched only by the heartbeat thread.
  private long hbCount = 0;
  private long hbSumNs = 0;
  private long hbLastPrintNs = 0;

  public LogAppenderWithHeartbeatThread(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
    super(server, leaderState, f);
  }

  @Override
  public void run() throws InterruptedException, IOException {
    startHeartbeatThread();
    try {
      super.run();
    } finally {
      stopHeartbeatThread();
    }
  }

  private void startHeartbeatThread() {
    final Thread t = new Thread(this::heartbeatLoop, this + "-heartbeat");
    t.setDaemon(true);
    heartbeatThread = t;
    t.start();
    LOG.info("{}: heartbeat thread started (raft.server.log.appender.heartbeat.thread=true)", this);
  }

  private void stopHeartbeatThread() {
    final Thread t = heartbeatThread;
    heartbeatThread = null;
    if (t != null) {
      t.interrupt();
      try {
        t.join(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private long heartbeatIntervalMs() {
    return Math.max(1L, getServer().properties().minRpcTimeoutMs() / 2);
  }

  private void heartbeatLoop() {
    final long intervalMs = heartbeatIntervalMs();
    while (heartbeatThread == Thread.currentThread() && !Thread.currentThread().isInterrupted()) {
      try {
        if (!isRunning()) {
          Thread.sleep(intervalMs);
          continue;
        }
        final long sinceLastSendMs = getFollower().getLastRpcSendTime().elapsedTimeMs();
        final long waitMs = intervalMs - sinceLastSendMs;
        if (waitMs > 0) {
          Thread.sleep(waitMs);
          continue;
        }
        sendHeartbeat();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Throwable t) {
        // Dropped, not retried: the data thread owns error handling and reconnection.
        LOG.debug("{}: heartbeat failed", this, t);
        try {
          Thread.sleep(intervalMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private void sendHeartbeat() throws IOException {
    final AppendEntriesRequestProto request = newAppendEntriesRequest(CallId.getAndIncrement(), true);
    if (request == null) {
      return;
    }
    getFollower().updateLastRpcSendTime(true);
    final long t0 = System.nanoTime();
    final AppendEntriesReplyProto reply = getServerRpc().appendEntries(request);
    recordHbStat(System.nanoTime() - t0);
    getFollower().updateLastRpcResponseTime();
    switch (reply.getResult()) {
      case NOT_LEADER:
        onFollowerTerm(reply.getTerm());
        break;
      default:
        // SUCCESS or INCONSISTENCY: nextIndex/matchIndex are handled by the data thread only.
        break;
    }
    getLeaderState().onFollowerCommitIndex(getFollower(), reply.getFollowerCommit());
    getLeaderState().onAppendEntriesReply(this, reply);
  }

  private void recordHbStat(long elapsedNs) {
    hbCount++;
    hbSumNs += elapsedNs;
    final long now = System.nanoTime();
    if (now - hbLastPrintNs >= HBSTAT_PRINT_INTERVAL_NS) {
      hbLastPrintNs = now;
      System.out.println("HBSTAT " + getFollowerId() + " " + hbCount + " " + hbSumNs);
    }
  }
}
