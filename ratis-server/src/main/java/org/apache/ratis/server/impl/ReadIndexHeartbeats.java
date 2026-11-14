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
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.util.JavaUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Confirms, for the reads of a leader, that it still leads: a read is answered after a majority acknowledged
 * an appendEntries request that was sent after the read arrived.
 * <p>
 * The confirmations are made in rounds. A round sends a heartbeat to every follower
 * and ends when a majority has acknowledged a request not older than the round.
 * A read never joins a round that has started, since the requests of that round may have been sent before the read
 * arrived, and an acknowledgement of such a request does not show that this server led when the read arrived:
 * another leader may have been elected, and may have committed a write, in between.
 * For the same reason a finished round confirms nothing for the reads that arrive after it,
 * whatever the commit index.
 * <p>
 * At most {@link #MAX_STARTED_ROUNDS} rounds are started at a time; the reads that arrive meanwhile share one round,
 * which starts when one of those ends. So a read waits for at most two round trips, and the number of heartbeats
 * does not grow with the number of reads.
 */
class ReadIndexHeartbeats {
  static final int MAX_STARTED_ROUNDS = 2;

  /** The acknowledgement from a {@link LogAppender} of a heartbeat for a particular call id. */
  static class HeartbeatAck {
    private final LogAppender appender;
    private final long minCallId;
    private volatile boolean acknowledged = false;

    HeartbeatAck(LogAppender appender) {
      this.appender = appender;
      this.minCallId = appender.getCallId();
    }

    /** Is the heartbeat (for a particular call id) acknowledged? */
    boolean isAcknowledged() {
      return acknowledged;
    }

    /**
     * @return true if the acknowledged state is changed from false to true;
     *         otherwise, the acknowledged state remains unchanged, return false.
     */
    boolean receive(AppendEntriesReplyProto reply) {
      if (acknowledged) {
        return false;
      }
      synchronized (this) {
        if (!acknowledged && isValid(reply)) {
          acknowledged = true;
          return true;
        }
        return false;
      }
    }

    private boolean isValid(AppendEntriesReplyProto reply) {
      if (reply == null || !reply.getServerReply().getSuccess()) {
        return false;
      }
      // valid only if the reply has a later call id than the min.
      return appender.getCallIdComparator().compare(reply.getServerReply().getCallId(), minCallId) >= 0;
    }
  }

  /** A round of leadership confirmation. */
  static class AppendEntriesListener {
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private final ConcurrentHashMap<RaftPeerId, HeartbeatAck> replies = new ConcurrentHashMap<>();

    /** Start this round: the acknowledgements counted are the ones of the requests sent from now on. */
    void start(Iterable<LogAppender> logAppenders) {
      for (LogAppender a : logAppenders) {
        // Before the trigger: the heartbeat it causes has a call id not smaller than the one read here.
        replies.put(a.getFollowerId(), new HeartbeatAck(a));
        a.triggerHeartbeat();
      }
    }

    CompletableFuture<Void> getFuture() {
      return future;
    }

    /** @return true if a majority has acknowledged this round. */
    boolean receive(LogAppender logAppender, AppendEntriesReplyProto proto,
                    Predicate<Predicate<RaftPeerId>> hasMajority) {
      // An appender made after the start of this round: its requests are counted from its next one.
      final HeartbeatAck reply = replies.computeIfAbsent(
          logAppender.getFollowerId(), key -> new HeartbeatAck(logAppender));
      return reply.receive(proto) && hasMajority.test(this::isAcknowledged);
    }

    boolean isAcknowledged(RaftPeerId id) {
      return Optional.ofNullable(replies.get(id)).filter(HeartbeatAck::isAcknowledged).isPresent();
    }
  }

  /** The rounds that have started and have not ended. */
  private final List<AppendEntriesListener> started = new ArrayList<>(MAX_STARTED_ROUNDS);
  /** The round of the reads that arrived when {@link #started} was full; it has not started. */
  private AppendEntriesListener waiting = null;
  private Iterable<LogAppender> waitingAppenders = null;
  private Exception exception = null;

  /**
   * Confirm the leadership for a read that arrives now.
   *
   * @return a future completed once a majority acknowledged a request sent after this call,
   *         or failed when the leader steps down.
   */
  synchronized CompletableFuture<Void> confirm(Iterable<LogAppender> logAppenders) {
    if (exception != null) {
      return JavaUtils.completeExceptionally(exception);
    }
    if (waiting == null) {
      waiting = new AppendEntriesListener();
    }
    waitingAppenders = logAppenders;
    final AppendEntriesListener round = waiting;
    startWaiting();
    return round.getFuture();
  }

  private void startWaiting() {
    if (waiting != null && started.size() < MAX_STARTED_ROUNDS) {
      final AppendEntriesListener round = waiting;
      waiting = null;
      started.add(round);
      round.start(waitingAppenders);
    }
  }

  void onAppendEntriesReply(LogAppender appender, AppendEntriesReplyProto reply,
                            Predicate<Predicate<RaftPeerId>> hasMajority) {
    List<AppendEntriesListener> confirmed = null;
    synchronized (this) {
      for (final Iterator<AppendEntriesListener> i = started.iterator(); i.hasNext(); ) {
        final AppendEntriesListener round = i.next();
        if (round.receive(appender, reply, hasMajority)) {
          i.remove();
          if (confirmed == null) {
            confirmed = new ArrayList<>(MAX_STARTED_ROUNDS);
          }
          confirmed.add(round);
        }
      }
      if (confirmed != null) {
        startWaiting();
      }
    }
    // Outside the lock: the reads continue on this thread.
    if (confirmed != null) {
      confirmed.forEach(round -> round.getFuture().complete(null));
    }
  }

  void failListeners(Exception e) {
    final List<AppendEntriesListener> failed;
    synchronized (this) {
      if (exception != null) {
        return;
      }
      exception = e;
      failed = new ArrayList<>(started);
      started.clear();
      if (waiting != null) {
        failed.add(waiting);
        waiting = null;
      }
    }
    failed.forEach(round -> round.getFuture().completeExceptionally(e));
  }
}
