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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/** For supporting linearizable read. */
class ReadRequests {
  private static final Logger LOG = LoggerFactory.getLogger(ReadRequests.class);

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

  static class AppendEntriesListener {
    private final long commitIndex;
    private final CompletableFuture<Long> future = new CompletableFuture<>();
    private final ConcurrentHashMap<RaftPeerId, HeartbeatAck> replies = new ConcurrentHashMap<>();

    AppendEntriesListener(long commitIndex) {
      this.commitIndex = commitIndex;
    }

    CompletableFuture<Long> getFuture() {
      return future;
    }

    boolean receive(LogAppender logAppender, AppendEntriesReplyProto proto,
                    Predicate<Predicate<RaftPeerId>> hasMajority) {
      if (isCompletedNormally()) {
        return true;
      }

      final HeartbeatAck reply = replies.computeIfAbsent(
          logAppender.getFollowerId(), key -> new HeartbeatAck(logAppender));
      if (reply.receive(proto)) {
        if (hasMajority.test(id -> replies.get(id).isAcknowledged())) {
          future.complete(commitIndex);
          return true;
        }
      }

      return isCompletedNormally();
    }

    boolean isCompletedNormally() {
      return future.isDone() && !future.isCancelled() && !future.isCompletedExceptionally();
    }
  }

  class AppendEntriesListeners {
    private final NavigableMap<Long, AppendEntriesListener> sorted = new TreeMap<>();

    synchronized AppendEntriesListener add(long commitIndex, Function<Long, AppendEntriesListener> constructor) {
      return sorted.computeIfAbsent(commitIndex, constructor);
    }

    synchronized void onAppendEntriesReply(LogAppender appender, AppendEntriesReplyProto reply,
                                           Predicate<Predicate<RaftPeerId>> hasMajority) {
      final long callId = reply.getServerReply().getCallId();

      Iterator<Map.Entry<Long, AppendEntriesListener>> iterator = sorted.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Long, AppendEntriesListener> entry = iterator.next();
        if (entry.getKey() > callId) {
          return;
        }

        final AppendEntriesListener listener = entry.getValue();
        if (listener == null) {
          continue;
        }

        if (listener.receive(appender, reply, hasMajority)) {
          ackedCommitIndex.updateToMax(listener.commitIndex, s -> LOG.debug("{}: {}", ReadRequests.this, s));
          iterator.remove();
        }
      }
    }
  }

  static class ReadIndexQueue {
    private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();
    private final NavigableMap<Long, CompletableFuture<Long>> sorted = new ConcurrentSkipListMap<>();
    private final TimeDuration readTimeout;

    ReadIndexQueue(TimeDuration readTimeout) {
      this.readTimeout = readTimeout;
    }

    CompletableFuture<Long> add(long readIndex) {
      final MemoizedSupplier<CompletableFuture<Long>> supplier = MemoizedSupplier.valueOf(CompletableFuture::new);
      final CompletableFuture<Long> f = sorted.computeIfAbsent(readIndex, i -> supplier.get());

      if (supplier.isInitialized()) {
        scheduler.onTimeout(readTimeout, () -> handleTimeout(readIndex),
            LOG, () -> "Failed to handle read timeout for index " + readIndex);
      }
      return f;
    }

    private void handleTimeout(long readIndex) {
      Optional.ofNullable(sorted.remove(readIndex)).ifPresent(consumer -> {
        consumer.completeExceptionally(
          new ReadException(new TimeoutIOException("Read timeout for index " + readIndex)));
      });
    }

    void complete(Long appliedIndex) {
      for(;;) {
        if (sorted.isEmpty()) {
          return;
        }
        final Long first = sorted.firstKey();
        if (first == null || first > appliedIndex) {
          return;
        }
        Optional.ofNullable(sorted.remove(first)).ifPresent(f -> f.complete(appliedIndex));
      }
    }
  }

  private final AppendEntriesListeners appendEntriesListeners = new AppendEntriesListeners();
  private final RaftLogIndex ackedCommitIndex = new RaftLogIndex("ackedCommitIndex", RaftLog.INVALID_LOG_INDEX);
  private final ReadIndexQueue readIndexQueue;
  private final StateMachine stateMachine;

  ReadRequests(RaftProperties properties, StateMachine stateMachine) {
    this.readIndexQueue = new ReadIndexQueue(RaftServerConfigKeys.Read.timeout(properties));
    this.stateMachine = stateMachine;
  }

  AppendEntriesListener addAppendEntriesListener(long commitIndex,
                                                 Function<Long, AppendEntriesListener> constructor) {
    if (commitIndex <= ackedCommitIndex.get()) {
      return null;
    }
    return appendEntriesListeners.add(commitIndex, constructor);
  }

  void onAppendEntriesReply(LogAppender appender, AppendEntriesReplyProto reply,
                            Predicate<Predicate<RaftPeerId>> hasMajority) {
    appendEntriesListeners.onAppendEntriesReply(appender, reply, hasMajority);
  }

  Consumer<Long> getAppliedIndexConsumer() {
    return readIndexQueue::complete;
  }

  CompletableFuture<Long> waitToAdvance(long readIndex) {
    if (stateMachine.getLastAppliedTermIndex().getIndex() >= readIndex) {
      return CompletableFuture.completedFuture(readIndex);
    }
    return readIndexQueue.add(readIndex);
  }
}
