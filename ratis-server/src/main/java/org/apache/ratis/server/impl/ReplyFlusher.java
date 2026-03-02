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

import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Implements the reply flush logic as part of the leader batch write when RepliedIndex is used.
 */
public class ReplyFlusher {
  static final Logger LOG = LoggerFactory.getLogger(ReplyFlusher.class);

  /** A write reply that has been built but not yet sent to the client. */
  static class HeldReply {
    private final PendingRequest pending;
    private final RaftClientReply reply;

    HeldReply(PendingRequest pending, RaftClientReply reply) {
      this.pending = pending;
      this.reply = reply;
    }

    void release() {
      pending.setReply(reply);
    }
  }

  static class Replies {
    private LinkedList<HeldReply> list = new LinkedList<>();

    synchronized void add(PendingRequest pending, RaftClientReply reply) {
      list.add(new HeldReply(pending, reply));
    }

    synchronized LinkedList<HeldReply> getAndSetNewList() {
      final LinkedList<HeldReply> old = list;
      list = new LinkedList<>();
      return old;
    }
  }

  private final String name;
  private final LifeCycle lifeCycle;
  private final Daemon daemon;
  private Replies replies = new Replies();
  private final RaftLogIndex repliedIndex;
  /** Supplies the last applied index from the state machine. */
  private final LongSupplier appliedIndexSupplier;
  /** The interval at which held write replies are flushed. */
  private final TimeDuration batchInterval;

  ReplyFlusher(String name, long repliedIndex, LongSupplier appliedIndexSupplier, TimeDuration batchInterval) {
    this.name = name + "-ReplyFlusher";
    this.lifeCycle = new LifeCycle(this.name);
    this.daemon = Daemon.newBuilder()
        .setName(this.name)
        .setRunnable(this::run)
        .build();
    this.repliedIndex = new RaftLogIndex("repliedIndex", repliedIndex);
    this.appliedIndexSupplier = appliedIndexSupplier;
    this.batchInterval = batchInterval;
  }

  long getRepliedIndex() {
    return repliedIndex.get();
  }

  /** Hold a write reply for later batch flushing. */
  void hold(PendingRequest pending, RaftClientReply reply) {
    replies.add(pending, reply);
  }

  void start() {
    lifeCycle.transition(LifeCycle.State.STARTING);
    // We need to transition to RUNNING first so that ReplyFlusher#run always
    // see that the lifecycle state is in RUNNING state.
    lifeCycle.transition(LifeCycle.State.RUNNING);
    daemon.start();
  }

  /** The reply flusher daemon loop. */
  private void run() {
    try {
      while (lifeCycle.getCurrentState() == LifeCycle.State.RUNNING) {
        try {
          Thread.sleep(batchInterval.toLong(TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        flush();
      }
    } finally {
      // Flush remaining on exit
      flush();
    }
  }

  /** Flush all held replies and advance {@link #repliedIndex} to the applied index. */
  private void flush() {
    final LinkedList<HeldReply> toFlush = replies.getAndSetNewList();
    for (HeldReply held : toFlush) {
      held.release();
    }
    final long appliedIndex = appliedIndexSupplier.getAsLong();
    repliedIndex.updateToMax(appliedIndex, s ->
        LOG.debug("{}: flushed {} replies, {}", name, toFlush.size(), s));
  }

  /** Stop the reply flusher daemon. */
  void stop() {
    lifeCycle.checkStateAndClose();
    daemon.interrupt();
    try {
      daemon.join(batchInterval.toLong(TimeUnit.MILLISECONDS )* 2);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
