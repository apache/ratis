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

import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
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

  private static final String CLASS_NAME = JavaUtils.getClassSimpleName(RaftServerImpl.class);
  public static final String FLUSH = CLASS_NAME + ".flush";

  static class Replies {
    /** When a {@link LongSupplier} is invoked, it completes a write reply and return the log index. */
    private LinkedList<LongSupplier> list = new LinkedList<>();

    synchronized void add(LongSupplier replyMethod) {
      list.add(replyMethod);
    }

    synchronized LinkedList<LongSupplier> getAndSetNewList() {
      final LinkedList<LongSupplier> old = list;
      list = new LinkedList<>();
      return old;
    }
  }

  private final Object id;
  private final LifeCycle lifeCycle;
  private final Daemon daemon;
  private final Replies replies = new Replies();
  private final RaftLogIndex repliedIndex;
  /** The interval at which held write replies are flushed. */
  private final TimeDuration batchInterval;

  ReplyFlusher(Object id, long repliedIndex, TimeDuration batchInterval) {
    this.id = id;
    final String name = id + "-ReplyFlusher";
    this.lifeCycle = new LifeCycle(name);
    this.daemon = Daemon.newBuilder()
        .setName(name)
        .setRunnable(this::run)
        .build();
    this.repliedIndex = new RaftLogIndex("repliedIndex", repliedIndex);
    this.batchInterval = batchInterval;
  }

  long getRepliedIndex() {
    return repliedIndex.get();
  }

  /** Hold a write reply for later batch flushing */
  void hold(LongSupplier replyMethod) {
    replies.add(replyMethod);
  }

  void start(long startIndex) {
    repliedIndex.updateToMax(startIndex, s -> LOG.debug("{}: {}", id, s));
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
        batchInterval.sleep();
        flush();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("{}: Interrupted ", daemon.getName(), e);
    } finally {
      // Flush remaining on exit
      flush();
    }
  }

  /** Flush all held replies and advance {@link #repliedIndex}. */
  private void flush() {
    CodeInjectionForTesting.execute(FLUSH, id, null);

    final LinkedList<LongSupplier> toFlush = replies.getAndSetNewList();
    if (toFlush.isEmpty()) {
      return;
    }
    long maxIndex = toFlush.removeLast().getAsLong();
    for (LongSupplier held : toFlush) {
      maxIndex = Math.max(maxIndex, held.getAsLong());
    }
    repliedIndex.updateToMax(maxIndex, s ->
        LOG.debug("{}: flushed {} replies, {}", id, toFlush.size(), s));
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
