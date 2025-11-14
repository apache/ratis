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
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;

/** For supporting linearizable read. */
class ReadRequests {
  private static final Logger LOG = LoggerFactory.getLogger(ReadRequests.class);

  static class ReadIndexQueue {
    private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();
    /** The log index known to be applied. */
    private long lastAppliedIndex;
    /**
     * Map      : readIndex -> appliedIndexFuture (when completes, readIndex <= appliedIndex).
     * Invariant: all keys > lastAppliedIndex.
     */
    private final NavigableMap<Long, CompletableFuture<Long>> sorted = new TreeMap<>();

    private final TimeDuration readTimeout;

    ReadIndexQueue(long lastAppliedIndex, TimeDuration readTimeout) {
      this.lastAppliedIndex = lastAppliedIndex;
      this.readTimeout = readTimeout;
    }

    CompletableFuture<Long> add(long readIndex) {
      final CompletableFuture<Long> returned;
      final boolean create;
      synchronized (this) {
        if (readIndex <= lastAppliedIndex) {
          return CompletableFuture.completedFuture(lastAppliedIndex);
        }
        // The same as computeIfAbsent except that it also tells if a new value is created.
        final CompletableFuture<Long> existing = sorted.get(readIndex);
        create = existing == null;
        if (create) {
          returned = new CompletableFuture<>();
          sorted.put(readIndex, returned);
        } else {
          returned = existing;
        }
      }

      if (create) {
        scheduler.onTimeout(readTimeout, () -> handleTimeout(readIndex),
            LOG, () -> "Failed to handle read timeout for index " + readIndex);
      }
      return returned;
    }

    private void handleTimeout(long readIndex) {
      final CompletableFuture<Long> removed;
      synchronized (this) {
        removed = sorted.remove(readIndex);
      }
      if (removed == null) {
        return;
      }
      removed.completeExceptionally(new ReadException("Read timeout " + readTimeout + " for index " + readIndex));
    }


    /** Complete all the entries less than or equal to the given applied index. */
    synchronized void complete(long appliedIndex) {
      if (appliedIndex > lastAppliedIndex) {
        lastAppliedIndex = appliedIndex;
      } else {
        // appliedIndex <= lastAppliedIndex: nothing to do
        if (!sorted.isEmpty()) {
          // Assert: all keys > lastAppliedIndex.
          final long first = sorted.firstKey();
          Preconditions.assertTrue(first > lastAppliedIndex,
              () -> "first = " + first + " <= lastAppliedIndex = " + lastAppliedIndex);
        }
        return;
      }
      final NavigableMap<Long, CompletableFuture<Long>> headMap = sorted.headMap(appliedIndex, true);
      headMap.values().forEach(f -> f.complete(appliedIndex));
      headMap.clear();
    }
  }

  private final ReadIndexQueue readIndexQueue;

  ReadRequests(long appliedIndex, RaftProperties properties) {
    this.readIndexQueue = new ReadIndexQueue(appliedIndex, RaftServerConfigKeys.Read.timeout(properties));
  }

  LongConsumer getAppliedIndexConsumer() {
    return readIndexQueue::complete;
  }

  CompletableFuture<Long> waitToAdvance(long readIndex) {
    return readIndexQueue.add(readIndex);
  }
}
