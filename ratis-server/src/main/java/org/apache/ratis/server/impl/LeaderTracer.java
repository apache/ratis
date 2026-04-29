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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.SpanContextProto;
import org.apache.ratis.trace.TraceUtils;
import org.apache.ratis.util.AutoCloseableLock;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class LeaderTracer {
  private final AppendEntriesSpans appendEntriesSpans;
  public LeaderTracer() {
    appendEntriesSpans = new AppendEntriesSpans();
  }
  /**
   * Client-originated trace context keyed by log index and propagated in appendEntries requests,
   * so follower appendEntries spans can join the same trace as the client write.
   */
  static class AppendEntriesSpans {
    private final NavigableMap<Long, SpanContextProto> sorted = new TreeMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    SpanContextProto get(long first, long last) {
      try (AutoCloseableLock ignored = AutoCloseableLock.acquire(lock.readLock())) {
        for (SpanContextProto sc : sorted.subMap(first, true, last, true).values()) {
          if (sc != null && !sc.getContextMap().isEmpty()) {
            return sc;
          }
        }
      }
      return null;
    }

    void put(long index, SpanContextProto spanContext) {
      try (AutoCloseableLock ignored = AutoCloseableLock.acquire(lock.writeLock())) {
        sorted.put(index, spanContext);
      }
    }

    void remove(long index) {
      try (AutoCloseableLock ignored = AutoCloseableLock.acquire(lock.writeLock())) {
        sorted.remove(index);
      }
    }

    void clear() {
      try (AutoCloseableLock ignored = AutoCloseableLock.acquire(lock.writeLock())) {
        sorted.clear();
      }
    }
  }

  void tracePendingRequest(PendingRequest pending) {
    if (pending == null || !TraceUtils.isEnabled()) {
      return;
    }
    final SpanContextProto spanContext = pending.getRequest().getSpanContext();
    if (spanContext != null && !spanContext.getContextMap().isEmpty()) {
      appendEntriesSpans.put(pending.getTermIndex().getIndex(), spanContext);
    }
  }

  void removePendingRequest(PendingRequest pending) {
    appendEntriesSpans.remove(pending.getTermIndex().getIndex());
  }

  SpanContextProto traceAppendEntries(List<LogEntryProto> entries) {
    if (entries == null || entries.isEmpty()) {
      return null;
    }
    final long first = entries.get(0).getIndex();
    final long last = entries.get(entries.size() - 1).getIndex();
    return appendEntriesSpans.get(first, last);
  }

  void close() {
    appendEntriesSpans.clear();
  }
}