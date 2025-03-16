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
package org.apache.ratis.server.protocol;

import org.apache.ratis.thirdparty.com.google.common.collect.MapMaker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * An implementation of {@link TermIndex}.
 * Note that this is not a public API.
 */
class TermIndexImpl implements TermIndex {
  private static final AtomicInteger OBJECT_COUNT = new AtomicInteger();

  /** Weak value cache -- the same {@link TermIndex} will not be created more than once. */
  static final class Cache {
    private Cache() {}

    private static <K, V> ConcurrentMap<K, V> newMap() {
      return new MapMaker().weakValues().makeMap();
    }

    /** Map: term -> (index -> {@link TermIndex}). */
    private static final ConcurrentMap<Long, ConcurrentMap<Long, TermIndexImpl>> MAP = new ConcurrentHashMap<>();

    private static TermIndex getOrCreate(long term, long index) {
      final ConcurrentMap<Long, TermIndexImpl> indexMap = MAP.computeIfAbsent(term, k -> newMap());
      final TermIndex computed = indexMap.computeIfAbsent(index, i -> new TermIndexImpl(term, i));
      if ((OBJECT_COUNT.get() & 0xFFFF) == 0) {
        cleanupEmptyInnerMaps(); // cleanup empty maps once in a while
      }
      return computed;
    }

    static int indexCount(long term) {
      final ConcurrentMap<Long, TermIndexImpl> indexMap = MAP.get(term);
      if (indexMap == null) {
        return 0;
      }

      // size() may return incorrect result; see Guava MapMaker javadoc
      int n = 0;
      for (Long ignored : indexMap.keySet()) {
        n++;
      }
      return n;
    }

    static void cleanupEmptyInnerMaps() {
      // isEmpty() may return incorrect result; see Guava MapMaker javadoc
      MAP.values().removeIf(e -> !e.entrySet().iterator().hasNext());
    }

    static int dump(Consumer<Object> out) {
      out.accept("TermIndex Cache:\n");
      int emptyCount = 0;
      for (Map.Entry<Long, ConcurrentMap<Long, TermIndexImpl>> entry : MAP.entrySet()) {
        final long term = entry.getKey();
        final ConcurrentMap<Long, TermIndexImpl> indexMap = entry.getValue();
        final int count = indexCount(term);
        if (count == 0) {
          emptyCount++;
        }

        out.accept("  term=" + term);
        out.accept(indexMap.keySet());
        out.accept(" count=" + indexCount(term));
        out.accept(", size=" + indexMap.size());
        out.accept("\n");
      }
      return emptyCount;
    }
  }

  static TermIndex valueOf(long term, long index) {
    return Cache.getOrCreate(term, index);
  }

  private final long term;
  private final long index;

  TermIndexImpl(long term, long index) {
    this.term = term;
    this.index = index;
    OBJECT_COUNT.incrementAndGet();
  }

  @Override
  public long getTerm() {
    return term;
  }

  @Override
  public long getIndex() {
    return index;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof TermIndexImpl)) {
      return false;
    }

    final TermIndexImpl that = (TermIndexImpl) obj;
    return this.getTerm() == that.getTerm()
        && this.getIndex() == that.getIndex();
  }

  @Override
  public int hashCode() {
    return Long.hashCode(term) ^ Long.hashCode(index);
  }

  private String longToString(long n) {
    return n >= 0L ? String.valueOf(n) : "~";
  }

  @Override
  public String toString() {
    return String.format("(t:%s, i:%s)", longToString(term), longToString(index));
  }
}