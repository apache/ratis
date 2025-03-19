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
package org.apache.ratis.util;

import org.apache.ratis.thirdparty.com.google.common.collect.MapMaker;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Weak Value Cache: ({@link OUTER}, {@link INNER}) -> {@link T}.
 * <p>
 * Note that the cached values are weakly referenced.
 * A cached value could be garage-collected (i.e. evicted from the cache)
 * when there are no external (strong) references.
 *
 * @param <OUTER> the type of the outer keys.
 * @param <INNER> the type of the inner keys.
 * @param <T> the type to be cached.
 */
public final class BiWeakValueCache<OUTER, INNER, T> {
  private static <K, V> ConcurrentMap<K, V> newMap() {
    return new MapMaker().weakValues().makeMap();
  }

  private final String outerName;
  private final String innerName;
  private final String name;

  /** For constructing {@link T} values from ({@link OUTER}, {@link INNER}) keys. */
  private final BiFunction<OUTER, INNER, T> constructor;
  /** Count the number of {@link T} values constructed. */
  private final AtomicInteger valueCount = new AtomicInteger(0);

  /**
   * Actual map {@link OUTER} -> ({@link INNER} -> {@link T})
   * for the logical view ({@link OUTER}, {@link INNER}) -> {@link T}.
   */
  private final ConcurrentMap<OUTER, ConcurrentMap<INNER, T>> map = new ConcurrentHashMap<>();

  /**
   * Create a cache for mapping ({@link OUTER}, {@link INNER}) keys to {@link T} values.
   *
   * @param outerName the name of the outer long.
   * @param innerName the name of the inner long.
   * @param constructor for constructing {@link T} values.
   */
  public BiWeakValueCache(String outerName, String innerName, BiFunction<OUTER, INNER, T> constructor) {
    this.outerName = outerName;
    this.innerName = innerName;
    this.name = "(" + outerName + ", " + innerName + ")-cache";
    this.constructor = constructor;
  }

  private T construct(OUTER outer, INNER inner) {
    final T constructed = constructor.apply(outer, inner);
    Objects.requireNonNull(constructed, "constructed == null");
    valueCount.incrementAndGet();
    return constructed;
  }

  /**
   * If the key ({@link OUTER}, {@link INNER}) is in the cache, return the cached values.
   * Otherwise, create a new value and then return it.
   */
  public T getOrCreate(OUTER outer, INNER inner) {
    Objects.requireNonNull(outer, () -> outerName + " (outer) == null");
    Objects.requireNonNull(inner, () -> innerName + " (inner) == null");
    final ConcurrentMap<INNER, T> innerMap = map.computeIfAbsent(outer, k -> newMap());
    final T computed = innerMap.computeIfAbsent(inner, i -> construct(outer, i));
    if ((valueCount.get() & 0xFFF) == 0) {
      cleanupEmptyInnerMaps(); // cleanup empty maps once in a while
    }
    return computed;
  }

  /** @return the value count for the given outer key.  */
  int count(OUTER outer) {
    final ConcurrentMap<INNER, T> innerMap = map.get(outer);
    if (innerMap == null) {
      return 0;
    }

    // size() may return incorrect result; see Guava MapMaker javadoc
    int n = 0;
    for (INNER ignored : innerMap.keySet()) {
      n++;
    }
    return n;
  }

  void cleanupEmptyInnerMaps() {
    // isEmpty() may return incorrect result; see Guava MapMaker javadoc
    map.values().removeIf(e -> !e.entrySet().iterator().hasNext());
  }

  @Override
  public String toString() {
    return name;
  }

  /** The cache content for debugging. */
  int dump(Consumer<String> out) {
    out.accept(name + ":\n");
    int emptyCount = 0;
    for (Map.Entry<OUTER, ConcurrentMap<INNER, T>> entry : map.entrySet()) {
      final OUTER outer = entry.getKey();
      final ConcurrentMap<INNER, T> innerMap = entry.getValue();
      final int count = count(outer);
      if (count == 0) {
        emptyCount++;
      }

      out.accept("  " + outerName + ":" + outer);
      out.accept(", " + innerName + ":" + innerMap.keySet());
      out.accept(", count=" + count);
      out.accept(", size=" + innerMap.size());
      out.accept("\n");
    }
    out.accept("  emptyCount=" + emptyCount);
    out.accept("\n");
    return emptyCount;
  }
}
