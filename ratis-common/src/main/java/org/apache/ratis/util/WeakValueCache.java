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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.ratis.util.BiWeakValueCache.newMap;

/**
 * Weak Value Cache: {@link K} -> {@link V}.
 * <p>
 * Note that the cached values are weakly referenced.
 * A cached value could be garage-collected (i.e. evicted from the cache)
 * when there are no external (strong) references.
 * <p>
 * For key types with two components, use {@link BiWeakValueCache}.
 *
 * @param <K> the type of the keys.
 * @param <V> the type to be cached values.
 */
public final class WeakValueCache<K, V> {
  private final String keyName;
  private final String name;

  /** For constructing a value from a key. */
  private final Function<K, V> constructor;
  /** Count the number of values constructed. */
  private final AtomicInteger constructionCount = new AtomicInteger(0);

  /** Map: {@link K} -> {@link V}. */
  private final ConcurrentMap<K, V> map = newMap();

  /**
   * Create a cache for mapping {@link K} keys to {@link V} values.
   *
   * @param keyName the name of the key.
   * @param constructor for constructing {@link V} values.
   */
  public WeakValueCache(String keyName, Function<K, V> constructor) {
    this.keyName = keyName;
    this.name = keyName + "-cache";
    this.constructor = constructor;
  }

  private V construct(K key) {
    final V constructed = constructor.apply(key);
    Objects.requireNonNull(constructed, "constructed == null");
    constructionCount.incrementAndGet();
    return constructed;
  }

  /**
   * If the given key is in the cache, return its cached values.
   * Otherwise, create a new value, put it in the cache and then return it.
   */
  public V getOrCreate(K key) {
    Objects.requireNonNull(key, () -> keyName + " (key) == null");
    return map.computeIfAbsent(key, this::construct);
  }

  List<V> getValues() {
    return new ArrayList<>(map.values());
  }

  @Override
  public String toString() {
    return name;
  }
}
