/**
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
package org.apache.ratis.conf;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A generic parameter map.
 * The difference between this class and {@link RaftProperties} is that
 * {@link RaftProperties} is {@link String} based, i.e. properties are strings,
 * while this class is {@link Object} based, i.e. parameters can be any objects.
 *
 * Null keys or null values are not supported.
 *
 * This class is thread safe.
 */
public class Parameters {
  private final Map<String, Object> map = new ConcurrentHashMap<>();

  /** Put the key-value pair to the map. */
  public <T> T put(String key, T value, Class<T> valueClass) {
    return valueClass.cast(map.put(
        Objects.requireNonNull(key, "key is null"),
        Objects.requireNonNull(value, () -> "value is null, key=" + key)));
  }

  /**
   * @param <T> The value type.
   * @return The value mapped to the given key;
   *         or return null if the key does not map to any value.
   * @throws IllegalArgumentException if the mapped value is not an instance of the given class.
   */
  public <T> T get(String key, Class<T> valueClass) {
    final Object value = map.get(Objects.requireNonNull(key, "key is null"));
    return valueClass.cast(value);
  }

  /**
   * The same as {@link #get(String, Class)} except that this method throws
   * a {@link NullPointerException} if the key does not map to any value.
   */
  public <T> T getNonNull(String key, Class<T> valueClass) {
    final T value = get(key, valueClass);
    if (value != null) {
      return value;
    }
    throw new NullPointerException("The key " + key + " does not map to any value.");
  }
}
