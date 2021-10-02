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
package org.apache.ratis.ratisshell.conf;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Ratis shell configuration.
 */
public interface RatisShellConfiguration {

  /**
   * Gets the value for the given key in the {@link java.util.Properties};
   * if this key is not found, a RuntimeException is thrown.
   *
   * @param key the key to get the value for
   * @return the value for the given key
   */
  String get(PropertyKey key);

  /**
   * @param key the key to get the value for
   * @param defaultValue the value to return if no value is set for the specified key
   * @return the value
   */
  default String getOrDefault(PropertyKey key, String defaultValue) {
    return isSet(key) ? get(key) : defaultValue;
  }

  /**
   * Checks if the configuration contains a value for the given key.
   *
   * @param key the key to check
   * @return true if there is value for the key, false otherwise
   */
  boolean isSet(PropertyKey key);

  /**
   * @param key the key to check
   * @return true if there is value for the key set by user, false otherwise even when there is a
   *         default value for the key
   */
  boolean isSetByUser(PropertyKey key);

  /**
   * @return the keys configured by the configuration
   */
  Set<PropertyKey> keySet();

  /**
   * @return the keys set by user
   */
  Set<PropertyKey> userKeySet();

  /**
   * Gets the integer representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as an {@code int}
   */
  int getInt(PropertyKey key);

  /**
   * Gets the long representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code long}
   */
  long getLong(PropertyKey key);

  /**
   * Gets the double representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code double}
   */
  double getDouble(PropertyKey key);

  /**
   * Gets the float representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code float}
   */
  float getFloat(PropertyKey key);

  /**
   * Gets the boolean representation of the value for the given key.
   *
   * @param key the key to get the value for
   * @return the value for the given key as a {@code boolean}
   */
  boolean getBoolean(PropertyKey key);

  /**
   * Gets the value for the given key as a list.
   *
   * @param key the key to get the value for
   * @param delimiter the delimiter to split the values
   * @return the list of values for the given key
   */
  List<String> getList(PropertyKey key, String delimiter);

  /**
   * Gets the value for the given key as an enum value.
   *
   * @param key the key to get the value for
   * @param enumType the type of the enum
   * @param <T> the type of the enum
   * @return the value for the given key as an enum value
   */
  <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType);

  /**
   * Gets the value for the given key as a class.
   *
   * @param key the key to get the value for
   * @param <T> the type of the class
   * @return the value for the given key as a class
   */
  <T> Class<T> getClass(PropertyKey key);

  /**
   * Gets a set of properties that share a given common prefix key as a map. E.g., if A.B=V1 and
   * A.C=V2, calling this method with prefixKey=A returns a map of {B=V1, C=V2}, where B and C are
   * also valid properties. If no property shares the prefix, an empty map is returned.
   *
   * @param prefixKey the prefix key
   * @return a map from nested properties aggregated by the prefix
   */
  Map<String, String> getNestedProperties(PropertyKey prefixKey);

  /**
   * Gets a copy of the {@link RatisShellProperties} which back the {@link RatisShellConfiguration}.
   *
   * @return A copy of RatisShellProperties representing the configuration
   */
  RatisShellProperties copyProperties();

  /**
   * Validates the configuration.
   *
   * @throws IllegalStateException if invalid configuration is encountered
   */
  void validate();

  /**
   * @return hash of properties, if hashing is not supported, return empty string
   */
  default String hash() {
    return "";
  }
}
