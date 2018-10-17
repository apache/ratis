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
package org.apache.ratis.logservice.api;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An encapsulation of configuration for a LogService.
 */
public class LogServiceConfiguration {

  private ConcurrentHashMap<String, String> configMap =
      new ConcurrentHashMap<String, String>();

  /**
   * Ctor
   */
  public LogServiceConfiguration() {
  }

  /**
   * Fetches the value for the given key from the configuration. If there is no entry for
   * the given key, {@code null} is returned.
   *
   * @param key The configuration key
   */
  public String get(String key) {
    return configMap.get(key);
  }

  /**
   * Sets the given key and value into this configuration. The configuration key may
   * not be null. A null value removes the key from the configuration.
   *
   * @param key Configuration key, must be non-null
   * @param value Configuration value
   */
  public void set(String key, String value) {
    configMap.put(key,  value);
  }

  /**
   * Removes any entry with the given key from the configuration. If there is no entry
   * for the given key, this method returns without error. The provided key must be
   * non-null.
   *
   * @param key The configuration key, must be non-null
   * @return value
   */
  public String remove(String key) {
    return configMap.remove(key);
  }

  /**
   * Sets the collection of key-value pairs into the configuration. This is functionally
   * equivalent to calling {@link #set(String, String)} numerous time.
   */
  public void setMany(Iterable<Entry<String,String>> entries) {
    for (Entry<String, String> entry: entries ) {
      configMap.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Returns an immutable view over the configuration as a {@code Map}.
   */
  public Map<String,String> asMap() {
    return Collections.unmodifiableMap(configMap);
  }
}
