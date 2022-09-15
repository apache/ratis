/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RatisMetrics {
  static final Logger LOG = LoggerFactory.getLogger(RatisMetrics.class);
  public static final String RATIS_APPLICATION_NAME_METRICS = "ratis";

  public static String getHeartbeatSuffix(boolean heartbeat) {
    return heartbeat ? "_heartbeat" : "";
  }

  private static <T> Function<Boolean, T> newHeartbeatFunction(String prefix, Function<String, T> function) {
    final T trueValue = function.apply(prefix + getHeartbeatSuffix(true));
    final T falseValue = function.apply(prefix + getHeartbeatSuffix(false));
    return b -> b? trueValue: falseValue;
  }

  protected static <T extends Enum<T>> Map<T, Map<String, LongCounter>> newCounterMaps(Class<T> clazz) {
    final EnumMap<T,Map<String, LongCounter>> maps = new EnumMap<>(clazz);
    Arrays.stream(clazz.getEnumConstants()).forEach(t -> maps.put(t, new ConcurrentHashMap<>()));
    return Collections.unmodifiableMap(maps);
  }

  protected static <T extends Enum<T>> Map<T, Timekeeper> newTimerMap(
      Class<T> clazz, Function<T, Timekeeper> constructor) {
    final EnumMap<T, Timekeeper> map = new EnumMap<>(clazz);
    Arrays.stream(clazz.getEnumConstants()).forEach(t -> map.put(t, constructor.apply(t)));
    return Collections.unmodifiableMap(map);
  }

  protected static RatisMetricRegistry create(MetricRegistryInfo info) {
    Optional<RatisMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    return metricRegistry.orElseGet(() -> {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating Metrics Registry : {}", info.getName());
      }
      return MetricRegistries.global().create(info);
    });
  }

  private final RatisMetricRegistry registry;

  protected RatisMetrics(RatisMetricRegistry registry) {
    this.registry = registry;
  }

  public void unregister() {
    MetricRegistryInfo info = registry.getMetricRegistryInfo();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unregistering Metrics Registry : {}", info.getName());
    }
    Optional<RatisMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    if (metricRegistry.isPresent()) {
      MetricRegistries.global().remove(info);
    }
  }

  public final RatisMetricRegistry getRegistry() {
    return registry;
  }

  protected Function<Boolean, Timekeeper> newHeartbeatTimer(String prefix) {
    return newHeartbeatFunction(prefix, getRegistry()::timer);
  }

  protected Function<Boolean, LongCounter> newHeartbeatCounter(String prefix) {
    return newHeartbeatFunction(prefix, getRegistry()::counter);
  }
}
