/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class MetricRegistriesLoader {
  private static final Logger LOG = LoggerFactory.getLogger(MetricRegistriesLoader.class);

  static final String DEFAULT_CLASS = "org.apache.ratis.metrics.impl.MetricRegistriesImpl";

  private MetricRegistriesLoader() {
  }

  /**
   * Creates a {@link MetricRegistries} instance using the corresponding {@link MetricRegistries}
   * available to {@link ServiceLoader} on the classpath. If no instance is found, then default
   * implementation will be loaded.
   * @return A {@link MetricRegistries} implementation.
   */
  public static MetricRegistries load() {
    List<MetricRegistries> availableImplementations = getDefinedImplementations();
    return load(availableImplementations);
  }

  /**
   * Creates a {@link MetricRegistries} instance using the corresponding {@link MetricRegistries}
   * available to {@link ServiceLoader} on the classpath. If no instance is found, then default
   * implementation will be loaded.
   * @return A {@link MetricRegistries} implementation.
   */
  @VisibleForTesting
  static MetricRegistries load(List<MetricRegistries> registries) {
    if (registries.isEmpty()) {
      try {
        return ReflectionUtils.newInstance(Class.forName(DEFAULT_CLASS).asSubclass(MetricRegistries.class));
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Failed to load default MetricRegistries " + DEFAULT_CLASS, e);
      }
    }

    final MetricRegistries first = registries.get(0);
    if (registries.size() == 1) {
      // One and only one instance -- what we want/expect
      LOG.debug("Loaded {}", first.getClass());
    } else {
      // Tell the user they're doing something wrong, and choose the first impl.
      final List<? extends Class<?>> classes = registries.stream().map(Object::getClass).collect(Collectors.toList());
      LOG.warn("Found multiple MetricRegistries: {}. Using the first: {}", classes, first.getClass());
    }
    return first;
  }

  private static List<MetricRegistries> getDefinedImplementations() {
    ServiceLoader<MetricRegistries> loader = ServiceLoader.load(
        MetricRegistries.class,
        MetricRegistries.class.getClassLoader());
    List<MetricRegistries> availableFactories = new ArrayList<>();
    for (MetricRegistries impl : loader) {
      availableFactories.add(impl);
    }
    return availableFactories;
  }
}
