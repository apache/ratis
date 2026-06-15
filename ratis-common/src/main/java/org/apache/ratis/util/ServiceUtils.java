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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Supplier;

/** Utility methods for {@link ServiceLoader}. */
public final class ServiceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceUtils.class);

  public static <T> T load(Class<T> serviceInterface, String defaultClass) {
    final Supplier<T> defaultInstance = () -> {
      try {
        return ReflectionUtils.newInstance(Class.forName(defaultClass).asSubclass(serviceInterface));
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Failed to load " + defaultClass, e);
      }
    };
    final List<T> providers = loadServiceProviders(serviceInterface);
    return load(serviceInterface, defaultInstance, providers);
  }

  public static <T> T load(Class<T> serviceInterface, Supplier<T> defaultInstance, List<T> loaded) {
    if (loaded.isEmpty()) {
      return defaultInstance.get();
    }

    final T first = loaded.get(0);
    if (loaded.size() == 1) {
      LOG.debug("Loaded {}", first.getClass());
    } else {
      // Warn that there are more than one services configured.
      final String classes = loaded.stream()
          .map(Object::getClass)
          .map(Class::getName)
          .reduce((a, b) -> a + ", " + b).orElse("");
      LOG.warn("Loaded {} duplicated services of {}: {}", loaded.size(), serviceInterface.getSimpleName(), classes);
      LOG.warn("Using the first: {}", first.getClass());
    }
    return first;
  }

  private static <T> List<T> loadServiceProviders(Class<T> serviceInterface) {
    final ServiceLoader<T> loader = ServiceLoader.load(serviceInterface, serviceInterface.getClassLoader());
    final List<T> loaded = new ArrayList<>();
    for (T impl : loader) {
      loaded.add(impl);
    }
    return loaded;
  }

  private ServiceUtils() {}
}
