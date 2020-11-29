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

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RatisMetrics {
  static final Logger LOG = LoggerFactory.getLogger(RatisMetrics.class);
  public static final String RATIS_APPLICATION_NAME_METRICS = "ratis";

  @SuppressWarnings("VisibilityModifier")
  protected RatisMetricRegistry registry;

  protected static RatisMetricRegistry create(MetricRegistryInfo info) {
    Optional<RatisMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    return metricRegistry.orElseGet(() -> {
      LOG.info("Creating Metrics Registry : {}", info.getName());
      return MetricRegistries.global().create(info);
    });
  }

  public void unregister() {
    MetricRegistryInfo info = registry.getMetricRegistryInfo();
    LOG.info("Unregistering Metrics Registry : {}", info.getName());
    Optional<RatisMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    if (metricRegistry.isPresent()) {
      MetricRegistries.global().remove(info);
    }
  }

  public RatisMetricRegistry getRegistry() {
    return registry;
  }
}
