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
package org.apache.ratis.metrics;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

public class JVMMetrics {
  static MetricRegistryInfo info = new MetricRegistryInfo("jvm", "ratis_jvm", "jvm", "jvm metrics");
  static RatisMetricRegistry registry = MetricRegistries.global().create(info);
  static {
    registry.registerAll("gc", new GarbageCollectorMetricSet());
    registry.registerAll("memory", new MemoryUsageGaugeSet());
    registry.registerAll("threads", new ThreadStatesGaugeSet());
    registry.registerAll("classLoading", new ClassLoadingGaugeSet());
  }

  public static RatisMetricRegistry getRegistry() {
    return registry;
  }

  public static void startJVMReporting(long period, TimeUnit unit,
      MetricsReporting.MetricReporterType... reporting){
    MetricsReporting metricsReporting = new MetricsReporting(period,unit);
    metricsReporting.startMetricsReporter(getRegistry(), reporting);
  }
}
