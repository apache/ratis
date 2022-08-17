/*
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

import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.apache.ratis.util.TimeDuration;

/**
 * Helper class to add JVM metrics.
 */
public interface JVMMetrics {
  static void initJvmMetrics(TimeDuration consoleReportRate) {
    final MetricRegistries registries = MetricRegistries.global();
    JVMMetrics.addJvmMetrics(registries);
    registries.enableConsoleReporter(consoleReportRate);
    registries.enableJmxReporter();
  }

  static void addJvmMetrics(MetricRegistries registries) {
    MetricRegistryInfo info = new MetricRegistryInfo("jvm", "ratis_jvm", "jvm", "jvm metrics");

    RatisMetricRegistry registry = registries.create(info);

    registry.registerAll("gc", new GarbageCollectorMetricSet());
    registry.registerAll("memory", new MemoryUsageGaugeSet());
    registry.registerAll("threads", new ThreadStatesGaugeSet());
    registry.registerAll("classLoading", new ClassLoadingGaugeSet());
  }
}
