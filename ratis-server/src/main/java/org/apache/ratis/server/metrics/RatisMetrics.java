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

package org.apache.ratis.server.metrics;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.JVMMetrics;
import org.apache.ratis.metrics.MetricsReporting;
import org.apache.ratis.metrics.RatisMetricRegistry;

public class RatisMetrics {
  public final static String RATIS_LOG_WORKER_METRICS_DESC = "Ratis metrics";
  public final static String RATIS_LOG_WORKER_METRICS_CONTEXT = "ratis";
  static MetricsReporting metricsReporting = new MetricsReporting(500, TimeUnit.MILLISECONDS);

  public static RatisMetricRegistry createMetricRegistryForLogWorker(String name) {
    return create(new MetricRegistryInfo(name, RATIS_LOG_WORKER_METRICS_DESC,
        RATIS_LOG_WORKER_METRICS_CONTEXT));
  }

  public static RatisMetricRegistry getMetricRegistryForLogWorker(String name) {
    return MetricRegistries.global().get(new MetricRegistryInfo(name, RATIS_LOG_WORKER_METRICS_DESC,
        RATIS_LOG_WORKER_METRICS_CONTEXT)).get();
  }

  private static RatisMetricRegistry create(MetricRegistryInfo info) {
    Optional<RatisMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    if (metricRegistry.isPresent()) {
      return metricRegistry.get();
    }
    RatisMetricRegistry registry = MetricRegistries.global().create(info);
    metricsReporting
        .startMetricsReporter(registry, "RatisCore", MetricsReporting.MetricReporterType.JMX,
            MetricsReporting.MetricReporterType.HADOOP2);
    // JVM metrics
    JVMMetrics
        .startJVMReporting(500, TimeUnit.MILLISECONDS, MetricsReporting.MetricReporterType.JMX,
            MetricsReporting.MetricReporterType.HADOOP2);

    return registry;
  }
}
