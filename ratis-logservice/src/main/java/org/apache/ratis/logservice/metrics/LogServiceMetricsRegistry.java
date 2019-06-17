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

package org.apache.ratis.logservice.metrics;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.JmxReporter;
import com.github.joshelser.dropwizard.metrics.hadoop.HadoopMetrics2Reporter;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.MetricsReporting;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.impl.RatisMetricRegistryImpl;

public class LogServiceMetricsRegistry {
  public static final String RATIS_LOG_SERVICE_METRICS_CONTEXT = "ratis_log_service";
      //context needs to be small case for hadoop2metrics
  public static final String RATIS_LOG_SERVICE_METRICS_DESC = "Ratis log service metrics";
  public static final String RATIS_LOG_SERVICE_META_DATA_METRICS_CONTEXT =
      "ratis_log_service_metadata";
  public static final String RATIS_LOG_SERVICE_META_DATA_METRICS_DESC =
      "Ratis log service metadata metrics";
  public static final String JMX_DOMAIN = "ratis_log_service";
  static MetricsReporting metricsReporting = new MetricsReporting(500, TimeUnit.MILLISECONDS);

  public static RatisMetricRegistry createMetricRegistryForLogService(String logName) {
    return create(new MetricRegistryInfo(logName, RATIS_LOG_SERVICE_METRICS_DESC,
        RATIS_LOG_SERVICE_METRICS_CONTEXT));
  }

  public static RatisMetricRegistry getMetricRegistryForLogService(String logName) {
    return MetricRegistries.global().get(
        new MetricRegistryInfo(logName, RATIS_LOG_SERVICE_METRICS_DESC,
            RATIS_LOG_SERVICE_METRICS_CONTEXT)).get();
  }

  public static RatisMetricRegistry createMetricRegistryForLogServiceMetaData(String className) {
    return create(new MetricRegistryInfo(className, RATIS_LOG_SERVICE_META_DATA_METRICS_DESC,
        RATIS_LOG_SERVICE_META_DATA_METRICS_CONTEXT));
  }

  public static RatisMetricRegistry getMetricRegistryForLogServiceMetaData(String className) {
    return MetricRegistries.global().get(
        new MetricRegistryInfo(className, RATIS_LOG_SERVICE_META_DATA_METRICS_DESC,
            RATIS_LOG_SERVICE_META_DATA_METRICS_CONTEXT)).get();
  }

  private static RatisMetricRegistry create(MetricRegistryInfo info) {
    RatisMetricRegistry registry = MetricRegistries.global().create(info);
    metricsReporting
        .startMetricsReporter(registry, JMX_DOMAIN, MetricsReporting.MetricReporterType.JMX,
            MetricsReporting.MetricReporterType.HADOOP2);
    return registry;
  }

}
