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

import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;

public final class LogServiceMetricsRegistry {
  public static final String RATIS_LOG_STATEMACHINE_METRICS = "log_statemachine";
  public static final String RATIS_LOG_SERVICE_METRICS = "ratis_log_service";
  public static final String RATIS_LOG_SERVICE_METRICS_DESC = "Ratis log service metrics";
  public static final String RATIS_LOG_SERVICE_META_DATA_METRICS = "metadata_statemachine";
  public static final String RATIS_LOG_SERVICE_META_DATA_METRICS_DESC =
      "Ratis log service metadata metrics";

  public static RatisMetricRegistry createMetricRegistryForLogService(String logName,
      String serverId) {
    return create(new MetricRegistryInfo(logName + "." + serverId, RATIS_LOG_SERVICE_METRICS,
        RATIS_LOG_STATEMACHINE_METRICS, RATIS_LOG_SERVICE_METRICS_DESC));
  }

  public static RatisMetricRegistry getMetricRegistryForLogService(String logName,
      String serverId) {
    return MetricRegistries.global().get(
        new MetricRegistryInfo(logName + "." + serverId, RATIS_LOG_SERVICE_METRICS,
            RATIS_LOG_STATEMACHINE_METRICS, RATIS_LOG_SERVICE_METRICS_DESC)).get();
  }

  public static RatisMetricRegistry createMetricRegistryForLogServiceMetaData(String serverId) {
    return create(new MetricRegistryInfo(serverId, RATIS_LOG_SERVICE_METRICS,
        RATIS_LOG_SERVICE_META_DATA_METRICS, RATIS_LOG_SERVICE_META_DATA_METRICS_DESC));
  }

  public static RatisMetricRegistry getMetricRegistryForLogServiceMetaData(String serverId) {
    return MetricRegistries.global().get(new MetricRegistryInfo(serverId, RATIS_LOG_SERVICE_METRICS,
        RATIS_LOG_SERVICE_META_DATA_METRICS, RATIS_LOG_SERVICE_META_DATA_METRICS_DESC)).get();
  }

  private static RatisMetricRegistry create(MetricRegistryInfo info) {
    RatisMetricRegistry registry = MetricRegistries.global().create(info);
    return registry;
  }

  private LogServiceMetricsRegistry() {
    throw new UnsupportedOperationException("no instances");
  }

}
