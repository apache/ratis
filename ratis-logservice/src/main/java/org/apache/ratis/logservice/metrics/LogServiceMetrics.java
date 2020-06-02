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

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;

import com.codahale.metrics.Timer;

public final class LogServiceMetrics extends RatisMetrics {
  public static final String RATIS_LOG_STATEMACHINE_METRICS = "log_statemachine";
  public static final String RATIS_LOG_SERVICE_METRICS = "ratis_log_service";
  public static final String RATIS_LOG_SERVICE_METRICS_DESC = "Ratis log service metrics";

  public LogServiceMetrics(String logName, String serverId) {
    registry = getMetricRegistryForLogService(logName, serverId);
  }

  private RatisMetricRegistry getMetricRegistryForLogService(String logName, String serverId) {
    return create(new MetricRegistryInfo(logName + "." + serverId,
        RATIS_LOG_SERVICE_METRICS,
        RATIS_LOG_STATEMACHINE_METRICS, RATIS_LOG_SERVICE_METRICS_DESC));
  }

  public Timer getTimer(String name) {
    return registry.timer(name);
  }
}
