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
package org.apache.ratis.grpc.metrics;

import com.codahale.metrics.Gauge;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Timer;

public class GrpcServerMetrics extends RatisMetrics {
  private static final String RATIS_GRPC_METRICS_APP_NAME = "ratis_grpc";
  private static final String RATIS_GRPC_METRICS_COMP_NAME = "log_appender";
  private static final String RATIS_GRPC_METRICS_DESC = "Metrics for Ratis Grpc Log Appender";

  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_LATENCY =
      "%s_latency";
  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_SUCCESS =
      "%s_success_reply_count";
  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_NOT_LEADER =
      "%s_not_leader_reply_count";
  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY =
      "%s_inconsistency_reply_count";
  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_TIMEOUT =
      "%s_append_entry_timeout_count";
  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_PENDING_COUNT
      = "%s_pending_log_requests_count";

  public static final String RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT = "num_retries";
  public static final String RATIS_GRPC_METRICS_REQUESTS_TOTAL = "num_requests";
  public static final String RATIS_GRPC_INSTALL_SNAPSHOT_COUNT = "num_install_snapshot";

  public GrpcServerMetrics(String serverId) {
    registry = getMetricRegistryForGrpcServer(serverId);
  }

  private RatisMetricRegistry getMetricRegistryForGrpcServer(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_GRPC_METRICS_APP_NAME,
        RATIS_GRPC_METRICS_COMP_NAME, RATIS_GRPC_METRICS_DESC));
  }

  public Timer getGrpcLogAppenderLatencyTimer(String follower,
      boolean isHeartbeat) {
    return registry.timer(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_LATENCY + getHeartbeatSuffix(isHeartbeat),
        follower));
  }

  public void onRequestRetry() {
    registry.counter(RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT).inc();
  }

  public void onRequestCreate(boolean isHeartbeat) {
    registry.counter(RATIS_GRPC_METRICS_REQUESTS_TOTAL + getHeartbeatSuffix(isHeartbeat)).inc();
  }

  public void onRequestSuccess(String follower, boolean isHearbeat) {
    registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_SUCCESS + getHeartbeatSuffix(isHearbeat),
        follower)).inc();
  }

  public void onRequestNotLeader(String follower) {
    registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_NOT_LEADER, follower)).inc();
  }

  public void onRequestInconsistency(String follower) {
    registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY, follower)).inc();
  }

  public void onRequestTimeout(String follower, boolean isHeartbeat) {
    registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_TIMEOUT + getHeartbeatSuffix(isHeartbeat),
        follower)).inc();
  }

  public void addPendingRequestsCount(String follower,
      Gauge pendinglogQueueSize) {
    registry.gauge(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_PENDING_COUNT, follower), () -> pendinglogQueueSize);
  }

  public void onInstallSnapshot() {
    registry.counter(RATIS_GRPC_INSTALL_SNAPSHOT_COUNT).inc();
  }

  public static String getHeartbeatSuffix(boolean heartbeat) {
    return heartbeat ? "_heartbeat" : "";
  }

  @VisibleForTesting
  public RatisMetricRegistry getRegistry() {
    return registry;
  }
}
