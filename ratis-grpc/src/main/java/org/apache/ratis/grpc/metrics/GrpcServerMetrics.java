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

import org.apache.ratis.metrics.LongCounter;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;

import org.apache.ratis.thirdparty.com.codahale.metrics.Timer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

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
  public static final String RATIS_GRPC_METRICS_REQUESTS_COUNT = "num_requests";
  public static final String RATIS_GRPC_INSTALL_SNAPSHOT_COUNT = "num_install_snapshot";

  private final LongCounter requestRetry;
  private final LongCounter requestInstallSnapshot;

  private final Function<Boolean, LongCounter> requestCreate;

  private final Map<String, Function<Boolean, LongCounter>> requestSuccess = new ConcurrentHashMap<>();
  private final Map<String, Function<Boolean, LongCounter>> requestTimeout = new ConcurrentHashMap<>();

  private final Map<String, LongCounter> requestNotLeader = new ConcurrentHashMap<>();
  private final Map<String, LongCounter> requestInconsistency = new ConcurrentHashMap<>();

  public GrpcServerMetrics(String serverId) {
    registry = getMetricRegistryForGrpcServer(serverId);

    requestRetry = registry.counter(RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT);
    requestInstallSnapshot = registry.counter(RATIS_GRPC_INSTALL_SNAPSHOT_COUNT);

    requestCreate = newHeartbeatCounter(RATIS_GRPC_METRICS_REQUESTS_COUNT);
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
    requestRetry.inc();
  }

  public void onRequestCreate(boolean isHeartbeat) {
    requestCreate.apply(isHeartbeat).inc();
  }

  private Function<Boolean, LongCounter> newRequestSuccess(String follower) {
    final String prefix = String.format(RATIS_GRPC_METRICS_LOG_APPENDER_SUCCESS, follower);
    return newHeartbeatCounter(prefix);
  }

  public void onRequestSuccess(String follower, boolean isHeartbeat) {
    requestSuccess.computeIfAbsent(follower, this::newRequestSuccess).apply(isHeartbeat).inc();
  }

  private LongCounter newRequestNotLeader(String follower) {
    return registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_NOT_LEADER, follower));
  }

  public void onRequestNotLeader(String follower) {
    requestNotLeader.computeIfAbsent(follower, this::newRequestNotLeader).inc();
  }

  private LongCounter newRequestInconsistency(String follower) {
    return registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY, follower));
  }

  public void onRequestInconsistency(String follower) {
    requestInconsistency.computeIfAbsent(follower, this::newRequestInconsistency).inc();
  }

  private Function<Boolean, LongCounter> newRequestTimeout(String follower) {
    final String prefix = String.format(RATIS_GRPC_METRICS_LOG_APPENDER_TIMEOUT, follower);
    return newHeartbeatCounter(prefix);
  }

  public void onRequestTimeout(String follower, boolean isHeartbeat) {
    requestTimeout.computeIfAbsent(follower, this::newRequestTimeout).apply(isHeartbeat).inc();
  }

  public void addPendingRequestsCount(String follower, Supplier<Integer> pendinglogQueueSize) {
    registry.gauge(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_PENDING_COUNT, follower), () -> pendinglogQueueSize);
  }

  public void onInstallSnapshot() {
    requestInstallSnapshot.inc();
  }

  private Function<Boolean, LongCounter> newHeartbeatCounter(String prefix) {
    final LongCounter trueCounter = registry.counter(prefix + getHeartbeatSuffix(true));
    final LongCounter falseCounter = registry.counter(prefix + getHeartbeatSuffix(false));
    return b -> b? trueCounter : falseCounter;
  }

  public static String getHeartbeatSuffix(boolean heartbeat) {
    return heartbeat ? "_heartbeat" : "";
  }
}
