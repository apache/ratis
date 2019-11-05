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

import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;

public class RatisMetrics {
  public static final String RATIS_APPLICATION_NAME_METRICS = "ratis_core";
  public static final String RATIS_LOG_WORKER_METRICS_DESC = "Metrics for Log Worker";
  public static final String RATIS_LOG_WORKER_METRICS = "log_worker";
  public static final String RATIS_LEADER_ELECTION_METRICS = "leader_election";
  public static final String RATIS_LEADER_ELECTION_METRICS_DESC = "Metrics for Ratis Leader Election.";
  public static final String RATIS_STATEMACHINE_METRICS = "state_machine";
  public static final String RATIS_STATEMACHINE_METRICS_DESC = "Metrics for State Machine Updater";
  public static final String RATIS_SERVER_METRICS = "server";
  public static final String RATIS_SERVER_METRICS_DESC = "Metrics for Raft server";
  public static final String RATIS_LOG_APPENDER_METRICS = "log_appender";
  public static final String RATIS_LOG_APPENDER_METRICS_DESC = "Metrics for log appender";

  private static RatisMetricRegistry create(MetricRegistryInfo info) {
    Optional<RatisMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    if (metricRegistry.isPresent()) {
      return metricRegistry.get();
    }
    RatisMetricRegistry registry = MetricRegistries.global().create(info);
    return registry;
  }

  public static RatisMetricRegistry getMetricRegistryForLeaderElection(String serverId) {
    return create(new MetricRegistryInfo(serverId, RATIS_APPLICATION_NAME_METRICS, RATIS_LEADER_ELECTION_METRICS,
        RATIS_LEADER_ELECTION_METRICS_DESC));
  }

  public static RatisMetricRegistry getMetricRegistryForRaftServer(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_APPLICATION_NAME_METRICS, RATIS_SERVER_METRICS,
        RATIS_SERVER_METRICS_DESC));
  }

  public static RatisMetricRegistry getMetricRegistryForStateMachine(String serverId) {
    return create(new MetricRegistryInfo(serverId, RATIS_APPLICATION_NAME_METRICS,
        RATIS_STATEMACHINE_METRICS, RATIS_STATEMACHINE_METRICS_DESC));
  }

  public static RaftLogMetrics createMetricRegistryForLogWorker(String name) {
    RatisMetricRegistry ratisMetricRegistry = getMetricRegistryForLogWorker(name);
    if (ratisMetricRegistry == null) {
      ratisMetricRegistry = create(new MetricRegistryInfo(name, RATIS_APPLICATION_NAME_METRICS,
          RATIS_LOG_WORKER_METRICS, RATIS_LOG_WORKER_METRICS_DESC));
    }
    return new RaftLogMetrics(ratisMetricRegistry);
  }

  public static RatisMetricRegistry getMetricRegistryForLogWorker(String name) {
    Optional<RatisMetricRegistry> ratisMetricRegistry = MetricRegistries.global().get(
        new MetricRegistryInfo(name, RATIS_APPLICATION_NAME_METRICS, RATIS_LOG_WORKER_METRICS,
            RATIS_LOG_WORKER_METRICS_DESC));
    return ratisMetricRegistry.orElse(null);
  }

  public static RatisMetricRegistry getMetricRegistryForLogAppender(String serverId) {
    return create(new MetricRegistryInfo(serverId, RATIS_APPLICATION_NAME_METRICS,
        RATIS_LOG_APPENDER_METRICS, RATIS_LOG_APPENDER_METRICS_DESC));
  }
}
