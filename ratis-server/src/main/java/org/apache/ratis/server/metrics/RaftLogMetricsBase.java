/*
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

package org.apache.ratis.server.metrics;

import com.codahale.metrics.Timer;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.server.raftlog.LogEntryHeader;

public class RaftLogMetricsBase extends RatisMetrics implements RaftLogMetrics {
  public static final String RATIS_LOG_WORKER_METRICS_DESC = "Metrics for Log Worker";
  public static final String RATIS_LOG_WORKER_METRICS = "log_worker";

  // Log Entry metrics
  public static final String METADATA_LOG_ENTRY_COUNT = "metadataLogEntryCount";
  public static final String CONFIG_LOG_ENTRY_COUNT = "configLogEntryCount";
  public static final String STATE_MACHINE_LOG_ENTRY_COUNT = "stateMachineLogEntryCount";

  //////////////////////////////
  // Raft Log Read Path Metrics
  /////////////////////////////
  // Time required to read a raft log entry from actual raft log file and create a raft log entry
  public static final String RAFT_LOG_READ_ENTRY_LATENCY = "readEntryLatency";
  // Time required to load and process raft log segments during restart
  public static final String RAFT_LOG_LOAD_SEGMENT_LATENCY = "segmentLoadLatency";

  public RaftLogMetricsBase(RaftGroupMemberId serverId) {
    this.registry = getLogWorkerMetricRegistry(serverId);
  }

  public static RatisMetricRegistry getLogWorkerMetricRegistry(RaftGroupMemberId serverId) {
    return create(new MetricRegistryInfo(serverId.toString(),
        RATIS_APPLICATION_NAME_METRICS,
        RATIS_LOG_WORKER_METRICS, RATIS_LOG_WORKER_METRICS_DESC));
  }

  private Timer getTimer(String timerName) {
    return registry.timer(timerName);
  }

  @Override
  public void onLogEntryCommitted(LogEntryHeader header) {
    switch (header.getLogEntryBodyCase()) {
      case CONFIGURATIONENTRY:
        registry.counter(CONFIG_LOG_ENTRY_COUNT).inc();
        return;
      case METADATAENTRY:
        registry.counter(METADATA_LOG_ENTRY_COUNT).inc();
        return;
      case STATEMACHINELOGENTRY:
        registry.counter(STATE_MACHINE_LOG_ENTRY_COUNT).inc();
        return;
      default:
    }
  }

  public Timer getRaftLogReadEntryTimer() {
    return getTimer(RAFT_LOG_READ_ENTRY_LATENCY);
  }

  public Timer getRaftLogLoadSegmentTimer() {
    return getTimer(RAFT_LOG_LOAD_SEGMENT_LATENCY);
  }
}
