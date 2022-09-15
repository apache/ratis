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

import org.apache.ratis.metrics.LongCounter;
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

  private final LongCounter configLogEntryCount = getRegistry().counter(CONFIG_LOG_ENTRY_COUNT);
  private final LongCounter metadataLogEntryCount = getRegistry().counter(METADATA_LOG_ENTRY_COUNT);
  private final LongCounter stateMachineLogEntryCount = getRegistry().counter(STATE_MACHINE_LOG_ENTRY_COUNT);

  public RaftLogMetricsBase(RaftGroupMemberId serverId) {
    super(createRegistry(serverId));
  }

  public static RatisMetricRegistry createRegistry(RaftGroupMemberId serverId) {
    return create(new MetricRegistryInfo(serverId.toString(),
        RATIS_APPLICATION_NAME_METRICS,
        RATIS_LOG_WORKER_METRICS, RATIS_LOG_WORKER_METRICS_DESC));
  }

  @Override
  public void onLogEntryCommitted(LogEntryHeader header) {
    switch (header.getLogEntryBodyCase()) {
      case CONFIGURATIONENTRY:
        configLogEntryCount.inc();
        return;
      case METADATAENTRY:
        metadataLogEntryCount.inc();
        return;
      case STATEMACHINELOGENTRY:
        stateMachineLogEntryCount.inc();
        return;
      default:
    }
  }
}
