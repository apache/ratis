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

import static org.apache.ratis.server.metrics.RaftLogMetrics.FOLLOWER_MATCH_INDEX;
import static org.apache.ratis.server.metrics.RaftLogMetrics.FOLLOWER_NEXT_INDEX;
import static org.apache.ratis.server.metrics.RaftLogMetrics.FOLLOWER_RPC_RESP_TIME;

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.server.impl.FollowerInfo;

public final class LogAppenderMetrics extends RatisMetrics {
  public static final String RATIS_LOG_APPENDER_METRICS = "log_appender";
  public static final String RATIS_LOG_APPENDER_METRICS_DESC = "Metrics for log appender";

  public LogAppenderMetrics(RaftGroupMemberId groupMemberId) {
    registry = getMetricRegistryForLogAppender(groupMemberId.toString());
  }

  private RatisMetricRegistry getMetricRegistryForLogAppender(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_APPLICATION_NAME_METRICS,
        RATIS_LOG_APPENDER_METRICS, RATIS_LOG_APPENDER_METRICS_DESC));
  }

  public void addFollowerGauges(FollowerInfo followerInfo) {
    registry.gauge(String.format(FOLLOWER_NEXT_INDEX,
        followerInfo.getPeer().getId().toString()),
        () -> followerInfo::getNextIndex);
    registry.gauge(String.format(FOLLOWER_MATCH_INDEX, followerInfo.getPeer().getId().toString()),
        () -> followerInfo::getMatchIndex);
    registry.gauge(String.format(FOLLOWER_RPC_RESP_TIME, followerInfo.getPeer().getId().toString()),
        () -> () -> followerInfo.getLastRpcTime().elapsedTimeMs());
  }
}
