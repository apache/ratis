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

import static org.apache.ratis.server.metrics.RatisMetricNames.FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC;

import java.util.HashMap;
import java.util.Map;

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.server.impl.RaftServerImpl;

/**
 * Metric Registry for Heartbeat. One instance per leader per group.
 */
public final class HeartbeatMetrics {

  private RatisMetricRegistry registry = null;
  private Map<String, Long> followerLastHeartbeatElapsedTimeMap = new HashMap<>();

  public static HeartbeatMetrics getHeartbeatMetrics(RaftServerImpl raftServer) {
     return new HeartbeatMetrics(raftServer.getMemberId().toString());
  }

  private HeartbeatMetrics(String serverId) {
    registry = RatisMetrics.getMetricRegistryForHeartbeat(serverId);
  }

  /**
   * Register a follower with this Heartbeat Metrics registry instance.
   * @param followerName Name of the follower.
   */
  public void addFollower(String followerName) {
    String followerMetricKey = String.format(FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC, followerName);
    followerLastHeartbeatElapsedTimeMap.put(followerName, 0L);
    registry.gauge(followerMetricKey, () -> () -> followerLastHeartbeatElapsedTimeMap.get(followerName));
  }

  /**
   * Record heartbeat elapsed time for a follower within a Raft group.
   * @param followerName Name of the follower.
   * @param elapsedTime Elapsed time in Nanos.
   */
  public void recordFollowerHeartbeatElapsedTime(String followerName, long elapsedTime) {
    followerLastHeartbeatElapsedTimeMap.put(followerName, elapsedTime);
  }
}
