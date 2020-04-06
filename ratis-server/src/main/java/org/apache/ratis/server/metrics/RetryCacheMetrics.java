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

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.server.impl.RaftServerImpl;

/**
 * Class to update the metrics related to retry cache.
 */
public final class RetryCacheMetrics extends RatisMetrics {

  public static final String RATIS_RETRY_CACHE_METRICS = "retry_cache";
  public static final String RATIS_RETRY_CACHE_METRICS_DESC = "Metrics for Ratis Retry Cache.";

  public static final String RETRY_CACHE_ENTRY_COUNT_METRIC = "retryCacheEntryCount";
  public static final String RETRY_CACHE_HIT_COUNT_METRIC = "retryCacheHitCount";
  public static final String RETRY_CACHE_HIT_RATE_METRIC = "retryCacheHitRate";
  public static final String RETRY_CACHE_MISS_COUNT_METRIC = "retryCacheMissCount";
  public static final String RETRY_CACHE_MISS_RATE_METRIC = "retryCacheMissRate";

  private RetryCacheMetrics(RaftServerImpl raftServer) {
    this.registry = getMetricRegistryForRetryCache(raftServer.getMemberId().toString());
    registry.gauge(RETRY_CACHE_ENTRY_COUNT_METRIC, () -> () -> raftServer.getRetryCache().size());
    registry.gauge(RETRY_CACHE_HIT_COUNT_METRIC, () -> () -> raftServer.getRetryCache().stats().hitCount());
    registry.gauge(RETRY_CACHE_HIT_RATE_METRIC, () -> () -> raftServer.getRetryCache().stats().hitRate());
    registry.gauge(RETRY_CACHE_MISS_COUNT_METRIC, () -> () -> raftServer.getRetryCache().stats().missCount());
    registry.gauge(RETRY_CACHE_MISS_RATE_METRIC, () -> () -> raftServer.getRetryCache().stats().missRate());
  }

  private RatisMetricRegistry getMetricRegistryForRetryCache(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_APPLICATION_NAME_METRICS, RATIS_RETRY_CACHE_METRICS,
        RATIS_RETRY_CACHE_METRICS_DESC));
  }

  public static RetryCacheMetrics getRetryCacheMetrics(RaftServerImpl raftServer) {
    return new RetryCacheMetrics(raftServer);
  }
}
