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

import java.util.Queue;

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.util.DataQueue;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import static org.apache.ratis.server.metrics.RatisMetricNames.*;

public class RaftLogMetrics {

  private RatisMetricRegistry registry = null;

  RaftLogMetrics(RatisMetricRegistry ratisMetricRegistry) {
    this.registry = ratisMetricRegistry;
  }

  public RatisMetricRegistry getRegistry() {
    return registry;
  }

  public void addDataQueueSizeGauge(DataQueue queue) {
    registry.gauge(RAFT_LOG_DATA_QUEUE_SIZE, () -> () -> {
      //q.size() is O(1) operation
      return queue.size();
    });
  }

  public void addLogWorkerQueueSizeGauge(Queue queue) {
    registry.gauge(RAFT_LOG_WORKER_QUEUE_SIZE, () -> () -> queue.size());
  }

  public void addFlushBatchSizeGauge(MetricRegistry.MetricSupplier<Gauge> supplier) {
    registry.gauge(RAFT_LOG_SYNC_BATCH_SIZE, supplier);
  }

  private Timer getTimer(String timerName) {
    return registry.timer(timerName);
  }

  public Timer getFlushTimer() {
    return getTimer(RAFT_LOG_FLUSH_TIME);
  }

  public Timer getRaftLogSyncTimer() {
    return getTimer(RAFT_LOG_SYNC_TIME);
  }

  public void onRaftLogCacheHit() {
    registry.counter(RAFT_LOG_CACHE_HIT_COUNT).inc();
  }

  public void onRaftLogCacheMiss() {
    registry.counter(RAFT_LOG_CACHE_MISS_COUNT).inc();
  }

  public void onRaftLogFlush() {
    registry.counter(RAFT_LOG_FLUSH_COUNT).inc();
  }

  public void onRaftLogAppendEntry() {
    registry.counter(RAFT_LOG_APPEND_ENTRY_COUNT).inc();
  }

  public Timer getRaftLogAppendEntryTimer() {
    return getTimer(RAFT_LOG_APPEND_ENTRY_LATENCY);
  }

  public Timer getRaftLogQueueTimer() {
    return getTimer(RAFT_LOG_TASK_QUEUE_TIME);
  }

  public Timer getRaftLogEnqueueDelayTimer() {
    return getTimer(RAFT_LOG_TASK_ENQUEUE_DELAY);
  }

  public Timer getRaftLogTaskExecutionTimer(String taskName) {
    return getTimer(String.format(RAFT_LOG_TASK_EXECUTION_TIME, taskName));
  }

  public Timer getRaftLogReadEntryTimer() {
    return getTimer(RAFT_LOG_READ_ENTRY_LATENCY);
  }

  public Timer getRaftLogLoadSegmentTimer() {
    return getTimer(RAFT_LOG_LOAD_SEGMENT_LATENCY);
  }

  public Timer getRaftLogPurgeTimer() {
    return getTimer(RAFT_LOG_PURGE_METRIC);
  }

}
