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

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.util.DataQueue;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class RaftLogMetrics extends RatisMetrics {

  public static final String RATIS_LOG_WORKER_METRICS_DESC = "Metrics for Log Worker";
  public static final String RATIS_LOG_WORKER_METRICS = "log_worker";

  //////////////////////////////
  // Raft Log Write Path Metrics
  /////////////////////////////
  // Time taken to flush log.
  public static final String RAFT_LOG_FLUSH_TIME = "flushTime";
  // Number of times raft log is flushed
  public static final String RAFT_LOG_FLUSH_COUNT = "flushCount";
  // Time taken to sync raft log.
  public static final String RAFT_LOG_SYNC_TIME = "syncTime";
  // Raft log data queue size which at any time gives the number of raft log related operations in the queue.
  public static final String RAFT_LOG_DATA_QUEUE_SIZE = "dataQueueSize";
  // Raft log worker queue size which at any time gives number of committed entries that are to be synced.
  public static final String RAFT_LOG_WORKER_QUEUE_SIZE = "workerQueueSize";
  // No. of raft log entries synced with each flush call
  public static final String RAFT_LOG_SYNC_BATCH_SIZE = "syncBatchSize";
  // Count of RaftLogCache Misses
  public static final String RAFT_LOG_CACHE_MISS_COUNT = "cacheMissCount";
  // Count of RaftLogCache Hits
  public static final String RAFT_LOG_CACHE_HIT_COUNT = "cacheHitCount";
  // Total time taken to append a raft log entry
  public static final String RAFT_LOG_APPEND_ENTRY_LATENCY = "appendEntryLatency";
  // Time spent by a Raft log operation in the queue.
  public static final String RAFT_LOG_TASK_QUEUE_TIME = "enqueuedTime";
  // Time taken for a Raft log operation to get into the queue after being requested. This will be the time it has to
  // wait for the queue to be non-full.
  public static final String RAFT_LOG_TASK_ENQUEUE_DELAY = "queueingDelay";
  // Time taken for a Raft log operation to complete execution.
  public static final String RAFT_LOG_TASK_EXECUTION_TIME = "%sExecutionTime";
  // Number of entries appended to the raft log
  public static final String RAFT_LOG_APPEND_ENTRY_COUNT = "appendEntryCount";
  public static final String RAFT_LOG_PURGE_METRIC = "purgeLog";
  public static final String LOG_APPENDER_INSTALL_SNAPSHOT_METRIC = "numInstallSnapshot";

  //////////////////////////////
  // Raft Log Read Path Metrics
  /////////////////////////////
  // Time required to read a raft log entry from actual raft log file and create a raft log entry
  public static final String RAFT_LOG_READ_ENTRY_LATENCY = "readEntryLatency";
  // Time required to load and process raft log segments during restart
  public static final String RAFT_LOG_LOAD_SEGMENT_LATENCY = "segmentLoadLatency";
  public static final String FOLLOWER_APPEND_ENTRIES_LATENCY = "follower_append_entry_latency";
  public static final String FOLLOWER_NEXT_INDEX = "follower_%s_next_index";
  public static final String FOLLOWER_MATCH_INDEX = "follower_%s_match_index";
  public static final String FOLLOWER_RPC_RESP_TIME = "follower_%s_rpc_response_time";

  public RaftLogMetrics(String serverId) {
    this.registry = getMetricRegistryForLogWorker(serverId);
  }

  private RatisMetricRegistry getMetricRegistryForLogWorker(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_APPLICATION_NAME_METRICS,
        RATIS_LOG_WORKER_METRICS, RATIS_LOG_WORKER_METRICS_DESC));
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
