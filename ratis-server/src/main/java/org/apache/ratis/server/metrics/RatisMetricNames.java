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

public final class RatisMetricNames {

  private RatisMetricNames() {
  }

  public static final String LEADER_ELECTION_COUNT_METRIC = "leaderElectionCount";
  public static final String LEADER_ELECTION_TIMEOUT_COUNT_METRIC = "leaderElectionTimeoutCount";
  public static final String LEADER_ELECTION_LATENCY = "leaderElectionLatency";
  public static final String LAST_LEADER_ELAPSED_TIME = "lastLeaderElapsedTime";

  public static final String
      LEADER_METRIC_FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC =
      "follower_%s_lastHeartbeatElapsedTime";
  public static final String LEADER_METRIC_PEER_COMMIT_INDEX =
      "%s_peerCommitIndex";

  public static final String STATEMACHINE_APPLIED_INDEX_GAUGE =
      "statemachine_applied_index";
  public static final String STATEMACHINE_APPLY_COMPLETED_GAUGE =
      "statemachine_apply_completed_index";

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
  public static final String RAFT_LOG_TASK_EXECUTION_TIME = "ExecutionTime";

  // Number of entries appended to the raft log
  public static final String RAFT_LOG_APPEND_ENTRY_COUNT = "appendEntryCount";

  //////////////////////////////
  // Raft Log Read Path Metrics
  /////////////////////////////

  // Time required to read a raft log entry from actual raft log file and create a raft log entry
  public static final String RAFT_LOG_READ_ENTRY_LATENCY = "readEntryLatency";

  // Time required to load and process raft log segments during restart
  public static final String RAFT_LOG_LOAD_SEGMENT_LATENCY = "segmentLoadLatency";

  public static final String FOLLOWER_APPEND_ENTRIES_LATENCY = "follower_append_entry_latency";
}
