<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->


# Metrics

## Ratis Server

### StateMachine Metrics

| Application | Component     | Name                | Type  | Description                                                  |
|-------------|---------------|---------------------|-------|--------------------------------------------------------------|
| ratis       | state_machine | appliedIndex        | Gauge | Applied index of state machine                               |
| ratis       | state_machine | applyCompletedIndex | Gauge | Last log index which completely applied to the state machine |
| ratis       | state_machine | takeSnapshot        | Timer | Time taken for state machine to take a snapshot              |


### Leader Election Metrics

| Application | Component       | Name                          | Type    | Description                                           |
|-------------|-----------------|-------------------------------|---------|-------------------------------------------------------|
| ratis       | leader_election | electionCount                 | Counter | Number of leader elections of this group              |
| ratis       | leader_election | timeoutCount                  | Counter | Number of election timeouts of this peer              |
| ratis       | leader_election | electionTime                  | Timer   | Time spent on leader election                         |
| ratis       | leader_election | lastLeaderElapsedTime         | Gauge   | Time elapsed since last hearing from an active leader |
| ratis       | leader_election | transferLeadershipCount       | Counter | Number of transferLeader requests                     |
| ratis       | leader_election | lastLeaderElectionElapsedTime | Gauge   | Time elapsed since last leader election               |

### Log Appender Metrics

| Application | Component    | Name                              | Type  | Description                                 |
|-------------|--------------|-----------------------------------|-------|---------------------------------------------|
| ratis       | log_appender | follower_{peer}_next_index        | Gauge | Next index of peer                          |
| ratis       | log_appender | follower_{peer}_match_index       | Gauge | Match index of peer                         |
| ratis       | log_appender | follower_{peer}_rpc_response_time | Gauge | Time elapsed since peer's last rpc response |

### Raft Log Metrics

| Application | Component  | Name                            | Type    | Description                                                                                                   |
|-------------|------------|---------------------------------|---------|---------------------------------------------------------------------------------------------------------------|
| ratis       | log_worker | metadataLogEntryCount           | Counter | Number of metadata(term-index) log entries                                                                    |
| ratis       | log_worker | configLogEntryCount             | Counter | Number of configuration log entries                                                                           |
| ratis       | log_worker | stateMachineLogEntryCount       | Counter | Number of statemachine log entries                                                                            |
| ratis       | log_worker | flushTime                       | Timer   | Time taken to flush log                                                                                       |
| ratis       | log_worker | flushCount                      | Counter | Number of times of log-flush invoked                                                                          |
| ratis       | log_worker | syncTime                        | Timer   | Time taken to log sync (fsync)                                                                                |
| ratis       | log_worker | dataQueueSize                   | Gauge   | Raft log data queue size which at any time gives the number of log related operations in the queue            |
| ratis       | log_worker | workerQueueSize                 | Gauge   | Raft log worker queue size which at any time gives number of committed entries that are to be synced          |
| ratis       | log_worker | syncBatchSize                   | Gauge   | Number of raft log entries synced in each flush call                                                          |
| ratis       | log_worker | cacheMissCount                  | Counter | Count of RaftLogCache Misses                                                                                  |
| ratis       | log_worker | cacheHitCount                   | Counter | Count of RaftLogCache Hits                                                                                    |
| ratis       | log_worker | closedSegmentsNum               | Gauge   | Number of closed raft log segments                                                                            |
| ratis       | log_worker | closedSegmentsSizeInBytes       | Gauge   | Size of closed raft log segments in bytes                                                                     |
| ratis       | log_worker | openSegmentSizeInBytes          | Gauge   | Size of open raft log segment in bytes                                                                        |
| ratis       | log_worker | appendEntryLatency              | Timer   | Total time taken to append a raft log entry                                                                   |
| ratis       | log_worker | enqueuedTime                    | Timer   | Time spent by a Raft log operation in the queue                                                               |
| ratis       | log_worker | queueingDelay                   | Timer   | Time taken for a Raft log operation to get into the queue after being requested, waiting queue to be non-full |
| ratis       | log_worker | {operation}ExecutionTime        | Timer   | Time taken for a Raft log operation(open/close/flush/write/purge) to complete execution                       |
| ratis       | log_worker | appendEntryCount                | Counter | Number of entries appended to the raft log                                                                    |
| ratis       | log_worker | purgeLog                        | Timer   | Time taken for Raft log purge operation to complete execution                                                 |
| ratis       | log_worker | numStateMachineDataWriteTimeout | Counter | Number of statemachine dataApi write timeouts                                                                 |
| ratis       | log_worker | numStateMachineDataReadTimeout  | Counter | Number of statemachine dataApi read timeouts                                                                  |
| ratis       | log_worker | readEntryLatency                | Timer   | Time required to read a raft log entry from actual raft log file and create a raft log entry                  |
| ratis       | log_worker | segmentLoadLatency              | Timer   | Time required to load and process raft log segments during restart                                            |


### Raft Server Metrics

| Application | Component | Name                                 | Type    | Description                                                         |
|-------------|-----------|--------------------------------------|---------|---------------------------------------------------------------------|
| ratis       | server    | {peer}_lastHeartbeatElapsedTime      | Gauge   | Time elapsed since last heartbeat rpc response                      |
| ratis       | server    | follower_append_entry_latency        | Timer   | Time taken for followers to append log entries                      |
| ratis       | server    | {peer}_peerCommitIndex               | Gauge   | Commit index of peer                                                |
| ratis       | server    | clientReadRequest                    | Timer   | Time taken to process read requests from client                     |
| ratis       | server    | clientStaleReadRequest               | Timer   | Time taken to process stale-read requests from client               |
| ratis       | server    | clientWriteRequest                   | Timer   | Time taken to process write requests from client                    |
| ratis       | server    | clientWatch{level}Request            | Timer   | Time taken to process watch(replication_level) requests from client |
| ratis       | server    | numRequestQueueLimitHits             | Counter | Number of (total client requests in queue) limit hits               |
| ratis       | server    | numRequestsByteSizeLimitHits         | Counter | Number of (total size of client requests in queue) limit hits       |
| ratis       | server    | numResourceLimitHits                 | Counter | Sum of numRequestQueueLimitHits and numRequestsByteSizeLimitHits    |
| ratis       | server    | numPendingRequestInQueue             | Gauge   | Number of pending client requests in queue                          |
| ratis       | server    | numPendingRequestMegaByteSize        | Gauge   | Total size of pending client requests in queue                      |
| ratis       | server    | retryCacheEntryCount                 | Gauge   | Number of entries in retry cache                                    |
| ratis       | server    | retryCacheHitCount                   | Gauge   | Number of retry cache hits                                          |
| ratis       | server    | retryCacheHitRate                    | Gauge   | Retry cache hit rate                                                |
| ratis       | server    | retryCacheMissCount                  | Gauge   | Number of retry cache misses                                        |
| ratis       | server    | retryCacheMissRate                   | Gauge   | Retry cache miss rate                                               |
| ratis       | server    | numFailedClientStaleReadOnServer     | Counter | Number of failed stale-read requests                                |
| ratis       | server    | numFailedClientReadOnServer          | Counter | Number of failed read requests                                      |
| ratis       | server    | numFailedClientWriteOnServer         | Counter | Number of failed write requests                                     |
| ratis       | server    | numFailedClientWatchOnServer         | Counter | Number of failed watch requests                                     |
| ratis       | server    | numFailedClientStreamOnServer        | Counter | Number of failed stream requests                                    |
| ratis       | server    | numInstallSnapshot                   | Counter | Number of install-snapshot requests                                 |
| ratis       | server    | numWatch{level}RequestTimeout        | Counter | Number of watch(replication_level) request timeout                  |
| ratis       | server    | numWatch{level}RequestInQueue        | Gauge   | Number of watch(replication_level) requests in queue                |
| ratis       | server    | numWatch{level}RequestQueueLimitHits | Counter | Number of (total watch request in queue) limit hits                 |


## Ratis Netty Metrics

| Application | Component     | Name                          | Type    | Description                               |
|-------------|---------------|-------------------------------|---------|-------------------------------------------|
| ratis_netty | stream_server | {request}_latency             | timer   | Time taken to process data stream request |
| ratis_netty | stream_server | {request}_success_reply_count | Counter | Number of success replies of request      |
| ratis_netty | stream_server | {request}_fail_reply_count    | Counter | Number of fail replies of request         |
| ratis_netty | stream_server | num_requests_{request}        | Counter | Number of total data stream requests      |

## Ratis gRPC Metrics

### Message Metrics

| Application | Component              | Name                       | Type    | Description                                      |
|-------------|------------------------|----------------------------|---------|--------------------------------------------------|
| ratis       | client_message_metrics | {method}_started_total     | Counter | total messages started of {method}               |
| ratis       | client_message_metrics | {method}_completed_total   | Counter | total messages completed of {method}             |
| ratis       | client_message_metrics | {method}_received_executed | Counter | total messages received and executed of {method} |
| ratis       | server_message_metrics | {method}_started_total     | Counter | total messages started of {method}               |
| ratis       | server_message_metrics | {method}_completed_total   | Counter | total messages completed of {method}             |
| ratis       | server_message_metrics | {method}_received_executed | Counter | total messages received and executed of {method} |

### gRPC Log Appender Metrics


| Application | Component    | Name                                  | Type    | Description                                 |
|-------------|--------------|---------------------------------------|---------|---------------------------------------------|
| ratis_grpc  | log_appender | {appendEntries}_latency               | Timer   | Latency of method (appendEntries/heartbeat) |
| ratis_grpc  | log_appender | {follower}_success_reply_count        | Counter | Number of success replies                   |
| ratis_grpc  | log_appender | {follower}_not_leader_reply_count     | Counter | Number of NotLeader replies                 |
| ratis_grpc  | log_appender | {follower}_inconsistency_reply_count  | Counter | Number of Inconsistency replies             |
| ratis_grpc  | log_appender | {follower}_append_entry_timeout_count | Counter | Number of appendEntries timeouts            |
| ratis_grpc  | log_appender | {follower}_pending_log_requests_count | Counter | Number of pending requests                  |
| ratis_grpc  | log_appender | num_retries                           | Counter | Number of request retries                   |
| ratis_grpc  | log_appender | num_requests                          | Counter | Number of requests in total                 |
| ratis_grpc  | log_appender | num_install_snapshot                  | Counter | Number of install snapshot requests         |
