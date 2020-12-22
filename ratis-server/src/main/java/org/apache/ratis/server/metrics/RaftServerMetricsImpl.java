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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.function.Supplier;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.protocol.RaftClientRequest.Type;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.Preconditions;

/**
 * Metric Registry for Raft Group Server. One instance per leader/follower.
 */
public final class RaftServerMetricsImpl extends RatisMetrics implements RaftServerMetrics {

  public static final String RATIS_SERVER_METRICS = "server";
  public static final String RATIS_SERVER_METRICS_DESC = "Metrics for Raft server";

  public static final String FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC = "%s_lastHeartbeatElapsedTime";
  public static final String FOLLOWER_APPEND_ENTRIES_LATENCY = "follower_append_entry_latency";
  public static final String LEADER_METRIC_PEER_COMMIT_INDEX = "%s_peerCommitIndex";
  public static final String RAFT_CLIENT_READ_REQUEST = "clientReadRequest";
  public static final String RAFT_CLIENT_STALE_READ_REQUEST = "clientStaleReadRequest";
  public static final String RAFT_CLIENT_WRITE_REQUEST = "clientWriteRequest";
  public static final String RAFT_CLIENT_WATCH_REQUEST = "clientWatch%sRequest";
  public static final String REQUEST_QUEUE_LIMIT_HIT_COUNTER = "numRequestQueueLimitHits";
  public static final String RESOURCE_LIMIT_HIT_COUNTER = "leaderNumResourceLimitHits";
  public static final String REQUEST_BYTE_SIZE_LIMIT_HIT_COUNTER = "numRequestsByteSizeLimitHits";
  public static final String REQUEST_QUEUE_SIZE = "numPendingRequestInQueue";
  public static final String REQUEST_BYTE_SIZE = "numPendingRequestByteSize";
  public static final String RETRY_CACHE_ENTRY_COUNT_METRIC = "retryCacheEntryCount";
  public static final String RETRY_CACHE_HIT_COUNT_METRIC = "retryCacheHitCount";
  public static final String RETRY_CACHE_HIT_RATE_METRIC = "retryCacheHitRate";
  public static final String RETRY_CACHE_MISS_COUNT_METRIC = "retryCacheMissCount";
  public static final String RETRY_CACHE_MISS_RATE_METRIC = "retryCacheMissRate";
  public static final String RATIS_SERVER_FAILED_CLIENT_STALE_READ_COUNT =
      "numFailedClientStaleReadOnServer";
  public static final String RATIS_SERVER_FAILED_CLIENT_READ_COUNT =
      "numFailedClientReadOnServer";
  public static final String RATIS_SERVER_FAILED_CLIENT_WRITE_COUNT =
      "numFailedClientWriteOnServer";
  public static final String RATIS_SERVER_FAILED_CLIENT_WATCH_COUNT =
      "numFailedClientWatchOnServer";
  public static final String RATIS_SERVER_FAILED_CLIENT_STREAM_COUNT =
      "numFailedClientStreamOnServer";
  public static final String RATIS_SERVER_INSTALL_SNAPSHOT_COUNT = "numInstallSnapshot";

  /** Follower Id -> heartbeat elapsed */
  private final Map<RaftPeerId, Long> followerLastHeartbeatElapsedTimeMap = new HashMap<>();
  private final Supplier<Function<RaftPeerId, CommitInfoProto>> commitInfoCache;

  /** id -> metric */
  private static final Map<RaftGroupMemberId, RaftServerMetricsImpl> METRICS = new ConcurrentHashMap<>();
  /** id -> key */
  private static final Map<RaftPeerId, String> PEER_COMMIT_INDEX_GAUGE_KEYS = new ConcurrentHashMap<>();

  private static String getPeerCommitIndexGaugeKey(RaftPeerId serverId) {
    return PEER_COMMIT_INDEX_GAUGE_KEYS.computeIfAbsent(serverId,
        key -> String.format(LEADER_METRIC_PEER_COMMIT_INDEX, key));
  }

  public static RaftServerMetricsImpl computeIfAbsentRaftServerMetrics(RaftGroupMemberId serverId,
      Supplier<Function<RaftPeerId, CommitInfoProto>> commitInfoCache,
      Supplier<RetryCache.Statistics> retryCacheStatistics) {
    return METRICS.computeIfAbsent(serverId,
        key -> new RaftServerMetricsImpl(serverId, commitInfoCache, retryCacheStatistics));
  }

  public static void removeRaftServerMetrics(RaftGroupMemberId serverId) {
    METRICS.remove(serverId);
  }

  public RaftServerMetricsImpl(RaftGroupMemberId serverId,
      Supplier<Function<RaftPeerId, CommitInfoProto>> commitInfoCache,
      Supplier<RetryCache.Statistics> retryCacheStatistics) {
    this.registry = getMetricRegistryForRaftServer(serverId.toString());
    this.commitInfoCache = commitInfoCache;
    addPeerCommitIndexGauge(serverId.getPeerId());
    addRetryCacheMetric(retryCacheStatistics);
  }

  private RatisMetricRegistry getMetricRegistryForRaftServer(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_APPLICATION_NAME_METRICS, RATIS_SERVER_METRICS,
        RATIS_SERVER_METRICS_DESC));
  }

  private void addRetryCacheMetric(Supplier<RetryCache.Statistics> retryCacheStatistics) {
    registry.gauge(RETRY_CACHE_ENTRY_COUNT_METRIC, () -> () -> retryCacheStatistics.get().size());
    registry.gauge(RETRY_CACHE_HIT_COUNT_METRIC  , () -> () -> retryCacheStatistics.get().hitCount());
    registry.gauge(RETRY_CACHE_HIT_RATE_METRIC   , () -> () -> retryCacheStatistics.get().hitRate());
    registry.gauge(RETRY_CACHE_MISS_COUNT_METRIC , () -> () -> retryCacheStatistics.get().missCount());
    registry.gauge(RETRY_CACHE_MISS_RATE_METRIC  , () -> () -> retryCacheStatistics.get().missRate());
  }

  /**
   * Register a follower with this Leader Metrics registry instance.
   */
  public void addFollower(RaftPeerId followerName) {
    String followerHbMetricKey = String.format(
        FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC,
        followerName);

    followerLastHeartbeatElapsedTimeMap.put(followerName, 0L);
    registry.gauge(followerHbMetricKey,
        () -> () -> followerLastHeartbeatElapsedTimeMap.get(followerName));

    addPeerCommitIndexGauge(followerName);
  }

  /**
   * Register a commit index tracker for the peer in cluster.
   */
  public void addPeerCommitIndexGauge(RaftPeerId peerId) {
    registry.gauge(getPeerCommitIndexGaugeKey(peerId), () -> () -> Optional.ofNullable(commitInfoCache.get())
        .map(cache -> cache.apply(peerId))
        .map(CommitInfoProto::getCommitIndex)
        .orElse(0L));
  }

  /**
   * Get the commit index gauge for the given peer of the server
   * @return Metric Gauge holding the value of commit index of the peer
   */
  @VisibleForTesting
  public static Gauge getPeerCommitIndexGauge(RaftGroupMemberId serverId, RaftPeerId peerId) {

    final RaftServerMetricsImpl serverMetrics = METRICS.get(serverId);
    if (serverMetrics == null) {
      return null;
    }

    final String followerCommitIndexKey = getPeerCommitIndexGaugeKey(peerId);

    SortedMap<String, Gauge> map =
        serverMetrics.registry.getGauges((s, metric) ->
            s.contains(followerCommitIndexKey));

    Preconditions.assertTrue(map.size() <= 1);
    return map.get(map.firstKey());
  }

  /**
   * Record heartbeat elapsed time for a follower within a Raft group.
   * @param followerId the follower id.
   * @param elapsedTimeNs Elapsed time in Nanos.
   */
  public void recordFollowerHeartbeatElapsedTime(RaftPeerId followerId, long elapsedTimeNs) {
    followerLastHeartbeatElapsedTimeMap.put(followerId, elapsedTimeNs);
  }

  public Timer getFollowerAppendEntryTimer(boolean isHeartbeat) {
    return registry.timer(FOLLOWER_APPEND_ENTRIES_LATENCY + (isHeartbeat ? "_heartbeat" : ""));
  }

  public Timer getTimer(String timerName) {
    return registry.timer(timerName);
  }

  public Counter getCounter(String counterName) {
    return registry.counter(counterName);
  }

  public Timer getClientRequestTimer(Type request) {
    if (request.is(TypeCase.READ)) {
      return getTimer(RAFT_CLIENT_READ_REQUEST);
    } else if (request.is(TypeCase.STALEREAD)) {
      return getTimer(RAFT_CLIENT_STALE_READ_REQUEST);
    } else if (request.is(TypeCase.WATCH)) {
      String watchType = Type.toString(request.getWatch().getReplication());
      return getTimer(String.format(RAFT_CLIENT_WATCH_REQUEST, watchType));
    } else if (request.is(TypeCase.WRITE)) {
      return getTimer(RAFT_CLIENT_WRITE_REQUEST);
    }
    return null;
  }

  public void onRequestQueueLimitHit() {
    registry.counter(REQUEST_QUEUE_LIMIT_HIT_COUNTER).inc();
  }

  public void addNumPendingRequestsGauge(Gauge queueSize) {
    registry.gauge(REQUEST_QUEUE_SIZE, () -> queueSize);
  }

  public boolean removeNumPendingRequestsGauge() {
    return registry.remove(REQUEST_QUEUE_SIZE);
  }

  public void addNumPendingRequestsByteSize(Gauge byteSize) {
    registry.gauge(REQUEST_BYTE_SIZE, () -> byteSize);
  }

  public boolean removeNumPendingRequestsByteSize() {
    return registry.remove(REQUEST_BYTE_SIZE);
  }

  public void onRequestByteSizeLimitHit() {
    registry.counter(REQUEST_BYTE_SIZE_LIMIT_HIT_COUNTER).inc();
  }

  public void onResourceLimitHit() {
    registry.counter(RESOURCE_LIMIT_HIT_COUNTER).inc();
  }

  void onFailedClientStaleRead() {
    registry.counter(RATIS_SERVER_FAILED_CLIENT_STALE_READ_COUNT).inc();
  }

  void onFailedClientRead() {
    registry.counter(RATIS_SERVER_FAILED_CLIENT_READ_COUNT).inc();
  }

  void onFailedClientWatch() {
    registry.counter(RATIS_SERVER_FAILED_CLIENT_WATCH_COUNT).inc();
  }

  void onFailedClientWrite() {
    registry.counter(RATIS_SERVER_FAILED_CLIENT_WRITE_COUNT).inc();
  }

  void onFailedClientStream() {
    registry.counter(RATIS_SERVER_FAILED_CLIENT_STREAM_COUNT).inc();
  }

  public void incFailedRequestCount(Type type) {
    if (type.is(TypeCase.STALEREAD)) {
      onFailedClientStaleRead();
    } else if (type.is(TypeCase.WATCH)) {
      onFailedClientWatch();
    } else if (type.is(TypeCase.WRITE)) {
      onFailedClientWrite();
    } else if (type.is(TypeCase.READ)) {
      onFailedClientRead();
    } else if (type.is(TypeCase.MESSAGESTREAM)) {
      onFailedClientStream();
    }
  }

  @Override
  public void onSnapshotInstalled() {
    registry.counter(RATIS_SERVER_INSTALL_SNAPSHOT_COUNT).inc();
  }

  public RatisMetricRegistry getRegistry() {
    return registry;
  }
}