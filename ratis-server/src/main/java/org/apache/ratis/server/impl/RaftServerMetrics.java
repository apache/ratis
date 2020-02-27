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

package org.apache.ratis.server.impl;

import static org.apache.ratis.server.metrics.RaftLogMetrics.FOLLOWER_APPEND_ENTRIES_LATENCY;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.metrics.RatisMetrics;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.Preconditions;

/**
 * Metric Registry for Raft Group Server. One instance per leader/follower.
 */
public final class RaftServerMetrics extends RatisMetrics {

  public static final String RATIS_SERVER_METRICS = "server";
  public static final String RATIS_SERVER_METRICS_DESC = "Metrics for Raft server";

  public static final String
      FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC = "%s_lastHeartbeatElapsedTime";
  public static final String LEADER_METRIC_PEER_COMMIT_INDEX = "%s_peerCommitIndex";
  public static final String RAFT_CLIENT_READ_REQUEST = "clientReadRequest";
  public static final String RAFT_CLIENT_STALE_READ_REQUEST = "clientStaleReadRequest";
  public static final String RAFT_CLIENT_WRITE_REQUEST = "clientWriteRequest";
  public static final String RAFT_CLIENT_WATCH_REQUEST = "clientWatch%sRequest";
  public static final String RETRY_REQUEST_CACHE_HIT_COUNTER = "numRetryCacheHits";
  public static final String REQUEST_QUEUE_LIMIT_HIT_COUNTER = "numRequestQueueLimitHits";
  public static final String RESOURCE_LIMIT_HIT_COUNTER = "leaderNumResourceLimitHits";
  public static final String REQUEST_BYTE_SIZE_LIMIT_HIT_COUNTER = "numRequestsByteSizeLimitHits";
  public static final String REQUEST_QUEUE_SIZE = "numPendingRequestInQueue";
  public static final String REQUEST_BYTE_SIZE = "numPendingRequestByteSize";


  private Map<String, Long> followerLastHeartbeatElapsedTimeMap = new HashMap<>();
  private CommitInfoCache commitInfoCache;

  private static Map<String, RaftServerMetrics> metricsMap = new HashMap<>();

  public static RaftServerMetrics getRaftServerMetrics(
      RaftServerImpl raftServer) {
    RaftServerMetrics serverMetrics = new RaftServerMetrics(raftServer);
    metricsMap.put(raftServer.getMemberId().toString(), serverMetrics);
    return serverMetrics;
  }

  private RaftServerMetrics(RaftServerImpl server) {
    registry = getMetricRegistryForRaftServer(server.getMemberId().toString());
    commitInfoCache = server.getCommitInfoCache();
    addPeerCommitIndexGauge(server.getId());
  }

  private RatisMetricRegistry getMetricRegistryForRaftServer(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_APPLICATION_NAME_METRICS, RATIS_SERVER_METRICS,
        RATIS_SERVER_METRICS_DESC));
  }

  /**
   * Register a follower with this Leader Metrics registry instance.
   * @param peer {@Link RaftPeer} representing the follower
   */
  public void addFollower(RaftPeer peer) {
    String followerName = peer.getId().toString();
    String followerHbMetricKey = String.format(
        FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC,
        followerName);

    followerLastHeartbeatElapsedTimeMap.put(followerName, 0L);
    registry.gauge(followerHbMetricKey,
        () -> () -> followerLastHeartbeatElapsedTimeMap.get(followerName));

    addPeerCommitIndexGauge(peer.getId());
  }

  /**
   * Register a commit index tracker for the peer in cluster.
   */
  public void addPeerCommitIndexGauge(RaftPeerId peerId) {
    String followerCommitIndexKey = String.format(
        LEADER_METRIC_PEER_COMMIT_INDEX, peerId);
    registry.gauge(followerCommitIndexKey, () -> () -> {
      RaftProtos.CommitInfoProto commitInfoProto = commitInfoCache.get(peerId);
      if (commitInfoProto != null) {
        return commitInfoProto.getCommitIndex();
      }
      return 0L;
    });
  }

  /**
   * Get the commit index gauge for the given peer of the server
   * @param server
   * @param peerServer
   * @return Metric Gauge holding the value of commit index of the peer
   */
  @VisibleForTesting
  public static Gauge getPeerCommitIndexGauge(RaftServerImpl server,
      RaftServerImpl peerServer) {

    RaftServerMetrics serverMetrics =
        metricsMap.get(server.getMemberId().toString());
    if (serverMetrics == null) {
      return null;
    }

    String followerCommitIndexKey = String.format(
        LEADER_METRIC_PEER_COMMIT_INDEX,
        peerServer.getPeer().getId().toString());

    SortedMap<String, Gauge> map =
        serverMetrics.registry.getGauges((s, metric) ->
            s.contains(followerCommitIndexKey));

    Preconditions.assertTrue(map.size() <= 1);
    return map.get(map.firstKey());
  }

  /**
   * Record heartbeat elapsed time for a follower within a Raft group.
   * @param peer {@Link RaftPeer} representing the follower.
   * @param elapsedTime Elapsed time in Nanos.
   */
  public void recordFollowerHeartbeatElapsedTime(RaftPeer peer, long elapsedTime) {
    followerLastHeartbeatElapsedTimeMap.put(peer.getId().toString(),
        elapsedTime);
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

  public Timer getClientRequestTimer(RaftClientRequest request) {
    if (request.is(TypeCase.READ)) {
      return getTimer(RAFT_CLIENT_READ_REQUEST);
    } else if (request.is(TypeCase.STALEREAD)) {
      return getTimer(RAFT_CLIENT_STALE_READ_REQUEST);
    } else if (request.is(TypeCase.WATCH)) {
      String watchType = RaftClientRequest.Type.toString(request.getType().getWatch().getReplication());
      return getTimer(String.format(RAFT_CLIENT_WATCH_REQUEST, watchType));
    } else if (request.is(TypeCase.WRITE)) {
      return getTimer(RAFT_CLIENT_WRITE_REQUEST);
    }
    return null;
  }

  public void onRetryRequestCacheHit() {
    registry.counter(RETRY_REQUEST_CACHE_HIT_COUNTER).inc();
  }

  public void onRequestQueueLimitHit() {
    registry.counter(REQUEST_QUEUE_LIMIT_HIT_COUNTER).inc();
  }

  void addNumPendingRequestsGauge(Gauge queueSize) {
    registry.gauge(REQUEST_QUEUE_SIZE, () -> queueSize);
  }

  void addNumPendingRequestsByteSize(Gauge byteSize) {
    registry.gauge(REQUEST_BYTE_SIZE, () -> byteSize);
  }

  void onRequestByteSizeLimitHit() {
    registry.counter(REQUEST_BYTE_SIZE_LIMIT_HIT_COUNTER).inc();
  }

  void onResourceLimitHit() {
    registry.counter(RESOURCE_LIMIT_HIT_COUNTER).inc();
  }

  public RatisMetricRegistry getRegistry() {
    return registry;
  }
}