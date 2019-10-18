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

import static org.apache.ratis.server.metrics.RatisMetricNames.LEADER_METRIC_PEER_COMMIT_INDEX;
import static org.apache.ratis.server.metrics.RatisMetricNames.LEADER_METRIC_FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import com.codahale.metrics.Gauge;

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.metrics.RatisMetrics;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.Preconditions;

/**
 * Metric Registry for Raft Group Server. One instance per leader/follower.
 */
public final class RaftServerMetrics {

  private RatisMetricRegistry registry = null;
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
    registry = RatisMetrics.getMetricRegistryForRaftServer(
        server.getMemberId().toString());
    commitInfoCache = server.getCommitInfoCache();
    addPeerCommitIndexGauge(server.getPeer());
  }

  /**
   * Register a follower with this Leader Metrics registry instance.
   * @param peer {@Link RaftPeer} representing the follower
   */
  public void addFollower(RaftPeer peer) {
    String followerName = peer.getId().toString();
    String followerHbMetricKey = String.format(
        LEADER_METRIC_FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC,
        followerName);

    followerLastHeartbeatElapsedTimeMap.put(followerName, 0L);
    registry.gauge(followerHbMetricKey,
        () -> () -> followerLastHeartbeatElapsedTimeMap.get(followerName));

    addPeerCommitIndexGauge(peer);
  }

  /**
   * Register a commit index tracker for the peer in cluster.
   * @param peer
   */
  public void addPeerCommitIndexGauge(RaftPeer peer) {
    String followerCommitIndexKey = String.format(
        LEADER_METRIC_PEER_COMMIT_INDEX, peer.getId().toString());
    registry.gauge(followerCommitIndexKey, () -> () -> {
      RaftProtos.CommitInfoProto commitInfoProto = commitInfoCache.get(peer.getId());
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
}