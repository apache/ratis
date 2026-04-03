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
package org.apache.ratis.server.leader;

import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.Timestamp;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Leader-side utility for selecting snapshot source followers.
 *
 * <p>The returned list is ranked by suitability for serving snapshots to a lagging target:
 * fully caught-up followers first, followed by followers with higher replication progress
 * and fresher append-response signals.</p>
 */
public final class SnapshotSourceSelector {
  private SnapshotSourceSelector() {
  }

  /**
   * Select and rank candidate source followers for snapshot download.
   * If this method returns an empty list, callers should fall back to leader-sourced snapshot install.
   *
   * <p>Selection rule:
   * only followers with {@code matchIndex >= firstAvailableLogIndex - 1} are returned.
   * </p>
   *
   * <p>Ranking rule (highest preference first):
   * <ol>
   *   <li>Fully caught up with the leader (as inferred from {@code leaderLastEntry})</li>
   *   <li>Higher {@link FollowerInfo#getMatchIndex()}</li>
   *   <li>Higher {@link FollowerInfo#getCommitIndex()}</li>
   *   <li>Fresher append-response timestamp</li>
   * </ol>
   * </p>
   *
   * @param followers all followers known to the leader.
   * @param targetFollowerId the follower that needs the snapshot; it will be excluded from candidates.
   * @param firstAvailableLogIndex leader's first available log index for catch-up.
   * @param leaderLastEntry leader last entry used to infer whether a follower is fully caught up.
   * @return ranked snapshot source followers; empty means "fall back to leader".
   */
  public static List<FollowerInfo> selectSourceFollowers(
      Collection<FollowerInfo> followers, RaftPeerId targetFollowerId,
      long firstAvailableLogIndex, TermIndex leaderLastEntry) {
    Objects.requireNonNull(followers, "followers == null");

    // A source follower must be able to bridge the target to the leader's first available log.
    // Without this lower bound, we may pick a follower that still cannot provide a usable snapshot.
    final long minimumMatchIndex = firstAvailableLogIndex - 1;
    final List<FollowerInfo> ranked = followers.stream()
        .filter(Objects::nonNull)
        .filter(f -> targetFollowerId == null || !targetFollowerId.equals(f.getId()))
        .filter(f -> f.getMatchIndex() >= minimumMatchIndex)
        .sorted(newFollowerComparator(leaderLastEntry))
        .collect(Collectors.toList());
    return ranked.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(ranked);
  }

  /**
   * Convert ranked candidate followers to ranked peer protos for notification payloads.
   *
   * @see #selectSourceFollowers(Collection, RaftPeerId, long, TermIndex)
   */
  public static List<RaftPeerProto> selectSourcePeers(
      Collection<FollowerInfo> followers, RaftPeerId targetFollowerId,
      long firstAvailableLogIndex, TermIndex leaderLastEntry) {
    final List<RaftPeerProto> peers = selectSourceFollowers(
            followers, targetFollowerId, firstAvailableLogIndex, leaderLastEntry).stream()
        .map(FollowerInfo::getPeer)
        .filter(Objects::nonNull)
        .map(peer -> peer.getRaftPeerProto())
        .collect(Collectors.toList());
    return peers.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(peers);
  }

  /**
   * Compare followers based on the following criteria one by one:
   * - Fully caught up with the leader (as inferred from {@code leaderLastEntry})
   * - Higher {@link FollowerInfo#getMatchIndex()}
   * - Higher {@link FollowerInfo#getCommitIndex()}
   * - Fresher append-response timestamp
   */
  private static Comparator<FollowerInfo> newFollowerComparator(TermIndex leaderLastEntry) {
    return Comparator.<FollowerInfo, Boolean>comparing(f -> isFullyCaughtUp(f, leaderLastEntry))
        .reversed()
        .thenComparing(Comparator.comparingLong(FollowerInfo::getMatchIndex).reversed())
        .thenComparing(Comparator.comparingLong(FollowerInfo::getCommitIndex).reversed())
        .thenComparing(SnapshotSourceSelector::freshnessTimestamp, SnapshotSourceSelector::compareFreshness)
        // Keep order deterministic when all ranking metrics are equal, we do not want it to change across runs.
        .thenComparing(f -> f.getId().toString());
  }

  private static boolean isFullyCaughtUp(FollowerInfo follower, TermIndex leaderLastEntry) {
    return leaderLastEntry != null && follower.getMatchIndex() >= leaderLastEntry.getIndex();
  }

  private static Timestamp freshnessTimestamp(FollowerInfo follower) {
    final Timestamp appendResponse = follower.getLastRespondedAppendEntriesSendTime();
    return appendResponse != null ? appendResponse : follower.getLastRpcResponseTime();
  }

  private static int compareFreshness(Timestamp left, Timestamp right) {
    if (left == right) {
      return 0;
    } else if (left == null) {
      return 1;
    } else if (right == null) {
      return -1;
    }
    // More recent timestamp first.
    return right.compareTo(left);
  }
}
