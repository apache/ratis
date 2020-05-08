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
package org.apache.ratis.server.raftlog.segmented;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache.LogSegmentList;
import org.apache.ratis.util.AutoCloseableLock;

public interface CacheInvalidationPolicy {
  /**
   * Determine which log segments should evict their log entry cache
   * @param followerNextIndices the next indices of all the follower peers. Null
   *                            if the local peer is not a leader.
   * @param safeEvictIndex the index up to which cache can be evicted. This
   *                       index depends on two factors:
   *                       1. the largest index belonging to a closed segment
   *                       2. the latest snapshotIndex
   *                       Logs with endIndex less than the max of these two
   *                       indices can be evicted.
   * @param lastAppliedIndex the last index that has been applied to state machine
   * @param segments The list of log segments. The segments should be sorted in
   *                 ascending order according to log index.
   * @param maxCachedSegments the max number of segments with cached log entries
   * @return the log segments that should evict cache
   */
  List<LogSegment> evict(long[] followerNextIndices, long safeEvictIndex,
      long lastAppliedIndex, LogSegmentList segments, int maxCachedSegments);

  class CacheInvalidationPolicyDefault implements CacheInvalidationPolicy {
    @Override
    public List<LogSegment> evict(long[] followerNextIndices,
        long safeEvictIndex, long lastAppliedIndex,
        LogSegmentList segments, final int maxCachedSegments) {
      try(AutoCloseableLock readLock = segments.readLock()) {
        return evictImpl(followerNextIndices, safeEvictIndex, lastAppliedIndex, segments, maxCachedSegments);
      }
    }

    private List<LogSegment> evictImpl(long[] followerNextIndices,
        long safeEvictIndex, long lastAppliedIndex,
        LogSegmentList segments, final int maxCachedSegments) {
      List<LogSegment> result = new ArrayList<>();
      int safeIndex = segments.size() - 1;
      for (; safeIndex >= 0; safeIndex--) {
        LogSegment segment = segments.get(safeIndex);
        // a segment's cache can be invalidated only if it's close and all its
        // entries have been flushed to the local disk and the local disk
        // segment is also closed.
        if (!segment.isOpen() && segment.getEndIndex() <= safeEvictIndex) {
          break;
        }
      }
      if (followerNextIndices == null || followerNextIndices.length == 0) {
        // no followers, determine the eviction based on lastAppliedIndex
        // first scan from the oldest segment to the one that is right before
        // lastAppliedIndex. All these segment's cache can be invalidated.
        int j = 0;
        for (; j <= safeIndex; j++) {
          LogSegment segment = segments.get(j);
          if (segment.getEndIndex() > lastAppliedIndex) {
            break;
          }
          if (segment.hasCache()) {
            result.add(segment);
          }
        }
        // if there is no cache invalidation target found, pick a segment that
        // later (but not now) the state machine will consume
        if (result.isEmpty()) {
          for (int i = safeIndex; i >= j; i--) {
            LogSegment s = segments.get(i);
            if (s.getStartIndex() > lastAppliedIndex && s.hasCache()) {
              result.add(s);
              break;
            }
          }
        }
      } else {
        // this peer is the leader with followers. determine the eviction based
        // on followers' next indices and the local lastAppliedIndex.
        Arrays.sort(followerNextIndices);
        // segments covering index minToRead will still be loaded. Thus we first
        // try to evict cache for segments before minToRead.
        final long minToRead = Math.min(followerNextIndices[0], lastAppliedIndex);
        int j = 0;
        for (; j <= safeIndex; j++) {
          LogSegment s = segments.get(j);
          if (s.getEndIndex() >= minToRead) {
            break;
          }
          if (s.hasCache()) {
            result.add(s);
          }
        }
        // if there is no eviction target, continue the scanning and evict
        // the one that is not being read currently.
        if (result.isEmpty()) {
          for (; j <= safeIndex; j++) {
            LogSegment s = segments.get(j);
            if (Arrays.stream(followerNextIndices).noneMatch(s::containsIndex)
                && !s.containsIndex(lastAppliedIndex) && s.hasCache()) {
              result.add(s);
              break;
            }
          }
        }
      }
      return result;
    }
  }
}
