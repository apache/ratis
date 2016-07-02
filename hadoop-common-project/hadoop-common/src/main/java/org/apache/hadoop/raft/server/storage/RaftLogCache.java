/**
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
package org.apache.hadoop.raft.server.storage;

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.binarySearch;

/**
 * In-memory RaftLog Cache. Currently we provide a simple implementation that
 * caches all the segments in the memory.
 */
class RaftLogCache {
  private LogSegment openSegment;
  private final List<LogSegment> closedSegments;

  RaftLogCache() {
    closedSegments = new ArrayList<>();
  }

  private boolean areConsecutiveSegments(LogSegment prev, LogSegment segment) {
    return !prev.isOpen() && prev.getEndIndex() + 1 == segment.getStartIndex();
  }

  private LogSegment getLastClosedSegment() {
    return closedSegments.isEmpty() ?
        null : closedSegments.get(closedSegments.size() - 1);
  }

  private void validateAdding(LogSegment segment) {
    final LogSegment lastClosed = getLastClosedSegment();
    if (!segment.isOpen()) {
      Preconditions.checkState(lastClosed == null ||
          areConsecutiveSegments(lastClosed, segment));
    } else {
      Preconditions.checkState(openSegment == null &&
          (lastClosed == null || areConsecutiveSegments(lastClosed, segment)));
    }
  }

  void addSegment(LogSegment segment) {
    validateAdding(segment);
    if (segment.isOpen()) {
      openSegment = segment;
    } else {
      closedSegments.add(segment);
    }
  }

  LogEntryProto getEntry(long index) {
    if (openSegment != null && index >= openSegment.getStartIndex()) {
      return openSegment.getEntry(index);
    } else {
      int segmentIndex = Collections.binarySearch(closedSegments, index);
      if (segmentIndex < 0) {
        return null;
      } else {
        return closedSegments.get(segmentIndex).getEntry(index);
      }
    }
  }
}
