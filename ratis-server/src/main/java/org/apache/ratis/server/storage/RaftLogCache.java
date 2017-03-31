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
package org.apache.ratis.server.storage;

import static org.apache.ratis.server.impl.RaftServerConstants.INVALID_LOG_INDEX;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.storage.LogSegment.LogRecord;
import org.apache.ratis.server.storage.LogSegment.SegmentFileInfo;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;

import org.apache.ratis.util.Preconditions;

/**
 * In-memory RaftLog Cache. Currently we provide a simple implementation that
 * caches all the segments in the memory. The cache is not thread-safe and
 * requires external lock protection.
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
      Preconditions.assertTrue(lastClosed == null ||
          areConsecutiveSegments(lastClosed, segment));
    } else {
      Preconditions.assertTrue(openSegment == null &&
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
      final LogRecord record = openSegment.getLogRecord(index);
      return record == null ? null : record.entry;
    } else {
      int segmentIndex = Collections.binarySearch(closedSegments, index);
      if (segmentIndex < 0) {
        return null;
      } else {
        return closedSegments.get(segmentIndex).getLogRecord(index).entry;
      }
    }
  }

  /**
   * @param startIndex inclusive
   * @param endIndex exclusive
   */
  LogEntryProto[] getEntries(final long startIndex, final long endIndex) {
    if (startIndex < 0 || startIndex < getStartIndex()) {
      throw new IndexOutOfBoundsException("startIndex = " + startIndex
          + ", log cache starts from index " + getStartIndex());
    }
    if (startIndex > endIndex) {
      throw new IndexOutOfBoundsException("startIndex(" + startIndex
          + ") > endIndex(" + endIndex + ")");
    }
    final long realEnd = Math.min(getEndIndex() + 1, endIndex);
    if (startIndex >= realEnd) {
      return RaftLog.EMPTY_LOGENTRY_ARRAY;
    }

    LogEntryProto[] entries = new LogEntryProto[(int) (realEnd - startIndex)];
    int segmentIndex = Collections.binarySearch(closedSegments, startIndex);
    if (segmentIndex < 0) {
      getEntriesFromSegment(openSegment, startIndex, entries, 0, entries.length);
    } else {
      long index = startIndex;
      for (int i = segmentIndex; i < closedSegments.size() && index < realEnd; i++) {
        LogSegment s = closedSegments.get(i);
        int numberFromSegment = (int) Math.min(realEnd - index,
            s.getEndIndex() - index + 1);
        getEntriesFromSegment(s, index, entries, (int) (index - startIndex),
            numberFromSegment);
        index += numberFromSegment;
      }
      if (index < realEnd) {
        getEntriesFromSegment(openSegment, index, entries,
            (int) (index - startIndex), (int) (realEnd - index));
      }
    }
    return entries;
  }

  private void getEntriesFromSegment(LogSegment segment, long startIndex,
      LogEntryProto[] entries, int offset, int size) {
    long endIndex = segment.getEndIndex();
    endIndex = Math.min(endIndex, startIndex + size - 1);
    int index = offset;
    for (long i = startIndex; i <= endIndex; i++) {
      entries[index++] = segment.getLogRecord(i).entry;
    }
  }

  long getStartIndex() {
    if (closedSegments.isEmpty()) {
      return openSegment != null ? openSegment.getStartIndex() :
          RaftServerConstants.INVALID_LOG_INDEX;
    } else {
      return closedSegments.get(0).getStartIndex();
    }
  }

  long getEndIndex() {
    return openSegment != null ? openSegment.getEndIndex() :
        (closedSegments.isEmpty() ?
            INVALID_LOG_INDEX :
            closedSegments.get(closedSegments.size() - 1).getEndIndex());
  }

  LogEntryProto getLastEntry() {
    return (openSegment != null && openSegment.numOfEntries() > 0) ?
        openSegment.getLastRecord().entry :
        (closedSegments.isEmpty() ? null :
            closedSegments.get(closedSegments.size() - 1).getLastRecord().entry);
  }

  LogSegment getOpenSegment() {
    return openSegment;
  }

  void appendEntry(LogEntryProto entry) {
    // SegmentedRaftLog does the segment creation/rolling work. Here we just
    // simply append the entry into the open segment.
    Preconditions.assertTrue(openSegment != null);
    openSegment.appendToOpenSegment(entry);
  }

  /**
   * finalize the current open segment, and start a new open segment
   */
  void rollOpenSegment(boolean createNewOpen) {
    Preconditions.assertTrue(openSegment != null
        && openSegment.numOfEntries() > 0);
    final long nextIndex = openSegment.getEndIndex() + 1;
    openSegment.close();
    closedSegments.add(openSegment);
    if (createNewOpen) {
      openSegment = LogSegment.newOpenSegment(nextIndex);
    } else {
      openSegment = null;
    }
  }

  private SegmentFileInfo deleteOpenSegment() {
    final long oldEnd = openSegment.getEndIndex();
    openSegment.clear();
    SegmentFileInfo info = new SegmentFileInfo(openSegment.getStartIndex(),
        oldEnd, true, 0, openSegment.getEndIndex());
    openSegment = null;
    return info;
  }

  /**
   * truncate log entries starting from the given index (inclusive)
   */
  TruncationSegments truncate(long index) {
    int segmentIndex = Collections.binarySearch(closedSegments, index);
    if (segmentIndex == -closedSegments.size() - 1) {
      if (openSegment != null && openSegment.getEndIndex() >= index) {
        final long oldEnd = openSegment.getEndIndex();
        if (index == openSegment.getStartIndex()) {
          // the open segment should be deleted
          return new TruncationSegments(null,
              Collections.singletonList(deleteOpenSegment()));
        } else {
          openSegment.truncate(index);
          Preconditions.assertTrue(!openSegment.isOpen());
          SegmentFileInfo info = new SegmentFileInfo(openSegment.getStartIndex(),
              oldEnd, true, openSegment.getTotalSize(),
              openSegment.getEndIndex());
          closedSegments.add(openSegment);
          openSegment = null;
          return new TruncationSegments(info, Collections.emptyList());
        }
      }
    } else if (segmentIndex >= 0) {
      LogSegment ts = closedSegments.get(segmentIndex);
      final long oldEnd = ts.getEndIndex();
      List<SegmentFileInfo> list = new ArrayList<>();
      ts.truncate(index);
      final int size = closedSegments.size();
      for (int i = size - 1;
           i >= (ts.numOfEntries() == 0 ? segmentIndex : segmentIndex + 1);
           i-- ) {
        LogSegment s = closedSegments.remove(i);
        final long endOfS = i == segmentIndex ? oldEnd : s.getEndIndex();
        s.clear();
        list.add(new SegmentFileInfo(s.getStartIndex(), endOfS, false, 0,
            s.getEndIndex()));
      }
      if (openSegment != null) {
        list.add(deleteOpenSegment());
      }
      SegmentFileInfo t = ts.numOfEntries() == 0 ? null :
          new SegmentFileInfo(ts.getStartIndex(), oldEnd, false,
              ts.getTotalSize(), ts.getEndIndex());
      return new TruncationSegments(t, list);
    }
    return null;
  }

  static class TruncationSegments {
    final SegmentFileInfo toTruncate; // name of the file to be truncated
    final SegmentFileInfo[] toDelete; // names of the files to be deleted

    TruncationSegments(SegmentFileInfo toTruncate,
        List<SegmentFileInfo> toDelete) {
      this.toDelete = toDelete == null ? null :
          toDelete.toArray(new SegmentFileInfo[toDelete.size()]);
      this.toTruncate = toTruncate;
    }
  }

  Iterator<LogEntryProto> iterator(long startIndex) {
    return new EntryIterator(startIndex);
  }

  private class EntryIterator implements Iterator<LogEntryProto> {
    private long nextIndex;
    private LogSegment currentSegment;
    private int segmentIndex;

    EntryIterator(long start) {
      this.nextIndex = start;
      segmentIndex = Collections.binarySearch(closedSegments, nextIndex);
      if (segmentIndex >= 0) {
        currentSegment = closedSegments.get(segmentIndex);
      } else {
        segmentIndex = -segmentIndex - 1;
        if (segmentIndex == closedSegments.size()) {
          currentSegment = openSegment;
        } else {
          // the start index is smaller than the first closed segment's start
          // index. We no longer keep the log entry (because of the snapshot) or
          // the start index is invalid.
          Preconditions.assertTrue(segmentIndex == 0);
          throw new IndexOutOfBoundsException();
        }
      }
    }

    @Override
    public boolean hasNext() {
      return currentSegment != null &&
          currentSegment.getLogRecord(nextIndex) != null;
    }

    @Override
    public LogEntryProto next() {
      LogRecord record;
      if (currentSegment == null ||
          (record = currentSegment.getLogRecord(nextIndex)) == null) {
        throw new NoSuchElementException();
      }
      if (++nextIndex > currentSegment.getEndIndex()) {
        if (currentSegment != openSegment) {
          segmentIndex++;
          currentSegment = segmentIndex == closedSegments.size() ?
              openSegment : closedSegments.get(segmentIndex);
        }
      }
      return record.entry;
    }
  }

  int getNumOfSegments() {
    return closedSegments.size() + (openSegment == null ? 0 : 1);
  }

  boolean isEmpty() {
    return closedSegments.isEmpty() && openSegment == null;
  }

  void clear() {
    openSegment = null;
    closedSegments.clear();
  }
}
