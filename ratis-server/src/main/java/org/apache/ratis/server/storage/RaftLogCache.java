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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.CacheInvalidationPolicy.CacheInvalidationPolicyDefault;
import org.apache.ratis.server.storage.LogSegment.LogRecord;
import org.apache.ratis.server.storage.RaftStorageDirectory.LogPathAndIndex;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import static org.apache.ratis.server.impl.RaftServerConstants.INVALID_LOG_INDEX;

/**
 * In-memory RaftLog Cache. Currently we provide a simple implementation that
 * caches all the segments in the memory. The cache is not thread-safe and
 * requires external lock protection.
 */
class RaftLogCache {
  public static final Logger LOG = LoggerFactory.getLogger(RaftLogCache.class);

  static class SegmentFileInfo {
    final long startIndex; // start index of the segment
    final long endIndex; // original end index
    final boolean isOpen;
    final long targetLength; // position for truncation
    final long newEndIndex; // new end index after the truncation

    SegmentFileInfo(long start, long end, boolean isOpen, long targetLength,
        long newEndIndex) {
      this.startIndex = start;
      this.endIndex = end;
      this.isOpen = isOpen;
      this.targetLength = targetLength;
      this.newEndIndex = newEndIndex;
    }

    @Override
    public String toString() {
      return "(" + startIndex + ", " + endIndex
          + ") isOpen? " + isOpen + ", length=" + targetLength
          + ", newEndIndex=" + newEndIndex;
    }
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

    @Override
    public String toString() {
      return "toTruncate: " + toTruncate
          + "\n  toDelete: " + Arrays.toString(toDelete);
    }
  }

  private final String name;
  private volatile LogSegment openSegment;
  private final List<LogSegment> closedSegments;
  private final RaftStorage storage;

  private final int maxCachedSegments;
  private final CacheInvalidationPolicy evictionPolicy = new CacheInvalidationPolicyDefault();

  RaftLogCache(RaftPeerId selfId, RaftStorage storage, RaftProperties properties) {
    this.name = selfId + "-" + getClass().getSimpleName();
    this.storage = storage;
    maxCachedSegments = RaftServerConfigKeys.Log.maxCachedSegmentNum(properties);
    closedSegments = new ArrayList<>();
  }

  int getMaxCachedSegments() {
    return maxCachedSegments;
  }

  void loadSegment(LogPathAndIndex pi, boolean keepEntryInCache,
      Consumer<LogEntryProto> logConsumer) throws IOException {
    LogSegment logSegment = LogSegment.loadSegment(storage, pi.getPath().toFile(),
        pi.startIndex, pi.endIndex, pi.isOpen(), keepEntryInCache, logConsumer);
    if (logSegment != null) {
      addSegment(logSegment);
    }
  }

  long getCachedSegmentNum() {
    return closedSegments.stream().filter(LogSegment::hasCache).count();
  }

  boolean shouldEvict() {
    return getCachedSegmentNum() > maxCachedSegments;
  }

  void evictCache(long[] followerIndices, long flushedIndex,
      long lastAppliedIndex) {
    List<LogSegment> toEvict = evictionPolicy.evict(followerIndices,
        flushedIndex, lastAppliedIndex, closedSegments, maxCachedSegments);
    for (LogSegment s : toEvict) {
      s.evictCache();
    }
  }

  private LogSegment getLastClosedSegment() {
    return closedSegments.isEmpty() ?
        null : closedSegments.get(closedSegments.size() - 1);
  }

  private void validateAdding(LogSegment segment) {
    final LogSegment lastClosed = getLastClosedSegment();
    if (lastClosed != null) {
      Preconditions.assertTrue(!lastClosed.isOpen());
      Preconditions.assertTrue(lastClosed.getEndIndex() + 1 == segment.getStartIndex());
    }
  }

  void addSegment(LogSegment segment) {
    validateAdding(segment);
    if (segment.isOpen()) {
      setOpenSegment(segment);
    } else {
      closedSegments.add(segment);
    }
  }

  void addOpenSegment(long startIndex) {
    setOpenSegment(LogSegment.newOpenSegment(storage, startIndex));
  }

  private void setOpenSegment(LogSegment openSegment) {
    LOG.trace("{}: setOpenSegment to {}", name, openSegment);
    Preconditions.assertTrue(this.openSegment == null);
    this.openSegment = Objects.requireNonNull(openSegment);
  }

  private void clearOpenSegment() {
    LOG.trace("{}: clearOpenSegment {}", name, openSegment);
    Objects.requireNonNull(openSegment);
    this.openSegment = null;
  }

  LogSegment getOpenSegment() {
    return openSegment;
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
    clearOpenSegment();
    if (createNewOpen) {
      addOpenSegment(nextIndex);
    }
  }

  LogSegment getSegment(long index) {
    if (openSegment != null && index >= openSegment.getStartIndex()) {
      return openSegment;
    } else {
      int segmentIndex = Collections.binarySearch(closedSegments, index);
      return segmentIndex < 0 ? null : closedSegments.get(segmentIndex);
    }
  }

  LogRecord getLogRecord(long index) {
    LogSegment segment = getSegment(index);
    return segment == null ? null : segment.getLogRecord(index);
  }

  /**
   * @param startIndex inclusive
   * @param endIndex exclusive
   */
  TermIndex[] getTermIndices(final long startIndex, final long endIndex) {
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
      return TermIndex.EMPTY_TERMINDEX_ARRAY;
    }

    TermIndex[] entries = new TermIndex[Math.toIntExact(realEnd - startIndex)];
    int segmentIndex = Collections.binarySearch(closedSegments, startIndex);
    if (segmentIndex < 0) {
      getFromSegment(openSegment, startIndex, entries, 0, entries.length);
    } else {
      long index = startIndex;
      for (int i = segmentIndex; i < closedSegments.size() && index < realEnd; i++) {
        LogSegment s = closedSegments.get(i);
        int numberFromSegment = Math.toIntExact(
            Math.min(realEnd - index, s.getEndIndex() - index + 1));
        getFromSegment(s, index, entries,
            Math.toIntExact(index - startIndex), numberFromSegment);
        index += numberFromSegment;
      }
      if (index < realEnd) {
        getFromSegment(openSegment, index, entries,
            Math.toIntExact(index - startIndex),
            Math.toIntExact(realEnd - index));
      }
    }
    return entries;
  }

  private void getFromSegment(LogSegment segment, long startIndex,
      TermIndex[] entries, int offset, int size) {
    long endIndex = segment.getEndIndex();
    endIndex = Math.min(endIndex, startIndex + size - 1);
    int index = offset;
    for (long i = startIndex; i <= endIndex; i++) {
      LogRecord r = segment.getLogRecord(i);
      entries[index++] = r == null ? null : r.getTermIndex();
    }
  }

  boolean isConfigEntry(TermIndex ti) {
    LogSegment segment = getSegment(ti.getIndex());
    return segment != null && segment.isConfigEntry(ti);
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

  TermIndex getLastTermIndex() {
    return (openSegment != null && openSegment.numOfEntries() > 0) ?
        openSegment.getLastTermIndex() :
        (closedSegments.isEmpty() ? null :
            closedSegments.get(closedSegments.size() - 1).getLastTermIndex());
  }

  void appendEntry(LogEntryProto entry) {
    // SegmentedRaftLog does the segment creation/rolling work. Here we just
    // simply append the entry into the open segment.
    Preconditions.assertTrue(openSegment != null);
    openSegment.appendToOpenSegment(entry);
  }

  private SegmentFileInfo deleteOpenSegment() {
    final long oldEnd = openSegment.getEndIndex();
    openSegment.clear();
    SegmentFileInfo info = new SegmentFileInfo(openSegment.getStartIndex(),
        oldEnd, true, 0, openSegment.getEndIndex());
    clearOpenSegment();
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
          clearOpenSegment();
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

  Iterator<TermIndex> iterator(long startIndex) {
    return new EntryIterator(startIndex);
  }

  private class EntryIterator implements Iterator<TermIndex> {
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
    public TermIndex next() {
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
      return record.getTermIndex();
    }
  }

  int getNumOfSegments() {
    return closedSegments.size() + (openSegment == null ? 0 : 1);
  }

  boolean isEmpty() {
    return closedSegments.isEmpty() && openSegment == null;
  }

  void clear() {
    if (openSegment != null) {
      openSegment.clear();
      clearOpenSegment();
    }
    closedSegments.forEach(LogSegment::clear);
    closedSegments.clear();
  }
}
