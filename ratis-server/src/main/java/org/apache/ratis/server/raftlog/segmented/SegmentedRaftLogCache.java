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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.raftlog.segmented.CacheInvalidationPolicy.CacheInvalidationPolicyDefault;
import org.apache.ratis.server.raftlog.segmented.LogSegment.LogRecord;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.AutoCloseableReadWriteLock;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * In-memory RaftLog Cache. Currently we provide a simple implementation that
 * caches all the segments in the memory. The cache is not thread-safe and
 * requires external lock protection.
 */
public class SegmentedRaftLogCache {
  public static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogCache.class);

  static final class SegmentFileInfo {
    static SegmentFileInfo newClosedSegmentFileInfo(LogSegment ls) {
      Objects.requireNonNull(ls, "ls == null");
      Preconditions.assertTrue(!ls.isOpen(), () -> ls + " is OPEN");
      return new SegmentFileInfo(ls.getStartIndex(), ls.getEndIndex(), ls.isOpen(), 0, 0);
    }

    private final long startIndex; // start index of the segment
    private final long endIndex; // original end index
    private final boolean isOpen;
    private final long targetLength; // position for truncation
    private final long newEndIndex; // new end index after the truncation

    public long getStartIndex() {
      return startIndex;
    }

    public long getEndIndex() {
      return endIndex;
    }

    public boolean isOpen() {
      return isOpen;
    }

    public long getTargetLength() {
      return targetLength;
    }

    public long getNewEndIndex() {
      return newEndIndex;
    }

    private SegmentFileInfo(long start, long end, boolean isOpen, long targetLength, long newEndIndex) {
      this.startIndex = start;
      this.endIndex = end;
      this.isOpen = isOpen;
      this.targetLength = targetLength;
      this.newEndIndex = newEndIndex;
    }

    File getFile(RaftStorage storage) {
      return LogSegmentStartEnd.valueOf(startIndex, endIndex, isOpen).getFile(storage);
    }

    File getNewFile(RaftStorage storage) {
      return LogSegmentStartEnd.valueOf(startIndex, newEndIndex, false).getFile(storage);
    }

    @Override
    public String toString() {
      return "(" + startIndex + ", " + endIndex
          + ") isOpen? " + isOpen + ", length=" + targetLength
          + ", newEndIndex=" + newEndIndex;
    }
  }

  static class TruncationSegments {
    private final SegmentFileInfo toTruncate; // name of the file to be truncated
    private final SegmentFileInfo[] toDelete; // names of the files to be deleted

    public SegmentFileInfo getToTruncate() {
      return toTruncate;
    }

    public SegmentFileInfo[] getToDelete() {
      return toDelete;
    }

    TruncationSegments(SegmentFileInfo toTruncate,
                       List<SegmentFileInfo> toDelete) {
      this.toDelete = toDelete == null ? null :
          toDelete.toArray(new SegmentFileInfo[toDelete.size()]);
      this.toTruncate = toTruncate;
    }

    long maxEndIndex() {
      long max = Long.MIN_VALUE;
      if (toTruncate != null) {
        max = toTruncate.endIndex;
      }
      for(SegmentFileInfo d : toDelete) {
        max = Math.max(max, d.endIndex);
      }
      return max;
    }

    @Override
    public String toString() {
      return "toTruncate: " + toTruncate
          + "\n  toDelete: " + Arrays.toString(toDelete);
    }
  }

  static class LogSegmentList {
    private final Object name;
    private final List<LogSegment> segments = new CopyOnWriteArrayList<>();
    private final AutoCloseableReadWriteLock lock;
    private long sizeInBytes;

    LogSegmentList(Object name) {
      this.name = name;
      this.lock = new AutoCloseableReadWriteLock(name);
      this.sizeInBytes = 0;
    }

    AutoCloseableLock readLock() {
      final StackTraceElement caller = LOG.isTraceEnabled()? JavaUtils.getCallerStackTraceElement(): null;
      return lock.readLock(caller, LOG::trace);
    }

    AutoCloseableLock writeLock() {
      final StackTraceElement caller = LOG.isTraceEnabled()? JavaUtils.getCallerStackTraceElement(): null;
      return lock.writeLock(caller, LOG::trace);
    }

    boolean isEmpty() {
      return segments.isEmpty();
    }

    int size() {
      return segments.size();
    }

    long getTotalFileSize() {
      return sizeInBytes;
    }

    long getTotalCacheSize() {
      return segments.stream().mapToLong(LogSegment::getTotalCacheSize).sum();
    }

    long countCached() {
      return segments.stream().filter(LogSegment::hasCache).count();
    }

    LogSegment getLast() {
      try(AutoCloseableLock readLock = readLock()) {
        return segments.isEmpty()? null: segments.get(segments.size() - 1);
      }
    }

    LogSegment get(int i) {
      return segments.get(i);
    }

    int binarySearch(long index) {
      try(AutoCloseableLock readLock = readLock()) {
        return Collections.binarySearch(segments, index);
      }
    }

    LogSegment search(long index) {
      try(AutoCloseableLock readLock = readLock()) {
        final int i = Collections.binarySearch(segments, index);
        return i < 0? null: segments.get(i);
      }
    }

    LogEntryHeader[] getTermIndex(long startIndex, long realEnd, LogSegment openSegment) {
      final LogEntryHeader[] entries = new LogEntryHeader[Math.toIntExact(realEnd - startIndex)];
      final int searchIndex;
      long index = startIndex;

      try(AutoCloseableLock readLock = readLock()) {
        searchIndex = Collections.binarySearch(segments, startIndex);
        if (searchIndex >= 0) {
          for(int i = searchIndex; i < segments.size() && index < realEnd; i++) {
            final LogSegment s = segments.get(i);
            final int numberFromSegment = Math.toIntExact(Math.min(realEnd - index, s.getEndIndex() - index + 1));
            getFromSegment(s, index, entries, Math.toIntExact(index - startIndex), numberFromSegment);
            index += numberFromSegment;
          }
        }
      }

      // openSegment is read outside the lock.
      if (searchIndex < 0) {
        getFromSegment(openSegment, startIndex, entries, 0, entries.length);
      } else if (index < realEnd) {
        getFromSegment(openSegment, index, entries,
            Math.toIntExact(index - startIndex), Math.toIntExact(realEnd - index));
      }
      return entries;
    }

    boolean add(LogSegment logSegment) {
      try(AutoCloseableLock writeLock = writeLock()) {
        sizeInBytes += logSegment.getTotalFileSize();
        return segments.add(logSegment);
      }
    }

    void clear() {
      try(AutoCloseableLock writeLock = writeLock()) {
        segments.forEach(LogSegment::clear);
        segments.clear();
        sizeInBytes = 0;
      }
    }

    TruncationSegments truncate(long index, LogSegment openSegment, Runnable clearOpenSegment) {
      try(AutoCloseableLock writeLock = writeLock()) {
        final int segmentIndex = binarySearch(index);
        if (segmentIndex == -segments.size() - 1) {
          if (openSegment != null && openSegment.getEndIndex() >= index) {
            final long oldEnd = openSegment.getEndIndex();
            if (index == openSegment.getStartIndex()) {
              // the open segment should be deleted
              final SegmentFileInfo deleted = deleteOpenSegment(openSegment, clearOpenSegment);
              return new TruncationSegments(null, Collections.singletonList(deleted));
            } else {
              openSegment.truncate(index);
              Preconditions.assertTrue(!openSegment.isOpen(),
                  () -> "Illegal state: " + openSegment + " remains open after truncate.");
              final SegmentFileInfo info = new SegmentFileInfo(openSegment.getStartIndex(),
                  oldEnd, true, openSegment.getTotalFileSize(), openSegment.getEndIndex());
              segments.add(openSegment);
              sizeInBytes += openSegment.getTotalFileSize();
              clearOpenSegment.run();
              return new TruncationSegments(info, Collections.emptyList());
            }
          }
        } else if (segmentIndex >= 0) {
          final LogSegment ts = segments.get(segmentIndex);
          final long oldEnd = ts.getEndIndex();
          final List<SegmentFileInfo> list = new ArrayList<>();
          sizeInBytes -= ts.getTotalFileSize();
          ts.truncate(index);
          sizeInBytes += ts.getTotalFileSize();
          final int size = segments.size();
          for(int i = size - 1;
              i >= (ts.numOfEntries() == 0? segmentIndex: segmentIndex + 1);
              i--) {
            LogSegment s = segments.remove(i);
            sizeInBytes -= s.getTotalFileSize();
            final long endOfS = i == segmentIndex? oldEnd: s.getEndIndex();
            s.clear();
            list.add(new SegmentFileInfo(s.getStartIndex(), endOfS, false, 0, s.getEndIndex()));
          }
          if (openSegment != null) {
            list.add(deleteOpenSegment(openSegment, clearOpenSegment));
          }
          SegmentFileInfo t = ts.numOfEntries() == 0? null:
              new SegmentFileInfo(ts.getStartIndex(), oldEnd, false, ts.getTotalFileSize(), ts.getEndIndex());
          return new TruncationSegments(t, list);
        }
        return null;
      }
    }

    TruncationSegments purge(long index) {
      try (AutoCloseableLock writeLock = writeLock()) {
        int segmentIndex = binarySearch(index);
        List<SegmentFileInfo> list = new ArrayList<>();

        if (segmentIndex == -segments.size() - 1) {
          for (LogSegment ls : segments) {
            list.add(SegmentFileInfo.newClosedSegmentFileInfo(ls));
          }
          segments.clear();
          sizeInBytes = 0;
        } else if (segmentIndex >= 0) {
          // we start to purge the closedSegments which do not overlap with index.
          LogSegment overlappedSegment = segments.get(segmentIndex);
          // if a segment's end index matches the passed in index, it is OK
          // to purge that.
          int startIndex = (overlappedSegment.getEndIndex() == index) ?
              segmentIndex : segmentIndex - 1;
          for (int i = 0; i <= startIndex; i++) {
            LogSegment segment = segments.remove(0); // must remove the first segment to avoid gaps.
            sizeInBytes -= segment.getTotalFileSize();
            list.add(SegmentFileInfo.newClosedSegmentFileInfo(segment));
          }
        } else {
          throw new IllegalStateException("Unexpected gap in segments: binarySearch(" + index + ") returns "
                  + segmentIndex + ", segments=" + segments);
        }
        return list.isEmpty() ? null : new TruncationSegments(null, list);
      }
    }

    static SegmentFileInfo deleteOpenSegment(LogSegment openSegment, Runnable clearOpenSegment) {
      final long oldEnd = openSegment.getEndIndex();
      openSegment.clear();
      final SegmentFileInfo info = new SegmentFileInfo(openSegment.getStartIndex(), oldEnd, true,
          0, openSegment.getEndIndex());
      clearOpenSegment.run();
      return info;
    }

    @Override
    public String toString() {
      return name + ":" + segments;
    }
  }

  private final String name;
  private volatile LogSegment openSegment;
  private final LogSegmentList closedSegments;
  private final RaftStorage storage;
  private final SegmentedRaftLogMetrics raftLogMetrics;

  private final int maxCachedSegments;
  private final CacheInvalidationPolicy evictionPolicy = new CacheInvalidationPolicyDefault();
  private final long maxSegmentCacheSize;

  SegmentedRaftLogCache(Object name, RaftStorage storage, RaftProperties properties,
      SegmentedRaftLogMetrics raftLogMetrics) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
    this.closedSegments = new LogSegmentList(name);
    this.storage = storage;
    this.raftLogMetrics = raftLogMetrics;
    this.raftLogMetrics.addClosedSegmentsNum(this);
    this.raftLogMetrics.addClosedSegmentsSizeInBytes(this);
    this.raftLogMetrics.addOpenSegmentSizeInBytes(this);
    this.maxCachedSegments = RaftServerConfigKeys.Log.segmentCacheNumMax(properties);
    this.maxSegmentCacheSize = RaftServerConfigKeys.Log.segmentCacheSizeMax(properties).getSize();
  }

  int getMaxCachedSegments() {
    return maxCachedSegments;
  }

  void loadSegment(LogSegmentPath pi, boolean keepEntryInCache,
      Consumer<LogEntryProto> logConsumer) throws IOException {
    final LogSegment logSegment = LogSegment.loadSegment(storage, pi.getPath().toFile(), pi.getStartEnd(),
        keepEntryInCache, logConsumer, raftLogMetrics);
    if (logSegment != null) {
      addSegment(logSegment);
    }
  }

  public long getCachedSegmentNum() {
    return closedSegments.countCached();
  }

  public long getClosedSegmentsSizeInBytes() {
    return closedSegments.getTotalFileSize();
  }

  public long getOpenSegmentSizeInBytes() {
    return openSegment == null ? 0 : openSegment.getTotalFileSize();
  }

  private long getTotalCacheSize() {
    return closedSegments.getTotalCacheSize() +
            Optional.ofNullable(openSegment).map(LogSegment::getTotalCacheSize).orElse(0L);
  }

  boolean shouldEvict() {
    return closedSegments.countCached() > maxCachedSegments || getTotalCacheSize() > maxSegmentCacheSize;
  }

  void evictCache(long[] followerIndices, long safeEvictIndex, long lastAppliedIndex) {
    List<LogSegment> toEvict = evictionPolicy.evict(followerIndices,
        safeEvictIndex, lastAppliedIndex, closedSegments, maxCachedSegments);
    for (LogSegment s : toEvict) {
      s.evictCache();
    }
  }


  private void validateAdding(LogSegment segment) {
    final LogSegment lastClosed = closedSegments.getLast();
    if (lastClosed != null) {
      Preconditions.assertTrue(!lastClosed.isOpen(),
          () -> "Unexpected log segment state: the log segment " + lastClosed
              + " is open but it is not the last segment. The next log segment is " + segment);
      Preconditions.assertTrue(lastClosed.getEndIndex() + 1 == segment.getStartIndex(),
          () -> "Found a gap between logs: the last log segment " + lastClosed + " ended at " + lastClosed.getEndIndex()
              + " but the next log segment " + segment + " started at " + segment.getStartIndex());
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
    setOpenSegment(LogSegment.newOpenSegment(storage, startIndex,raftLogMetrics));
  }

  private void setOpenSegment(LogSegment openSegment) {
    LOG.trace("{}: setOpenSegment to {}", name, openSegment);
    Preconditions.assertNull(this.openSegment, "this.openSegment");
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
    Preconditions.assertTrue(openSegment != null && openSegment.numOfEntries() > 0,
        () -> "The number of entries of " + openSegment + " is " + openSegment.numOfEntries());
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
      return closedSegments.search(index);
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
  LogEntryHeader[] getTermIndices(final long startIndex, final long endIndex) {
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
      return LogEntryHeader.EMPTY_ARRAY;
    }
    return closedSegments.getTermIndex(startIndex, realEnd, openSegment);
  }

  private static void getFromSegment(LogSegment segment, long startIndex,
      LogEntryHeader[] entries, int offset, int size) {
    long endIndex = segment.getEndIndex();
    endIndex = Math.min(endIndex, startIndex + size - 1);
    int index = offset;
    for (long i = startIndex; i <= endIndex; i++) {
      entries[index++] = Optional.ofNullable(segment.getLogRecord(i)).map(LogRecord::getLogEntryHeader).orElse(null);
    }
  }

  long getStartIndex() {
    try (AutoCloseableLock readLock = closedSegments.readLock()) {
      if (closedSegments.isEmpty()) {
        return Optional.ofNullable(openSegment).map(LogSegment::getStartIndex).orElse(RaftLog.INVALID_LOG_INDEX);
      } else {
        return closedSegments.get(0).getStartIndex();
      }
    }
  }

  long getEndIndex() {
    try (AutoCloseableLock readLock = closedSegments.readLock()) {
      return openSegment != null ? openSegment.getEndIndex() :
          (closedSegments.isEmpty() ?
              RaftLog.INVALID_LOG_INDEX:
              closedSegments.get(closedSegments.size() - 1).getEndIndex());
    }
  }

  long getLastIndexInClosedSegments() {
    try (AutoCloseableLock readLock = closedSegments.readLock()) {
      return (closedSegments.isEmpty() ? RaftLog.INVALID_LOG_INDEX :
          closedSegments.get(closedSegments.size() - 1).getEndIndex());
    }
  }

  TermIndex getLastTermIndex() {
    try (AutoCloseableLock readLock = closedSegments.readLock()) {
      return (openSegment != null && openSegment.numOfEntries() > 0) ?
          openSegment.getLastTermIndex() :
          (closedSegments.isEmpty() ? null :
              closedSegments.get(closedSegments.size() - 1).getLastTermIndex());
    }
  }

  void appendEntry(LogEntryProto entry, LogSegment.Op op) {
    // SegmentedRaftLog does the segment creation/rolling work. Here we just
    // simply append the entry into the open segment.
    Preconditions.assertNotNull(openSegment, "openSegment");
    openSegment.appendToOpenSegment(entry, op);
  }

  /**
   * truncate log entries starting from the given index (inclusive)
   */
  TruncationSegments truncate(long index) {
    return closedSegments.truncate(index, openSegment, this::clearOpenSegment);
  }

  TruncationSegments purge(long index) {
    return closedSegments.purge(index);
  }

  Iterator<TermIndex> iterator(long startIndex) {
    return new EntryIterator(startIndex);
  }

  static class TruncateIndices {
    private final int arrayIndex;
    private final long truncateIndex;

    TruncateIndices(int arrayIndex, long truncateIndex) {
      this.arrayIndex = arrayIndex;
      this.truncateIndex = truncateIndex;
    }

    int getArrayIndex() {
      return arrayIndex;
    }

    long getTruncateIndex() {
      return truncateIndex;
    }
  }

  TruncateIndices computeTruncateIndices(Consumer<TermIndex> failClientRequest, List<LogEntryProto> entries) {
    int arrayIndex = 0;
    long truncateIndex = -1;

    try(AutoCloseableLock readLock = closedSegments.readLock()) {
      final Iterator<TermIndex> i = iterator(entries.get(0).getIndex());
      for(; i.hasNext() && arrayIndex < entries.size(); arrayIndex++) {
        final TermIndex storedEntry = i.next();
        LogEntryProto logEntryProto = entries.get(arrayIndex);
        Preconditions.assertTrue(storedEntry.getIndex() == logEntryProto.getIndex(),
            "The stored entry's index %s is not consistent with the received entries[%s]'s index %s",
            storedEntry.getIndex(), arrayIndex, logEntryProto.getIndex());

        if (storedEntry.getTerm() != logEntryProto.getTerm()) {
          // we should truncate from the storedEntry's arrayIndex
          truncateIndex = storedEntry.getIndex();
          if (LOG.isTraceEnabled()) {
            LOG.trace("{}: truncate to {}, arrayIndex={}, ti={}, storedEntry={}, entries={}",
                name, truncateIndex, arrayIndex,
                TermIndex.valueOf(logEntryProto), storedEntry,
                LogProtoUtils.toLogEntriesString(entries));
          }

          // fail all requests starting at truncateIndex
          failClientRequest.accept(storedEntry);
          for(; i.hasNext(); ) {
            failClientRequest.accept(i.next());
          }
          break;
        }
      }
    }
    return new TruncateIndices(arrayIndex, truncateIndex);
  }

  private class EntryIterator implements Iterator<TermIndex> {
    private long nextIndex;
    private LogSegment currentSegment;
    private int segmentIndex;

    EntryIterator(long start) {
      this.nextIndex = start;
      segmentIndex = closedSegments.binarySearch(nextIndex);
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
          Preconditions.assertTrue(segmentIndex == 0,
              () -> "segmentIndex is expected to be 0 but segmentIndex = " + segmentIndex);
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

  void close() {
    if (openSegment != null) {
      openSegment.clear();
      clearOpenSegment();
    }
    closedSegments.clear();
  }
}
