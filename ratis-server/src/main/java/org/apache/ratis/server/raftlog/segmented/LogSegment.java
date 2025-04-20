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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


/**
 * In-memory cache for a log segment file. All the updates will be first written
 * into LogSegment then into corresponding files in the same order.
 *
 * This class will be protected by the {@link SegmentedRaftLog}'s read-write lock.
 */
public final class LogSegment {

  static final Logger LOG = LoggerFactory.getLogger(LogSegment.class);

  enum Op {
    LOAD_SEGMENT_FILE,
    REMOVE_CACHE,
    CHECK_SEGMENT_FILE_FULL,
    WRITE_CACHE_WITH_STATE_MACHINE_CACHE,
    WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE
  }

  static long getEntrySize(LogEntryProto entry, Op op) {
    LogEntryProto e = entry;
    if (op == Op.CHECK_SEGMENT_FILE_FULL) {
      e = LogProtoUtils.removeStateMachineData(entry);
    } else if (op == Op.LOAD_SEGMENT_FILE || op == Op.WRITE_CACHE_WITH_STATE_MACHINE_CACHE) {
      Preconditions.assertTrue(entry == LogProtoUtils.removeStateMachineData(entry),
          () -> "Unexpected LogEntryProto with StateMachine data: op=" + op + ", entry=" + entry);
    } else {
      Preconditions.assertTrue(op == Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE || op == Op.REMOVE_CACHE,
          () -> "Unexpected op " + op + ", entry=" + entry);
    }
    final int serialized = e.getSerializedSize();
    return serialized + CodedOutputStream.computeUInt32SizeNoTag(serialized) + 4L;
  }

  static class LogRecord {
    /** starting offset in the file */
    private final long offset;
    private final LogEntryHeader logEntryHeader;

    LogRecord(long offset, LogEntryProto entry) {
      this.offset = offset;
      this.logEntryHeader = LogEntryHeader.valueOf(entry);
    }

    LogEntryHeader getLogEntryHeader() {
      return logEntryHeader;
    }

    TermIndex getTermIndex() {
      return getLogEntryHeader().getTermIndex();
    }

    long getOffset() {
      return offset;
    }
  }

  static LogSegment newOpenSegment(RaftStorage storage, long start, SizeInBytes maxOpSize,
      SegmentedRaftLogMetrics raftLogMetrics) {
    Preconditions.assertTrue(start >= 0);
    return new LogSegment(storage, true, start, start - 1, maxOpSize, raftLogMetrics);
  }

  @VisibleForTesting
  static LogSegment newCloseSegment(RaftStorage storage,
      long start, long end, SizeInBytes maxOpSize, SegmentedRaftLogMetrics raftLogMetrics) {
    Preconditions.assertTrue(start >= 0 && end >= start);
    return new LogSegment(storage, false, start, end, maxOpSize, raftLogMetrics);
  }

  static LogSegment newLogSegment(RaftStorage storage, LogSegmentStartEnd startEnd, SizeInBytes maxOpSize,
      SegmentedRaftLogMetrics metrics) {
    return startEnd.isOpen()? newOpenSegment(storage, startEnd.getStartIndex(), maxOpSize, metrics)
        : newCloseSegment(storage, startEnd.getStartIndex(), startEnd.getEndIndex(), maxOpSize, metrics);
  }

  public static int readSegmentFile(File file, LogSegmentStartEnd startEnd, SizeInBytes maxOpSize,
      CorruptionPolicy corruptionPolicy, SegmentedRaftLogMetrics raftLogMetrics, Consumer<LogEntryProto> entryConsumer)
      throws IOException {
    int count = 0;
    try(SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(file, startEnd, maxOpSize, raftLogMetrics)) {
      for(LogEntryProto prev = null, next; (next = in.nextEntry()) != null; prev = next) {
        if (prev != null) {
          Preconditions.assertTrue(next.getIndex() == prev.getIndex() + 1,
              "gap between entry %s and entry %s", prev, next);
        }

        if (entryConsumer != null) {
          entryConsumer.accept(next);
        }
        count++;
      }
    } catch (IOException ioe) {
      switch (corruptionPolicy) {
        case EXCEPTION: throw ioe;
        case WARN_AND_RETURN:
          LOG.warn("Failed to read segment file {} ({}): only {} entries read successfully",
              file, startEnd, count, ioe);
          break;
        default:
          throw new IllegalStateException("Unexpected enum value: " + corruptionPolicy
              + ", class=" + CorruptionPolicy.class);
      }
    }

    return count;
  }

  static LogSegment loadSegment(RaftStorage storage, File file, LogSegmentStartEnd startEnd, SizeInBytes maxOpSize,
      boolean keepEntryInCache, Consumer<LogEntryProto> logConsumer, SegmentedRaftLogMetrics raftLogMetrics)
      throws IOException {
    final LogSegment segment = newLogSegment(storage, startEnd, maxOpSize, raftLogMetrics);
    final CorruptionPolicy corruptionPolicy = CorruptionPolicy.get(storage, RaftStorage::getLogCorruptionPolicy);
    final boolean isOpen = startEnd.isOpen();
    final int entryCount = readSegmentFile(file, startEnd, maxOpSize, corruptionPolicy, raftLogMetrics, entry -> {
      segment.append(keepEntryInCache || isOpen, entry, Op.LOAD_SEGMENT_FILE);
      if (logConsumer != null) {
        logConsumer.accept(entry);
      }
    });
    LOG.info("Successfully read {} entries from segment file {}", entryCount, file);

    final long start = startEnd.getStartIndex();
    final long end = isOpen? segment.getEndIndex(): startEnd.getEndIndex();
    final int expectedEntryCount = Math.toIntExact(end - start + 1);
    final boolean corrupted = entryCount != expectedEntryCount;
    if (corrupted) {
      LOG.warn("Segment file is corrupted: expected to have {} entries but only {} entries read successfully",
          expectedEntryCount, entryCount);
    }

    if (entryCount == 0) {
      // The segment does not have any entries, delete the file.
      final Path deleted = FileUtils.deleteFile(file);
      LOG.info("Deleted RaftLog segment since entry count is zero: startEnd={}, path={}", startEnd, deleted);
      return null;
    } else if (file.length() > segment.getTotalFileSize()) {
      // The segment has extra padding, truncate it.
      FileUtils.truncateFile(file, segment.getTotalFileSize());
    }

    try {
      segment.assertSegment(start, entryCount, corrupted, end);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read segment file " + file, e);
    }
    return segment;
  }

  private void assertSegment(long expectedStart, int expectedEntryCount, boolean corrupted, long expectedEnd) {
    Preconditions.assertSame(expectedStart, getStartIndex(), "Segment start index");
    Preconditions.assertSame(expectedEntryCount, records.size(), "Number of records");

    final long expectedLastIndex = expectedStart + expectedEntryCount - 1;
    Preconditions.assertSame(expectedLastIndex, getEndIndex(), "Segment end index");

    final LogRecord last = getLastRecord();
    if (last != null) {
      Preconditions.assertSame(expectedLastIndex, last.getTermIndex().getIndex(), "Index at the last record");
      Preconditions.assertSame(expectedStart, records.get(0).getTermIndex().getIndex(), "Index at the first record");
    }
    if (!corrupted) {
      Preconditions.assertSame(expectedEnd, expectedLastIndex, "End/last Index");
    }
  }

  /**
   * The current log entry loader simply loads the whole segment into the memory.
   * In most of the cases this may be good enough considering the main use case
   * for load log entries is for leader appending to followers.
   *
   * In the future we can make the cache loader configurable if necessary.
   */
  class LogEntryLoader extends CacheLoader<LogRecord, LogEntryProto> {
    private final SegmentedRaftLogMetrics raftLogMetrics;

    LogEntryLoader(SegmentedRaftLogMetrics raftLogMetrics) {
      this.raftLogMetrics = raftLogMetrics;
    }

    @Override
    public LogEntryProto load(LogRecord key) throws IOException {
      final File file = getFile();
      // note the loading should not exceed the endIndex: it is possible that
      // the on-disk log file should be truncated but has not been done yet.
      final AtomicReference<LogEntryProto> toReturn = new AtomicReference<>();
      final LogSegmentStartEnd startEnd = LogSegmentStartEnd.valueOf(startIndex, endIndex, isOpen);
      readSegmentFile(file, startEnd, maxOpSize,
          getLogCorruptionPolicy(), raftLogMetrics, entry -> {
        final TermIndex ti = TermIndex.valueOf(entry);
        putEntryCache(ti, entry, Op.LOAD_SEGMENT_FILE);
        if (ti.equals(key.getTermIndex())) {
          toReturn.set(entry);
        }
      });
      loadingTimes.incrementAndGet();
      final LogEntryProto proto = toReturn.get();
      if (proto == null) {
        throw new RaftLogIOException("Failed to load log entry " + key);
      }
      return proto;
    }
  }

  File getFile() {
    return LogSegmentStartEnd.valueOf(startIndex, endIndex, isOpen).getFile(storage);
  }

  private volatile boolean isOpen;
  private long totalFileSize = SegmentedRaftLogFormat.getHeaderLength();
  private AtomicLong totalCacheSize = new AtomicLong(0);
  /** Segment start index, inclusive. */
  private final long startIndex;
  /** Segment end index, inclusive. */
  private volatile long endIndex;
  private final RaftStorage storage;
  private final SizeInBytes maxOpSize;
  private final LogEntryLoader cacheLoader;
  /** later replace it with a metric */
  private final AtomicInteger loadingTimes = new AtomicInteger();

  /**
   * the list of records is more like the index of a segment
   */
  private final List<LogRecord> records = new ArrayList<>();
  /**
   * the entryCache caches the content of log entries.
   */
  private final Map<TermIndex, LogEntryProto> entryCache = new ConcurrentHashMap<>();

  private LogSegment(RaftStorage storage, boolean isOpen, long start, long end, SizeInBytes maxOpSize,
      SegmentedRaftLogMetrics raftLogMetrics) {
    this.storage = storage;
    this.isOpen = isOpen;
    this.startIndex = start;
    this.endIndex = end;
    this.maxOpSize = maxOpSize;
    this.cacheLoader = new LogEntryLoader(raftLogMetrics);
  }

  long getStartIndex() {
    return startIndex;
  }

  long getEndIndex() {
    return endIndex;
  }

  boolean isOpen() {
    return isOpen;
  }

  int numOfEntries() {
    return Math.toIntExact(endIndex - startIndex + 1);
  }

  CorruptionPolicy getLogCorruptionPolicy() {
    return CorruptionPolicy.get(storage, RaftStorage::getLogCorruptionPolicy);
  }

  void appendToOpenSegment(LogEntryProto entry, Op op) {
    Preconditions.assertTrue(isOpen(), "The log segment %s is not open for append", this);
    append(true, entry, op);
  }

  private void append(boolean keepEntryInCache, LogEntryProto entry, Op op) {
    Objects.requireNonNull(entry, "entry == null");
    if (records.isEmpty()) {
      Preconditions.assertTrue(entry.getIndex() == startIndex,
          "gap between start index %s and first entry to append %s",
          startIndex, entry.getIndex());
    }

    final LogRecord currentLast = getLastRecord();
    if (currentLast != null) {
      Preconditions.assertTrue(entry.getIndex() == currentLast.getTermIndex().getIndex() + 1,
          "gap between entries %s and %s", entry.getIndex(), currentLast.getTermIndex().getIndex());
    }

    final LogRecord record = new LogRecord(totalFileSize, entry);
    if (keepEntryInCache) {
      putEntryCache(record.getTermIndex(), entry, op);
    }
    records.add(record);
    totalFileSize += getEntrySize(entry, op);
    endIndex = entry.getIndex();
  }

  LogEntryProto getEntryFromCache(TermIndex ti) {
    return entryCache.get(ti);
  }

  /**
   * Acquire LogSegment's monitor so that there is no concurrent loading.
   */
  synchronized LogEntryProto loadCache(LogRecord record) throws RaftLogIOException {
    LogEntryProto entry = entryCache.get(record.getTermIndex());
    if (entry != null) {
      return entry;
    }
    try {
      return cacheLoader.load(record);
    } catch (RaftLogIOException e) {
      throw e;
    } catch (Exception e) {
      throw new RaftLogIOException("Failed to loadCache for log entry " + record, e);
    }
  }

  LogRecord getLogRecord(long index) {
    if (index >= startIndex && index <= endIndex) {
      return records.get(Math.toIntExact(index - startIndex));
    }
    return null;
  }

  private LogRecord getLastRecord() {
    return records.isEmpty() ? null : records.get(records.size() - 1);
  }

  TermIndex getLastTermIndex() {
    LogRecord last = getLastRecord();
    return last == null ? null : last.getTermIndex();
  }

  long getTotalFileSize() {
    return totalFileSize;
  }

  long getTotalCacheSize() {
    return totalCacheSize.get();
  }

  /**
   * Remove records from the given index (inclusive)
   */
  synchronized void truncate(long fromIndex) {
    Preconditions.assertTrue(fromIndex >= startIndex && fromIndex <= endIndex);
    for (long index = endIndex; index >= fromIndex; index--) {
      LogRecord removed = records.remove(Math.toIntExact(index - startIndex));
      removeEntryCache(removed.getTermIndex(), Op.REMOVE_CACHE);
      totalFileSize = removed.offset;
    }
    isOpen = false;
    this.endIndex = fromIndex - 1;
  }

  void close() {
    Preconditions.assertTrue(isOpen());
    isOpen = false;
  }

  @Override
  public String toString() {
    return isOpen() ? "log_" + "inprogress_" + startIndex :
        "log-" + startIndex + "_" + endIndex;
  }

  /** Comparator to find <code>index</code> in list of <code>LogSegment</code>s. */
  static final Comparator<Object> SEGMENT_TO_INDEX_COMPARATOR = (o1, o2) -> {
    if (o1 instanceof LogSegment && o2 instanceof Long) {
      return ((LogSegment) o1).compareTo((Long) o2);
    } else if (o1 instanceof Long && o2 instanceof LogSegment) {
      return Integer.compare(0, ((LogSegment) o2).compareTo((Long) o1));
    }
    throw new IllegalStateException("Unexpected objects to compare(" + o1 + "," + o2 + ")");
  };

  private int compareTo(Long l) {
    return (l >= getStartIndex() && l <= getEndIndex()) ? 0 :
        (this.getEndIndex() < l ? -1 : 1);
  }

  synchronized void clear() {
    records.clear();
    evictCache();
    endIndex = startIndex - 1;
  }

  int getLoadingTimes() {
    return loadingTimes.get();
  }

  void evictCache() {
    entryCache.clear();
    totalCacheSize.set(0);
  }

  void putEntryCache(TermIndex key, LogEntryProto value, Op op) {
    final LogEntryProto previous = entryCache.put(key, value);
    long previousSize = 0;
    if (previous != null) {
      // Different threads maybe load LogSegment file into cache at the same time, so duplicate maybe happen
      previousSize = getEntrySize(value, Op.REMOVE_CACHE);
    }
    totalCacheSize.getAndAdd(getEntrySize(value, op) - previousSize);
  }

  void removeEntryCache(TermIndex key, Op op) {
    LogEntryProto value = entryCache.remove(key);
    if (value != null) {
      totalCacheSize.getAndAdd(-getEntrySize(value, op));
    }
  }

  boolean hasCache() {
    return isOpen || !entryCache.isEmpty(); // open segment always has cache.
  }

  boolean containsIndex(long index) {
    return startIndex <= index && endIndex >= index;
  }

  boolean hasEntries() {
    return numOfEntries() > 0;
  }

}
