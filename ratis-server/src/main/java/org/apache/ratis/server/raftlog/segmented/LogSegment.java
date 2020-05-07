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
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.metrics.RaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * In-memory cache for a log segment file. All the updates will be first written
 * into LogSegment then into corresponding files in the same order.
 *
 * This class will be protected by the {@link SegmentedRaftLog}'s read-write lock.
 */
@SuppressWarnings("checkstyle:FinalClass")
public class LogSegment implements Comparable<Long> {

  //TODO: This class needs to be made final to address checkstyle issue. However, TestCacheEviction fails as Mockito
  // cannot spy final class. This problem can be fixed when stable version of Mockito 2.x is available which provides
  // a feature to mock final class/method.

  static final Logger LOG = LoggerFactory.getLogger(LogSegment.class);

  static long getEntrySize(LogEntryProto entry) {
    final int serialized = ServerProtoUtils.removeStateMachineData(entry).getSerializedSize();
    return serialized + CodedOutputStream.computeUInt32SizeNoTag(serialized) + 4;
  }

  static class LogRecord {
    /** starting offset in the file */
    private final long offset;
    private final TermIndex termIndex;

    LogRecord(long offset, LogEntryProto entry) {
      this.offset = offset;
      this.termIndex = ServerProtoUtils.toTermIndex(entry);
    }

    TermIndex getTermIndex() {
      return termIndex;
    }

    long getOffset() {
      return offset;
    }
  }

  static LogSegment newOpenSegment(RaftStorage storage, long start, RaftLogMetrics raftLogMetrics) {
    Preconditions.assertTrue(start >= 0);
    return new LogSegment(storage, true, start, start - 1, raftLogMetrics);
  }

  @VisibleForTesting
  static LogSegment newCloseSegment(RaftStorage storage,
      long start, long end, RaftLogMetrics raftLogMetrics) {
    Preconditions.assertTrue(start >= 0 && end >= start);
    return new LogSegment(storage, false, start, end, raftLogMetrics);
  }

  public static int readSegmentFile(File file, long start, long end,
      boolean isOpen, CorruptionPolicy corruptionPolicy,
      RaftLogMetrics raftLogMetrics, Consumer<LogEntryProto> entryConsumer) throws
      IOException {
    int count = 0;
    try (SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(file, start, end, isOpen, raftLogMetrics)) {
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
          LOG.warn("Failed to read segment file {} (start={}, end={}, isOpen? {}): only {} entries read successfully",
              file, start, end, isOpen, count, ioe);
          break;
        default:
          throw new IllegalStateException("Unexpected enum value: " + corruptionPolicy
              + ", class=" + CorruptionPolicy.class);
      }
    }

    return count;
  }

  @SuppressWarnings("parameternumber")
  static LogSegment loadSegment(RaftStorage storage, File file, long start, long end, boolean isOpen,
      boolean keepEntryInCache, Consumer<LogEntryProto> logConsumer, RaftLogMetrics raftLogMetrics)
      throws IOException {
    final LogSegment segment = isOpen ?
        LogSegment.newOpenSegment(storage, start, raftLogMetrics) :
        LogSegment.newCloseSegment(storage, start, end, raftLogMetrics);

    final CorruptionPolicy corruptionPolicy = CorruptionPolicy.get(storage, RaftStorage::getLogCorruptionPolicy);
    final int entryCount = readSegmentFile(file, start, end, isOpen, corruptionPolicy, raftLogMetrics, entry -> {
      segment.append(keepEntryInCache || isOpen, entry);
      if (logConsumer != null) {
        logConsumer.accept(entry);
      }
    });
    LOG.info("Successfully read {} entries from segment file {}", entryCount, file);

    final int expectedEntryCount = Math.toIntExact(end - start + 1);
    final boolean corrupted = entryCount != expectedEntryCount;
    if (corrupted) {
      LOG.warn("Segment file is corrupted: expected to have {} entries but only {} entries read successfully",
          expectedEntryCount, entryCount);
    }

    if (entryCount == 0) {
      // The segment does not have any entries, delete the file.
      FileUtils.deleteFile(file);
      return null;
    } else if (file.length() > segment.getTotalSize()) {
      // The segment has extra padding, truncate it.
      FileUtils.truncateFile(file, segment.getTotalSize());
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
    if (!isOpen && !corrupted) {
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
    private RaftLogMetrics raftLogMetrics;

    LogEntryLoader(RaftLogMetrics raftLogMetrics) {
      this.raftLogMetrics = raftLogMetrics;
    }

    @Override
    public LogEntryProto load(LogRecord key) throws IOException {
      final File file = getSegmentFile();
      // note the loading should not exceed the endIndex: it is possible that
      // the on-disk log file should be truncated but has not been done yet.
      readSegmentFile(file, startIndex, endIndex, isOpen, getLogCorruptionPolicy(), raftLogMetrics,
          entry -> entryCache.put(ServerProtoUtils.toTermIndex(entry), entry));
      loadingTimes.incrementAndGet();
      return Objects.requireNonNull(entryCache.get(key.getTermIndex()));
    }
  }

  private File getSegmentFile() {
    return isOpen ?
        storage.getStorageDir().getOpenLogFile(startIndex) :
        storage.getStorageDir().getClosedLogFile(startIndex, endIndex);
  }

  private volatile boolean isOpen;
  private long totalSize = SegmentedRaftLogFormat.getHeaderLength();
  /** Segment start index, inclusive. */
  private long startIndex;
  /** Segment end index, inclusive. */
  private volatile long endIndex;
  private RaftStorage storage;
  private RaftLogMetrics raftLogMetrics;
  private final LogEntryLoader cacheLoader = new LogEntryLoader(raftLogMetrics);
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
  private final Set<TermIndex> configEntries = new HashSet<>();

  private LogSegment(RaftStorage storage, boolean isOpen, long start, long end, RaftLogMetrics raftLogMetrics) {
    this.storage = storage;
    this.isOpen = isOpen;
    this.startIndex = start;
    this.endIndex = end;
    this.raftLogMetrics = raftLogMetrics;
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

  void appendToOpenSegment(LogEntryProto entry) {
    Preconditions.assertTrue(isOpen(), "The log segment %s is not open for append", this);
    append(true, entry);
  }

  private void append(boolean keepEntryInCache, LogEntryProto entry) {
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

    final LogRecord record = new LogRecord(totalSize, entry);
    records.add(record);
    if (keepEntryInCache) {
      entryCache.put(record.getTermIndex(), entry);
    }
    if (entry.hasConfigurationEntry()) {
      configEntries.add(record.getTermIndex());
    }
    totalSize += getEntrySize(entry);
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
    } catch (Exception e) {
      throw new RaftLogIOException(e);
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

  boolean isConfigEntry(TermIndex ti) {
    return configEntries.contains(ti);
  }

  long getTotalSize() {
    return totalSize;
  }

  /**
   * Remove records from the given index (inclusive)
   */
  synchronized void truncate(long fromIndex) {
    Preconditions.assertTrue(fromIndex >= startIndex && fromIndex <= endIndex);
    for (long index = endIndex; index >= fromIndex; index--) {
      LogRecord removed = records.remove(Math.toIntExact(index - startIndex));
      entryCache.remove(removed.getTermIndex());
      configEntries.remove(removed.getTermIndex());
      totalSize = removed.offset;
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

  @Override
  public int compareTo(Long l) {
    return (l >= getStartIndex() && l <= getEndIndex()) ? 0 :
        (this.getEndIndex() < l ? -1 : 1);
  }

  synchronized void clear() {
    records.clear();
    entryCache.clear();
    configEntries.clear();
    endIndex = startIndex - 1;
  }

  int getLoadingTimes() {
    return loadingTimes.get();
  }

  synchronized void evictCache() {
    entryCache.clear();
  }

  boolean hasCache() {
    return isOpen || !entryCache.isEmpty(); // open segment always has cache.
  }

  boolean containsIndex(long index) {
    return startIndex <= index && endIndex >= index;
  }
}
