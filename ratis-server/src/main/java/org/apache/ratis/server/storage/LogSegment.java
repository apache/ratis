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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
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
 * This class will be protected by the RaftServer's lock.
 */
class LogSegment implements Comparable<Long> {
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
      termIndex = TermIndex.newTermIndex(entry.getTerm(), entry.getIndex());
    }

    TermIndex getTermIndex() {
      return termIndex;
    }

    long getOffset() {
      return offset;
    }
  }

  static class LogRecordWithEntry {
    private final LogRecord record;
    private final LogEntryProto entry;

    LogRecordWithEntry(LogRecord record, LogEntryProto entry) {
      this.record = record;
      this.entry = entry;
    }

    LogRecord getRecord() {
      return record;
    }

    LogEntryProto getEntry() {
      return entry;
    }

    boolean hasEntry() {
      return entry != null;
    }
  }

  static LogSegment newOpenSegment(RaftStorage storage, long start) {
    Preconditions.assertTrue(start >= 0);
    return new LogSegment(storage, true, start, start - 1);
  }

  @VisibleForTesting
  static LogSegment newCloseSegment(RaftStorage storage,
      long start, long end) {
    Preconditions.assertTrue(start >= 0 && end >= start);
    return new LogSegment(storage, false, start, end);
  }

  private static int readSegmentFile(File file, long start, long end,
      boolean isOpen, Consumer<LogEntryProto> entryConsumer) throws IOException {
    int count = 0;
    try (LogInputStream in = new LogInputStream(file, start, end, isOpen)) {
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
    }
    return count;
  }

  static LogSegment loadSegment(RaftStorage storage, File file,
      long start, long end, boolean isOpen,
      boolean keepEntryInCache, Consumer<LogEntryProto> logConsumer)
      throws IOException {
    final LogSegment segment = isOpen ?
        LogSegment.newOpenSegment(storage, start) :
        LogSegment.newCloseSegment(storage, start, end);

    final int entryCount = readSegmentFile(file, start, end, isOpen, entry -> {
      segment.append(keepEntryInCache || isOpen, entry);
      if (logConsumer != null) {
        logConsumer.accept(entry);
      }
    });
    LOG.info("Successfully read {} entries from segment file {}", entryCount, file);

    if (entryCount == 0) {
      // The segment does not have any entries, delete the file.
      FileUtils.deleteFile(file);
      return null;
    } else if (file.length() > segment.getTotalSize()) {
      // The segment has extra padding, truncate it.
      FileUtils.truncateFile(file, segment.getTotalSize());
    }

    Preconditions.assertTrue(start == segment.getStartIndex());
    if (!segment.records.isEmpty()) {
      Preconditions.assertTrue(start == segment.records.get(0).getTermIndex().getIndex());
    }
    if (!isOpen) {
      Preconditions.assertTrue(segment.getEndIndex() == end);
    }
    return segment;
  }

  /**
   * The current log entry loader simply loads the whole segment into the memory.
   * In most of the cases this may be good enough considering the main use case
   * for load log entries is for leader appending to followers.
   *
   * In the future we can make the cache loader configurable if necessary.
   */
  class LogEntryLoader extends CacheLoader<LogRecord, LogEntryProto> {
    @Override
    public LogEntryProto load(LogRecord key) throws IOException {
      final File file = getSegmentFile();
      // note the loading should not exceed the endIndex: it is possible that
      // the on-disk log file should be truncated but has not been done yet.
      readSegmentFile(file, startIndex, endIndex, isOpen,
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

  public String toDebugString() {
    final StringBuilder b = new StringBuilder()
        .append("startIndex=").append(startIndex)
        .append(", endIndex=").append(endIndex)
        .append(", numOfEntries=").append(numOfEntries())
        .append(", isOpen? ").append(isOpen)
        .append(", file=").append(getSegmentFile());
    records.stream().map(LogRecord::getTermIndex).forEach(
        ti -> b.append("  ").append(ti).append(", cache=")
            .append(ServerProtoUtils.toLogEntryString(entryCache.get(ti)))
    );
    return b.toString();
  }

  private volatile boolean isOpen;
  private long totalSize;
  private final long startIndex;
  private volatile long endIndex;
  private final RaftStorage storage;
  private final CacheLoader<LogRecord, LogEntryProto> cacheLoader = new LogEntryLoader();
  /** later replace it with a metric */
  private final AtomicInteger loadingTimes = new AtomicInteger();
  private volatile boolean hasEntryCache;

  /**
   * the list of records is more like the index of a segment
   */
  private final List<LogRecord> records = new ArrayList<>();
  /**
   * the entryCache caches the content of log entries.
   */
  private final Map<TermIndex, LogEntryProto> entryCache = new ConcurrentHashMap<>();
  private final Set<TermIndex> configEntries = new HashSet<>();

  private LogSegment(RaftStorage storage, boolean isOpen, long start, long end) {
    this.storage = storage;
    this.isOpen = isOpen;
    this.startIndex = start;
    this.endIndex = end;
    totalSize = SegmentedRaftLogFormat.getHeaderLength();
    hasEntryCache = isOpen;
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

  void appendToOpenSegment(LogEntryProto... entries) {
    Preconditions.assertTrue(isOpen(),
        "The log segment %s is not open for append", this.toString());
    append(true, entries);
  }

  private void append(boolean keepEntryInCache, LogEntryProto... entries) {
    Preconditions.assertTrue(entries != null && entries.length > 0);
    final long term = entries[0].getTerm();
    if (records.isEmpty()) {
      Preconditions.assertTrue(entries[0].getIndex() == startIndex,
          "gap between start index %s and first entry to append %s",
          startIndex, entries[0].getIndex());
    }
    for (LogEntryProto entry : entries) {
      // all these entries should be of the same term
      Preconditions.assertTrue(entry.getTerm() == term,
          "expected term:%s, term of the entry:%s", term, entry.getTerm());
      final LogRecord currentLast = getLastRecord();
      if (currentLast != null) {
        Preconditions.assertTrue(
            entry.getIndex() == currentLast.getTermIndex().getIndex() + 1,
            "gap between entries %s and %s", entry.getIndex(),
            currentLast.getTermIndex().getIndex());
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
  }

  LogRecordWithEntry getEntryWithoutLoading(long index) {
    LogRecord record = getLogRecord(index);
    if (record == null) {
      return null;
    }
    return new LogRecordWithEntry(record, entryCache.get(record.getTermIndex()));
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
      entry = cacheLoader.load(record);
      hasEntryCache = true;
      return entry;
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
  void truncate(long fromIndex) {
    Preconditions.assertTrue(fromIndex >= startIndex && fromIndex <= endIndex);
    LogRecord record = records.get(Math.toIntExact(fromIndex - startIndex));
    for (long index = endIndex; index >= fromIndex; index--) {
      LogRecord removed = records.remove(Math.toIntExact(index - startIndex));
      entryCache.remove(removed.getTermIndex());
      configEntries.remove(removed.getTermIndex());
    }
    totalSize = record.offset;
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

  void clear() {
    records.clear();
    entryCache.clear();
    hasEntryCache = false;
    configEntries.clear();
    endIndex = startIndex - 1;
  }

  public int getLoadingTimes() {
    return loadingTimes.get();
  }

  void evictCache() {
    hasEntryCache = false;
    entryCache.clear();
  }

  boolean hasCache() {
    return hasEntryCache;
  }

  boolean containsIndex(long index) {
    return startIndex <= index && endIndex >= index;
  }
}
