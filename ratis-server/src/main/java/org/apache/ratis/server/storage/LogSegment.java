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

import org.apache.ratis.server.impl.ConfigurationManager;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.shaded.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto.LogEntryBodyCase.CONFIGURATIONENTRY;

/**
 * In-memory cache for a log segment file. All the updates will be first written
 * into LogSegment then into corresponding files in the same order.
 *
 * This class will be protected by the RaftServer's lock.
 */
class LogSegment implements Comparable<Long> {
  static class LogRecord {
    /** starting offset in the file */
    final long offset;
    final LogEntryProto entry;

    LogRecord(long offset, LogEntryProto entry) {
      this.offset = offset;
      this.entry = entry;
    }
  }

  static class SegmentFileInfo {
    final long startIndex; // start index of the
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
  }

  static long getEntrySize(LogEntryProto entry) {
    final int serialized = entry.getSerializedSize();
    return serialized + CodedOutputStream.computeUInt32SizeNoTag(serialized) + 4;
  }

  private boolean isOpen;
  private final List<LogRecord> records = new ArrayList<>();
  private long totalSize;
  private final long startIndex;
  private long endIndex;

  private LogSegment(boolean isOpen, long start, long end) {
    this.isOpen = isOpen;
    this.startIndex = start;
    this.endIndex = end;
    totalSize = SegmentedRaftLog.HEADER_BYTES.length;
  }

  static LogSegment newOpenSegment(long start) {
    Preconditions.assertTrue(start >= 0);
    return new LogSegment(true, start, start - 1);
  }

  private static LogSegment newCloseSegment(long start, long end) {
    Preconditions.assertTrue(start >= 0 && end >= start);
    return new LogSegment(false, start, end);
  }

  static LogSegment loadSegment(File file, long start, long end, boolean isOpen,
      ConfigurationManager confManager) throws IOException {
    final LogSegment segment;
    try (LogInputStream in = new LogInputStream(file, start, end, isOpen)) {
      segment = isOpen ? LogSegment.newOpenSegment(start) :
          LogSegment.newCloseSegment(start, end);
      LogEntryProto next;
      LogEntryProto prev = null;
      while ((next = in.nextEntry()) != null) {
        if (prev != null) {
          Preconditions.assertTrue(next.getIndex() == prev.getIndex() + 1,
              "gap between entry %s and entry %s", prev, next);
        }
        segment.append(next);
        if (confManager != null &&
            next.getLogEntryBodyCase() == CONFIGURATIONENTRY) {
          confManager.addConfiguration(next.getIndex(),
              ServerProtoUtils.toRaftConfiguration(next.getIndex(),
                  next.getConfigurationEntry()));
        }
        prev = next;
      }
    }

    // truncate padding if necessary
    if (file.length() > segment.getTotalSize()) {
      FileUtils.truncateFile(file, segment.getTotalSize());
    }

    Preconditions.assertTrue(start == segment.records.get(0).entry.getIndex());
    if (!isOpen) {
      Preconditions.assertTrue(segment.getEndIndex() == end);
    }
    return segment;
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
    return (int) (endIndex - startIndex + 1);
  }

  void appendToOpenSegment(LogEntryProto... entries) {
    Preconditions.assertTrue(isOpen(),
        "The log segment %s is not open for append", this.toString());
    append(entries);
  }

  private void append(LogEntryProto... entries) {
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
            entry.getIndex() == currentLast.entry.getIndex() + 1,
            "gap between entries %s and %s", entry.getIndex(),
            currentLast.entry.getIndex());
      }

      final LogRecord record = new LogRecord(totalSize, entry);
      records.add(record);
      totalSize += getEntrySize(entry);
      endIndex = entry.getIndex();
    }
  }

  LogRecord getLogRecord(long index) {
    if (index >= startIndex && index <= endIndex) {
      return records.get((int) (index - startIndex));
    }
    return null;
  }

  LogRecord getLastRecord() {
    return records.isEmpty() ? null : records.get(records.size() - 1);
  }

  long getTotalSize() {
    return totalSize;
  }

  /**
   * Remove records from the given index (inclusive)
   */
  void truncate(long fromIndex) {
    Preconditions.assertTrue(fromIndex >= startIndex && fromIndex <= endIndex);
    LogRecord record = records.get((int) (fromIndex - startIndex));
    for (long index = endIndex; index >= fromIndex; index--) {
      records.remove((int)(index - startIndex));
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
    endIndex = startIndex - 1;
  }
}
