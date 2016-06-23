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
import org.apache.hadoop.raft.server.RaftConstants;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * In-memory cache for a log segment file. All the updates will be first written
 * into LogSegment then into corresponding files in the same order.
 *
 * This class will be protected by the RaftServer's lock.
 */
public class LogSegment {

  static class LogRecord {
    /** starting offset in the file */
    final int offset;
    final LogEntryProto entry;

    LogRecord(int offset, LogEntryProto entry) {
      this.offset = offset;
      this.entry = entry;
    }
  }

  private File file;
  private boolean isOpen;
  private final List<LogRecord> records = new ArrayList<>();
  private int totalSize;
  private final long startIndex;
  private long endIndex;

  private LogSegment(File file, boolean isOpen, long start, long end) {
    this.file = file;
    this.isOpen = isOpen;
    this.startIndex = start;
    this.endIndex = end;
  }

  public static LogSegment newOpenSegment(File file, long start) {
    Preconditions.checkArgument(start >= 0);
    return new LogSegment(file, true, start, RaftConstants.INVALID_LOG_INDEX);
  }

  public static LogSegment newCloseSegment(File file, long start, long end) {
    Preconditions.checkArgument(start >= 0 && end >= start);
    return new LogSegment(file, false, start, end);
  }

  public void load() {
    // load records from a log segment file, update totalSize
  }

  public boolean isOpen() {
    return isOpen;
  }

  public void append(LogEntryProto... entries) {
    Preconditions.checkState(isOpen(),
        "The log segment %s is not open for append", this.toString());
    Preconditions.checkArgument(entries != null && entries.length > 0);
    final long term = entries[0].getTerm();
    for (LogEntryProto entry : entries) {
      // all these entries should be of the same term
      Preconditions.checkArgument(entry.getTerm() == term,
          "expected term:%s, term of the entry:%s", term, entry.getTerm());
      final LogRecord record = new LogRecord(totalSize, entry);
      records.add(record);
      totalSize += entry.getSerializedSize();
      endIndex = entry.getIndex();
    }
  }

  /**
   * Remove records from the given index (inclusive)
   */
  public void truncate(long fromIndex) {

  }

  public void close() {
    Preconditions.checkState(isOpen());
    isOpen = false;
    // TODO: change file
  }

  @Override
  public String toString() {
    return isOpen() ? "log-" + startIndex + "-inprogress" :
        "log-" + startIndex + "-" + endIndex;
  }
}
