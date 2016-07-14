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
import org.apache.commons.io.Charsets;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.storage.RaftStorageDirectory.PathAndIndex;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * The RaftLog implementation that writes log entries into segmented files in
 * local disk.
 *
 * The max log segment size is 8MB. The real log segment size may not be
 * exactly equal to this limit. If a log entry's size exceeds 8MB, this entry
 * will be stored in a single segment.
 *
 * There are two types of segments: closed segment and open segment. The former
 * is named as "log_startindex-endindex", the later is named as
 * "log_inprogress_startindex".
 *
 * There can be multiple closed segments but there is at most one open segment.
 * When the open segment reaches the size limit, or the log term increases, we
 * close the open segment and start a new open segment. A closed segment cannot
 * be appended anymore, but it can be truncated in case that a follower's log is
 * inconsistent with the current leader.
 *
 * Every closed segment should be non-empty, i.e., it should contain at least
 * one entry.
 */
public class SegmentedRaftLog extends RaftLog implements Closeable {
  static final byte[] HEADER = "RAFTLOG1".getBytes(Charsets.UTF_8);

  private final RaftStorage storage;
  private final RaftLogCache cache;
  private final RaftLogWorker fileLogWorker;

  public SegmentedRaftLog(String selfId, File rootDir) throws IOException {
    super(selfId);
    storage = new RaftStorage(rootDir, RaftConstants.StartupOption.REGULAR);
    cache = new RaftLogCache();
    fileLogWorker = new RaftLogWorker(storage);
    loadLogSegments();
    fileLogWorker.start();
  }

  private void loadLogSegments() throws IOException {
    List<PathAndIndex> paths = storage.getStorageDir().getLogSegmentFiles();
    for (PathAndIndex pi : paths) {
      final LogSegment logSegment = parseLogSegment(pi);
      cache.addSegment(logSegment);
    }
  }

  // TODO: update state machine and configuration based on a passed-in callback
  private LogSegment parseLogSegment(PathAndIndex pi) throws IOException {
    final boolean isOpen = pi.endIndex == RaftConstants.INVALID_LOG_INDEX;
    return LogSegment.loadSegment(pi.path.toFile(), pi.startIndex, pi.endIndex,
        isOpen);
  }

  @Override
  public LogEntryProto get(long index) {
    return cache.getEntry(index);
  }

  @Override
  public LogEntryProto[] getEntries(long startIndex, long endIndex) {
    return cache.getEntries(startIndex, endIndex);
  }

  @Override
  public LogEntryProto getLastEntry() {
    return cache.getLastEntry();
  }

  /**
   * The method, along with {@link #appendEntry} and
   * {@link #append(LogEntryProto...)} need protection of RaftServer's lock.
   */
  @Override
  void truncate(long index) {
    RaftLogCache.TruncationSegments ts = cache.truncate(index);
    if (ts != null) {
      fileLogWorker.truncate(ts);
    }
  }

  @Override
  void appendEntry(LogEntryProto entry) {
    final LogSegment currentOpenSegment = cache.getOpenSegment();
    if (currentOpenSegment == null) {
      cache.addSegment(LogSegment.newOpenSegment(entry.getIndex()));
      fileLogWorker.startLogSegment(getNextIndex());
    } else if (currentOpenSegment.isFull()) {
      cache.rollOpenSegment(true);
      fileLogWorker.rollLogSegment(currentOpenSegment);
    } else if (currentOpenSegment.numOfEntries() > 0 &&
        currentOpenSegment.getLastRecord().entry.getTerm() != entry.getTerm()) {
      // the term changes
      final long currentTerm = currentOpenSegment.getLastRecord().entry.getTerm();
      Preconditions.checkState(currentTerm < entry.getTerm(),
          "open segment's term %s is larger than the new entry's term %s",
          currentTerm, entry.getTerm());
      cache.rollOpenSegment(true);
      fileLogWorker.rollLogSegment(currentOpenSegment);
    }

    cache.appendEntry(entry);
    fileLogWorker.writeLogEntry(entry);
  }

  @Override
  public void append(LogEntryProto... entries) {
    if (entries == null || entries.length == 0) {
      return;
    }
    Iterator<LogEntryProto> iter = cache.iterator(entries[0].getIndex());
    int index = 0;
    long truncateIndex = -1;
    for (;iter.hasNext() && index < entries.length; index++) {
      LogEntryProto storedEntry = iter.next();
      Preconditions.checkState(
          storedEntry.getIndex() == entries[index].getIndex(),
          "The stored entry's index %s is not consistent with" +
              " the received entries[%s]'s index %s",
          storedEntry.getIndex(), index, entries[index].getIndex());

      if (storedEntry.getTerm() != entries[index].getTerm()) {
        // we should truncate from the storedEntry's index
        truncateIndex = storedEntry.getIndex();
        break;
      }
    }
    if (truncateIndex != -1) {
      // truncate from truncateIndex
      truncate(truncateIndex);
    }
    // append from entries[index]
    for (int i = index; i < entries.length; i++) {
      appendEntry(entries[i]);
    }
  }

  @Override
  public void logSync(long index) throws InterruptedException {
    fileLogWorker.waitForFlush(index);
  }

  @Override
  public long getLatestFlushedIndex() {
    return fileLogWorker.getFlushedIndex();
  }

  @Override
  public void close() throws IOException {
    fileLogWorker.close();
    storage.close();
  }
}
