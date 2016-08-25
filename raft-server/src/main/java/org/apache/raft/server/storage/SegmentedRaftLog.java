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
package org.apache.raft.server.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.Charsets;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.server.ConfigurationManager;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.storage.RaftStorageDirectory.LogPathAndIndex;
import org.apache.raft.util.CodeInjectionForTesting;

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
 *
 * There should not be any gap between segments. The first segment may not start
 * from index 0 since there may be snapshots as log compaction. The last index
 * in segments should be no smaller than the last index of snapshot, otherwise
 * we may have hole when append further log.
 */
public class SegmentedRaftLog extends RaftLog {
  static final String HEADER_STR = "RAFTLOG1";
  static final byte[] HEADER_BYTES = HEADER_STR.getBytes(Charsets.UTF_8);

  /**
   * I/O task definitions.
   */
  static abstract class Task {
    private boolean done = false;

    synchronized void done() {
      done = true;
      notifyAll();
    }

    synchronized void waitForDone() throws InterruptedException {
      while (!done) {
        wait();
      }
    }

    abstract void execute() throws IOException;

    abstract long getEndIndex();

    @Override
    public String toString() {
      return getClass().getSimpleName() + "-" + getEndIndex();
    }
  }
  private static final ThreadLocal<Task> myTask = new ThreadLocal<>();

  private final RaftStorage storage;
  private final RaftLogCache cache;
  private final RaftLogWorker fileLogWorker;

  public SegmentedRaftLog(String selfId, RaftServer server, RaftStorage storage,
      long lastIndexInSnapshot) throws IOException {
    super(selfId);
    this.storage = storage;
    cache = new RaftLogCache();
    fileLogWorker = new RaftLogWorker(server, storage);
    lastCommitted.set(lastIndexInSnapshot);
  }

  @Override
  public void open(ConfigurationManager confManager, long lastIndexInSnapshot)
      throws IOException {
    loadLogSegments(confManager, lastIndexInSnapshot);
    fileLogWorker.start(Math.max(cache.getEndIndex(), lastIndexInSnapshot));
    super.open(confManager, lastIndexInSnapshot);
  }

  @Override
  public long getStartIndex() {
    return cache.getStartIndex();
  }

  private void loadLogSegments(ConfigurationManager confManager,
      long lastIndexInSnapshot) throws IOException {
    writeLock();
    try {
      List<LogPathAndIndex> paths = storage.getStorageDir().getLogSegmentFiles();
      for (LogPathAndIndex pi : paths) {
        LogSegment logSegment = parseLogSegment(pi, confManager);
        cache.addSegment(logSegment);
      }

      // if the largest index is smaller than the last index in snapshot, we do
      // not load the log to avoid holes between log segments. This may happen
      // when the local I/O worker is too slow to persist log (slower than
      // committing the log and taking snapshot)
      if (!cache.isEmpty() && cache.getEndIndex() < lastIndexInSnapshot) {
        LOG.warn("End log index {} is smaller than last index in snapshot {}",
            cache.getEndIndex(), lastIndexInSnapshot);
        cache.clear();
        // TODO purge all segment files
      }
    } finally {
      writeUnlock();
    }
  }

  private LogSegment parseLogSegment(LogPathAndIndex pi,
      ConfigurationManager confManager) throws IOException {
    final boolean isOpen = pi.endIndex == RaftServerConstants.INVALID_LOG_INDEX;
    return LogSegment.loadSegment(pi.path.toFile(), pi.startIndex, pi.endIndex,
        isOpen, confManager);
  }

  @Override
  public LogEntryProto get(long index) {
    checkLogState();
    readLock();
    try {
      return cache.getEntry(index);
    } finally {
      readUnlock();
    }
  }

  @Override
  public LogEntryProto[] getEntries(long startIndex, long endIndex) {
    checkLogState();
    readLock();
    try {
      return cache.getEntries(startIndex, endIndex);
    } finally {
      readUnlock();
    }
  }

  @Override
  public LogEntryProto getLastEntry() {
    checkLogState();
    readLock();
    try {
      return cache.getLastEntry();
    } finally {
      readUnlock();
    }
  }

  /**
   * The method, along with {@link #appendEntry} and
   * {@link #append(LogEntryProto...)} need protection of RaftServer's lock.
   */
  @Override
  void truncate(long index) {
    checkLogState();
    writeLock();
    try {
      RaftLogCache.TruncationSegments ts = cache.truncate(index);
      if (ts != null) {
        Task task = fileLogWorker.truncate(ts);
        myTask.set(task);
      }
    } finally {
      writeUnlock();
    }
  }

  @Override
  void appendEntry(LogEntryProto entry) {
    checkLogState();
    writeLock();
    try {
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
        final long currentTerm = currentOpenSegment.getLastRecord().entry
            .getTerm();
        Preconditions.checkState(currentTerm < entry.getTerm(),
            "open segment's term %s is larger than the new entry's term %s",
            currentTerm, entry.getTerm());
        cache.rollOpenSegment(true);
        fileLogWorker.rollLogSegment(currentOpenSegment);
      }

      cache.appendEntry(entry);
      myTask.set(fileLogWorker.writeLogEntry(entry));
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void append(LogEntryProto... entries) {
    checkLogState();
    writeLock();
    try {
      if (entries == null || entries.length == 0) {
        return;
      }
      Iterator<LogEntryProto> iter = cache.iterator(entries[0].getIndex());
      int index = 0;
      long truncateIndex = -1;
      for (; iter.hasNext() && index < entries.length; index++) {
        LogEntryProto storedEntry = iter.next();
        Preconditions.checkState(
            storedEntry.getIndex() == entries[index].getIndex(),
            "The stored entry's index %s is not consistent with" +
                " the received entries[%s]'s index %s", storedEntry.getIndex(),
            index, entries[index].getIndex());

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
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void logSync() throws InterruptedException {
    CodeInjectionForTesting.execute(LOG_SYNC, getSelfId(), null);
    final Task task = myTask.get();
    if (task != null) {
      task.waitForDone();
    }
  }

  @Override
  public long getLatestFlushedIndex() {
    return fileLogWorker.getFlushedIndex();
  }

  /**
   * {@inheritDoc}
   *
   * This operation is protected by the RaftServer's lock
   */
  @Override
  public void writeMetadata(long term, String votedFor) throws IOException {
    storage.getMetaFile().set(term, votedFor);
  }

  @Override
  public Metadata loadMetadata() throws IOException {
    return new Metadata(storage.getMetaFile().getVotedFor(),
        storage.getMetaFile().getTerm());
  }

  @Override
  public void syncWithSnapshot(long lastSnapshotIndex) {
    fileLogWorker.syncWithSnapshot(lastSnapshotIndex);
    // TODO purge log files and normal/tmp/corrupt snapshot files
    // if the last index in snapshot is larger than the index of the last
    // log entry, we should delete all the log entries and their cache to avoid
    // gaps between log segments.
  }

  @Override
  public void close() throws IOException {
    super.close();
    fileLogWorker.close();
    storage.close();
  }

  @VisibleForTesting
  RaftLogCache getRaftLogCache() {
    return cache;
  }
}
