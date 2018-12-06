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
package org.apache.ratis.server.storage;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.LogSegment.LogRecord;
import org.apache.ratis.server.storage.LogSegment.LogRecordWithEntry;
import org.apache.ratis.server.storage.RaftLogCache.TruncateIndices;
import org.apache.ratis.server.storage.RaftStorageDirectory.LogPathAndIndex;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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
  /**
   * I/O task definitions.
   */
  static abstract class Task {
    private final CompletableFuture<Long> future = new CompletableFuture<>();

    CompletableFuture<Long> getFuture() {
      return future;
    }

    void done() {
      future.complete(getEndIndex());
    }


    abstract void execute() throws IOException;

    abstract long getEndIndex();

    @Override
    public String toString() {
      return getClass().getSimpleName() + ":" + getEndIndex();
    }
  }

  private final Optional<RaftServerImpl> server;
  private final RaftStorage storage;
  private final RaftLogCache cache;
  private final RaftLogWorker fileLogWorker;
  private final long segmentMaxSize;
  private final boolean stateMachineCachingEnabled;

  public SegmentedRaftLog(RaftPeerId selfId, RaftServerImpl server,
      RaftStorage storage, long lastIndexInSnapshot, RaftProperties properties) {
    this(selfId, server, server != null? server.getStateMachine(): null,
        server != null? server::submitUpdateCommitEvent: null,
        storage, lastIndexInSnapshot, properties);
  }

  SegmentedRaftLog(RaftPeerId selfId, RaftServerImpl server,
      StateMachine stateMachine, Runnable submitUpdateCommitEvent,
      RaftStorage storage, long lastIndexInSnapshot, RaftProperties properties) {
    super(selfId, lastIndexInSnapshot, RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties).getSizeInt());
    this.server = Optional.ofNullable(server);
    this.storage = storage;
    segmentMaxSize = RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    cache = new RaftLogCache(selfId, storage, properties);
    this.fileLogWorker = new RaftLogWorker(selfId, stateMachine, submitUpdateCommitEvent, storage, properties);
    stateMachineCachingEnabled = RaftServerConfigKeys.Log.StateMachineData.cachingEnabled(properties);
  }

  @Override
  protected void openImpl(long lastIndexInSnapshot, Consumer<LogEntryProto> consumer) throws IOException {
    loadLogSegments(lastIndexInSnapshot, consumer);
    File openSegmentFile = null;
    LogSegment openSegment = cache.getOpenSegment();
    if (openSegment != null) {
      openSegmentFile = storage.getStorageDir()
          .getOpenLogFile(openSegment.getStartIndex());
    }
    fileLogWorker.start(Math.max(cache.getEndIndex(), lastIndexInSnapshot),
        openSegmentFile);
  }

  @Override
  public long getStartIndex() {
    return cache.getStartIndex();
  }

  private void loadLogSegments(long lastIndexInSnapshot,
      Consumer<LogEntryProto> logConsumer) throws IOException {
    try(AutoCloseableLock writeLock = writeLock()) {
      List<LogPathAndIndex> paths = storage.getStorageDir().getLogSegmentFiles();
      int i = 0;
      for (LogPathAndIndex pi : paths) {
        // During the initial loading, we can only confirm the committed
        // index based on the snapshot. This means if a log segment is not kept
        // in cache after the initial loading, later we have to load its content
        // again for updating the state machine.
        // TODO we should let raft peer persist its committed index periodically
        // so that during the initial loading we can apply part of the log
        // entries to the state machine
        boolean keepEntryInCache = (paths.size() - i++) <= cache.getMaxCachedSegments();
        cache.loadSegment(pi, keepEntryInCache, logConsumer);
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
    }
  }

  @Override
  public LogEntryProto get(long index) throws RaftLogIOException {
    checkLogState();
    LogSegment segment;
    LogRecordWithEntry recordAndEntry;
    try (AutoCloseableLock readLock = readLock()) {
      segment = cache.getSegment(index);
      if (segment == null) {
        return null;
      }
      recordAndEntry = segment.getEntryWithoutLoading(index);
      if (recordAndEntry == null) {
        return null;
      }
      if (recordAndEntry.hasEntry()) {
        return recordAndEntry.getEntry();
      }
    }

    // the entry is not in the segment's cache. Load the cache without holding
    // RaftLog's lock.
    checkAndEvictCache();
    return segment.loadCache(recordAndEntry.getRecord());
  }

  @Override
  public EntryWithData getEntryWithData(long index) throws RaftLogIOException {
    final LogEntryProto entry = get(index);
    if (!ServerProtoUtils.shouldReadStateMachineData(entry)) {
      return new EntryWithData(entry, null);
    }

    try {
      return new EntryWithData(entry, server.map(s -> s.getStateMachine().readStateMachineData(entry)).orElse(null));
    } catch (Throwable e) {
      final String err = getSelfId() + ": Failed readStateMachineData for " +
          ServerProtoUtils.toLogEntryString(entry);
      LOG.error(err, e);
      throw new RaftLogIOException(err, JavaUtils.unwrapCompletionException(e));
    }
  }

  private void checkAndEvictCache() {
    if (server.isPresent() && cache.shouldEvict()) {
      // TODO if the cache is hitting the maximum size and we cannot evict any
      // segment's cache, should block the new entry appending or new segment
      // allocation.
      final RaftServerImpl s = server.get();
      cache.evictCache(s.getFollowerNextIndices(), fileLogWorker.getFlushedIndex(), s.getState().getLastAppliedIndex());
    }
  }

  @Override
  public TermIndex getTermIndex(long index) {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      LogRecord record = cache.getLogRecord(index);
      return record != null ? record.getTermIndex() : null;
    }
  }

  @Override
  public TermIndex[] getEntries(long startIndex, long endIndex) {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      return cache.getTermIndices(startIndex, endIndex);
    }
  }

  @Override
  public TermIndex getLastEntryTermIndex() {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      return cache.getLastTermIndex();
    }
  }

  /**
   * The method, along with {@link #appendEntry} and
   * {@link #append(LogEntryProto...)} need protection of RaftServer's lock.
   */
  @Override
  CompletableFuture<Long> truncateImpl(long index) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      RaftLogCache.TruncationSegments ts = cache.truncate(index);
      if (ts != null) {
        Task task = fileLogWorker.truncate(ts);
        return task.getFuture();
      }
    }
    return CompletableFuture.completedFuture(index);
  }

  @Override
  CompletableFuture<Long> appendEntryImpl(LogEntryProto entry) {
    checkLogState();
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: appendEntry {}", getSelfId(),
          ServerProtoUtils.toLogEntryString(entry));
    }
    try(AutoCloseableLock writeLock = writeLock()) {
      validateLogEntry(entry);
      final LogSegment currentOpenSegment = cache.getOpenSegment();
      if (currentOpenSegment == null) {
        cache.addOpenSegment(entry.getIndex());
        fileLogWorker.startLogSegment(entry.getIndex());
      } else if (isSegmentFull(currentOpenSegment, entry)) {
        cache.rollOpenSegment(true);
        fileLogWorker.rollLogSegment(currentOpenSegment);
        checkAndEvictCache();
      } else if (currentOpenSegment.numOfEntries() > 0 &&
          currentOpenSegment.getLastTermIndex().getTerm() != entry.getTerm()) {
        // the term changes
        final long currentTerm = currentOpenSegment.getLastTermIndex().getTerm();
        Preconditions.assertTrue(currentTerm < entry.getTerm(),
            "open segment's term %s is larger than the new entry's term %s",
            currentTerm, entry.getTerm());
        cache.rollOpenSegment(true);
        fileLogWorker.rollLogSegment(currentOpenSegment);
        checkAndEvictCache();
      }

      // If the entry has state machine data, then the entry should be inserted
      // to statemachine first and then to the cache. Not following the order
      // will leave a spurious entry in the cache.
      CompletableFuture<Long> writeFuture =
          fileLogWorker.writeLogEntry(entry).getFuture();
      if (stateMachineCachingEnabled) {
        // The stateMachineData will be cached inside the StateMachine itself.
        cache.appendEntry(ServerProtoUtils.removeStateMachineData(entry));
      } else {
        cache.appendEntry(entry);
      }
      return writeFuture;
    } catch (Throwable throwable) {
      LOG.error(getSelfId() + ": Failed to append " + ServerProtoUtils.toLogEntryString(entry), throwable);
      throw throwable;
    }
  }

  private boolean isSegmentFull(LogSegment segment, LogEntryProto entry) {
    if (segment.getTotalSize() >= segmentMaxSize) {
      return true;
    } else {
      final long entrySize = LogSegment.getEntrySize(entry);
      // if entry size is greater than the max segment size, write it directly
      // into the current segment
      return entrySize <= segmentMaxSize &&
          segment.getTotalSize() + entrySize > segmentMaxSize;
    }
  }

  private void failClientRequest(TermIndex ti) {
    if (!server.isPresent()) {
      return;
    }
    try {
      final LogEntryProto entry = get(ti.getIndex());
      server.get().failClientRequest(entry);
    } catch(RaftLogIOException e) {
      LOG.error(getName() + ": Failed to read log " + ti, e);
    }
  }

  @Override
  public List<CompletableFuture<Long>> appendImpl(LogEntryProto... entries) {
    checkLogState();
    if (entries == null || entries.length == 0) {
      return Collections.emptyList();
    }
    try(AutoCloseableLock writeLock = writeLock()) {
      final TruncateIndices ti = cache.computeTruncateIndices(this::failClientRequest, entries);
      final long truncateIndex = ti.getTruncateIndex();
      final int index = ti.getArrayIndex();
      LOG.debug("truncateIndex={}, arrayIndex={}", truncateIndex, index);

      final List<CompletableFuture<Long>> futures;
      if (truncateIndex != -1) {
        futures = new ArrayList<>(entries.length - index + 1);
        futures.add(truncate(truncateIndex));
      } else {
        futures = new ArrayList<>(entries.length - index);
      }
      for (int i = index; i < entries.length; i++) {
        futures.add(appendEntry(entries[i]));
      }
      return futures;
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
  public void writeMetadata(long term, RaftPeerId votedFor) throws IOException {
    storage.getMetaFile().set(term, votedFor != null ? votedFor.toString() : null);
  }

  @Override
  public Metadata loadMetadata() throws IOException {
    return new Metadata(
        RaftPeerId.getRaftPeerId(storage.getMetaFile().getVotedFor()),
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
  public boolean isConfigEntry(TermIndex ti) {
    return cache.isConfigEntry(ti);
  }

  @Override
  public void close() throws IOException {
    try(AutoCloseableLock writeLock = writeLock()) {
      super.close();
      cache.clear();
    }
    fileLogWorker.close();
    storage.close();
  }

  RaftLogCache getRaftLogCache() {
    return cache;
  }
}
