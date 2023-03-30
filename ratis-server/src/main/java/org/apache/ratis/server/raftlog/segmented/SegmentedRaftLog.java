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
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLogBase;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.storage.RaftStorageMetadata;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.raftlog.segmented.LogSegment.LogRecord;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache.TruncateIndices;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import com.codahale.metrics.Timer;

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
public class SegmentedRaftLog extends RaftLogBase {
  /**
   * I/O task definitions.
   */
  abstract static class Task {
    private final CompletableFuture<Long> future = new CompletableFuture<>();
    private Timer.Context queueTimerContext;

    CompletableFuture<Long> getFuture() {
      return future;
    }

    void done() {
      completeFuture();
    }

    final void completeFuture() {
      final boolean completed = future.complete(getEndIndex());
      Preconditions.assertTrue(completed,
          () -> this + " is already " + StringUtils.completableFuture2String(future, false));
    }

    void failed(IOException e) {
      this.getFuture().completeExceptionally(e);
    }

    abstract void execute() throws IOException;

    abstract long getEndIndex();

    void startTimerOnEnqueue(Timer queueTimer) {
      queueTimerContext = queueTimer.time();
    }

    void stopTimerOnDequeue() {
      if (queueTimerContext != null) {
        queueTimerContext.stop();
      }
    }

    int getSerializedSize() {
      return 0;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" + getEndIndex();
    }
  }

  /** The server methods used in {@link SegmentedRaftLog}. */
  interface ServerLogMethods {
    ServerLogMethods DUMMY = new ServerLogMethods() {};

    default long[] getFollowerNextIndices() {
      return null;
    }

    default long getLastAppliedIndex() {
      return INVALID_LOG_INDEX;
    }

    /** Notify the server that a log entry is being truncated. */
    default void notifyTruncatedLogEntry(TermIndex ti) {
    }
  }

  /**
   * When the server is null, return the dummy instance of {@link ServerLogMethods}.
   * Otherwise, the server is non-null, return the implementation using the given server.
   */
  private ServerLogMethods newServerLogMethods(RaftServer.Division impl,
      Consumer<LogEntryProto> notifyTruncatedLogEntry) {
    if (impl == null) {
      return ServerLogMethods.DUMMY;
    }

    return new ServerLogMethods() {
      @Override
      public long[] getFollowerNextIndices() {
        return impl.getInfo().getFollowerNextIndices();
      }

      @Override
      public long getLastAppliedIndex() {
        return impl.getInfo().getLastAppliedIndex();
      }

      @Override
      public void notifyTruncatedLogEntry(TermIndex ti) {
        try {
          final LogEntryProto entry = get(ti.getIndex());
          notifyTruncatedLogEntry.accept(entry);
        } catch (RaftLogIOException e) {
          LOG.error("{}: Failed to read log {}", getName(), ti, e);
        }
      }
    };
  }

  private final ServerLogMethods server;
  private final RaftStorage storage;
  private final StateMachine stateMachine;
  private final SegmentedRaftLogCache cache;
  private final SegmentedRaftLogWorker fileLogWorker;
  private final long segmentMaxSize;
  private final boolean stateMachineCachingEnabled;
  private final SegmentedRaftLogMetrics metrics;

  @SuppressWarnings("parameternumber")
  public SegmentedRaftLog(RaftGroupMemberId memberId, RaftServer.Division server,
      StateMachine stateMachine, Consumer<LogEntryProto> notifyTruncatedLogEntry, Runnable submitUpdateCommitEvent,
      RaftStorage storage, LongSupplier snapshotIndexSupplier, RaftProperties properties) {
    super(memberId, snapshotIndexSupplier, properties);
    this.metrics = new SegmentedRaftLogMetrics(memberId);

    this.server = newServerLogMethods(server, notifyTruncatedLogEntry);
    this.storage = storage;
    this.stateMachine = stateMachine;
    segmentMaxSize = RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.cache = new SegmentedRaftLogCache(memberId, storage, properties, getRaftLogMetrics());
    this.fileLogWorker = new SegmentedRaftLogWorker(memberId, stateMachine,
        submitUpdateCommitEvent, server, storage, properties, getRaftLogMetrics());
    stateMachineCachingEnabled = RaftServerConfigKeys.Log.StateMachineData.cachingEnabled(properties);
  }

  @Override
  public SegmentedRaftLogMetrics getRaftLogMetrics() {
    return metrics;
  }

  @Override
  protected void openImpl(long lastIndexInSnapshot, Consumer<LogEntryProto> consumer) throws IOException {
    loadLogSegments(lastIndexInSnapshot, consumer);
    final File openSegmentFile = Optional.ofNullable(cache.getOpenSegment()).map(LogSegment::getFile).orElse(null);
    fileLogWorker.start(Math.max(cache.getEndIndex(), lastIndexInSnapshot),
        Math.min(cache.getLastIndexInClosedSegments(), lastIndexInSnapshot),
        openSegmentFile);
  }

  @Override
  public long getStartIndex() {
    return cache.getStartIndex();
  }

  private void loadLogSegments(long lastIndexInSnapshot,
      Consumer<LogEntryProto> logConsumer) throws IOException {
    try(AutoCloseableLock writeLock = writeLock()) {
      final List<LogSegmentPath> paths = LogSegmentPath.getLogSegmentPaths(storage);
      int i = 0;
      for (LogSegmentPath pi : paths) {
        // During the initial loading, we can only confirm the committed
        // index based on the snapshot. This means if a log segment is not kept
        // in cache after the initial loading, later we have to load its content
        // again for updating the state machine.
        // TODO we should let raft peer persist its committed index periodically
        // so that during the initial loading we can apply part of the log
        // entries to the state machine
        boolean keepEntryInCache = (paths.size() - i++) <= cache.getMaxCachedSegments();
        final Timer.Context loadSegmentContext = getRaftLogMetrics().getRaftLogLoadSegmentTimer().time();
        cache.loadSegment(pi, keepEntryInCache, logConsumer);
        loadSegmentContext.stop();
      }

      // if the largest index is smaller than the last index in snapshot, we do
      // not load the log to avoid holes between log segments. This may happen
      // when the local I/O worker is too slow to persist log (slower than
      // committing the log and taking snapshot)
      if (!cache.isEmpty() && cache.getEndIndex() < lastIndexInSnapshot) {
        LOG.warn("End log index {} is smaller than last index in snapshot {}",
            cache.getEndIndex(), lastIndexInSnapshot);
        purgeImpl(lastIndexInSnapshot);
      }
    }
  }

  @Override
  public LogEntryProto get(long index) throws RaftLogIOException {
    checkLogState();
    final LogSegment segment;
    final LogRecord record;
    try (AutoCloseableLock readLock = readLock()) {
      segment = cache.getSegment(index);
      if (segment == null) {
        return null;
      }
      record = segment.getLogRecord(index);
      if (record == null) {
        return null;
      }
      final LogEntryProto entry = segment.getEntryFromCache(record.getTermIndex());
      if (entry != null) {
        getRaftLogMetrics().onRaftLogCacheHit();
        return entry;
      }
    }

    // the entry is not in the segment's cache. Load the cache without holding the lock.
    getRaftLogMetrics().onRaftLogCacheMiss();
    checkAndEvictCache();
    return segment.loadCache(record);
  }

  @Override
  public EntryWithData getEntryWithData(long index) throws RaftLogIOException {
    final LogEntryProto entry = get(index);
    if (entry == null) {
      throw new RaftLogIOException("Log entry not found: index = " + index);
    }
    if (!LogProtoUtils.isStateMachineDataEmpty(entry)) {
      return newEntryWithData(entry, null);
    }

    try {
      CompletableFuture<ByteString> future = null;
      if (stateMachine != null) {
        future = stateMachine.data().read(entry).exceptionally(ex -> {
          stateMachine.event().notifyLogFailed(ex, entry);
          throw new CompletionException("Failed to read state machine data for log entry " + entry, ex);
        });
      }
      return newEntryWithData(entry, future);
    } catch (Exception e) {
      final String err = getName() + ": Failed readStateMachineData for " +
          LogProtoUtils.toLogEntryString(entry);
      LOG.error(err, e);
      throw new RaftLogIOException(err, JavaUtils.unwrapCompletionException(e));
    }
  }

  private void checkAndEvictCache() {
    if (cache.shouldEvict()) {
      // TODO if the cache is hitting the maximum size and we cannot evict any
      // segment's cache, should block the new entry appending or new segment
      // allocation.
      cache.evictCache(server.getFollowerNextIndices(), fileLogWorker.getSafeCacheEvictIndex(),
          server.getLastAppliedIndex());
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
  public LogEntryHeader[] getEntries(long startIndex, long endIndex) {
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

  @Override
  protected CompletableFuture<Long> truncateImpl(long index) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      SegmentedRaftLogCache.TruncationSegments ts = cache.truncate(index);
      if (ts != null) {
        Task task = fileLogWorker.truncate(ts, index);
        return task.getFuture();
      }
    }
    return CompletableFuture.completedFuture(index);
  }


  @Override
  protected CompletableFuture<Long> purgeImpl(long index) {
    try (AutoCloseableLock writeLock = writeLock()) {
      SegmentedRaftLogCache.TruncationSegments ts = cache.purge(index);
      updateSnapshotIndexFromStateMachine();
      LOG.debug("purging segments:{}", ts);
      if (ts != null) {
        Task task = fileLogWorker.purge(ts);
        return task.getFuture();
      }
    }
    return CompletableFuture.completedFuture(index);
  }

  @Override
  protected CompletableFuture<Long> appendEntryImpl(LogEntryProto entry) {
    checkLogState();
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: appendEntry {}", getName(), LogProtoUtils.toLogEntryString(entry));
    }
    try(AutoCloseableLock writeLock = writeLock()) {
      final Timer.Context appendEntryTimerContext = getRaftLogMetrics().startAppendEntryTimer();
      validateLogEntry(entry);
      final LogSegment currentOpenSegment = cache.getOpenSegment();
      if (currentOpenSegment == null) {
        cache.addOpenSegment(entry.getIndex());
        fileLogWorker.startLogSegment(entry.getIndex());
      } else if (isSegmentFull(currentOpenSegment, entry)) {
        cache.rollOpenSegment(true);
        fileLogWorker.rollLogSegment(currentOpenSegment);
      } else if (currentOpenSegment.numOfEntries() > 0 &&
          currentOpenSegment.getLastTermIndex().getTerm() != entry.getTerm()) {
        // the term changes
        final long currentTerm = currentOpenSegment.getLastTermIndex().getTerm();
        Preconditions.assertTrue(currentTerm < entry.getTerm(),
            "open segment's term %s is larger than the new entry's term %s",
            currentTerm, entry.getTerm());
        cache.rollOpenSegment(true);
        fileLogWorker.rollLogSegment(currentOpenSegment);
      }

      //TODO(runzhiwang): If there is performance problem, start a daemon thread to checkAndEvictCache
      checkAndEvictCache();

      // If the entry has state machine data, then the entry should be inserted
      // to statemachine first and then to the cache. Not following the order
      // will leave a spurious entry in the cache.
      CompletableFuture<Long> writeFuture =
          fileLogWorker.writeLogEntry(entry).getFuture();
      if (stateMachineCachingEnabled) {
        // The stateMachineData will be cached inside the StateMachine itself.
        cache.appendEntry(LogProtoUtils.removeStateMachineData(entry),
            LogSegment.Op.WRITE_CACHE_WITH_STATE_MACHINE_CACHE);
      } else {
        cache.appendEntry(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
      }
      writeFuture.whenComplete((clientReply, exception) -> appendEntryTimerContext.stop());
      return writeFuture;
    } catch (Exception e) {
      LOG.error("{}: Failed to append {}", getName(), LogProtoUtils.toLogEntryString(entry), e);
      throw e;
    }
  }

  private boolean isSegmentFull(LogSegment segment, LogEntryProto entry) {
    if (segment.getTotalFileSize() >= segmentMaxSize) {
      return true;
    } else {
      final long entrySize = LogSegment.getEntrySize(entry, LogSegment.Op.CHECK_SEGMENT_FILE_FULL);
      // if entry size is greater than the max segment size, write it directly
      // into the current segment
      return entrySize <= segmentMaxSize &&
          segment.getTotalFileSize() + entrySize > segmentMaxSize;
    }
  }

  @Override
  public List<CompletableFuture<Long>> appendImpl(List<LogEntryProto> entries) {
    checkLogState();
    if (entries == null || entries.isEmpty()) {
      return Collections.emptyList();
    }
    try(AutoCloseableLock writeLock = writeLock()) {
      final TruncateIndices ti = cache.computeTruncateIndices(server::notifyTruncatedLogEntry, entries);
      final long truncateIndex = ti.getTruncateIndex();
      final int index = ti.getArrayIndex();
      LOG.debug("truncateIndex={}, arrayIndex={}", truncateIndex, index);

      final List<CompletableFuture<Long>> futures;
      if (truncateIndex != -1) {
        futures = new ArrayList<>(entries.size() - index + 1);
        futures.add(truncate(truncateIndex));
      } else {
        futures = new ArrayList<>(entries.size() - index);
      }
      for (int i = index; i < entries.size(); i++) {
        futures.add(appendEntry(entries.get(i)));
      }
      return futures;
    }
  }


  @Override
  public long getFlushIndex() {
    return fileLogWorker.getFlushIndex();
  }

  @Override
  public void persistMetadata(RaftStorageMetadata metadata) throws IOException {
    storage.getMetadataFile().persist(metadata);
  }

  @Override
  public RaftStorageMetadata loadMetadata() throws IOException {
    return storage.getMetadataFile().getMetadata();
  }

  @Override
  public CompletableFuture<Long> onSnapshotInstalled(long lastSnapshotIndex) {
    updateSnapshotIndex(lastSnapshotIndex);
    fileLogWorker.syncWithSnapshot(lastSnapshotIndex);
    // TODO purge normal/tmp/corrupt snapshot files
    // if the last index in snapshot is larger than the index of the last
    // log entry, we should delete all the log entries and their cache to avoid
    // gaps between log segments.

    // Close open log segment if entries are already included in snapshot
    LogSegment openSegment = cache.getOpenSegment();
    if (openSegment != null && openSegment.hasEntries()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("syncWithSnapshot : Found open segment {}, with end index {},"
                + " snapshotIndex {}", openSegment, openSegment.getEndIndex(),
            lastSnapshotIndex);
      }
      if (openSegment.getEndIndex() <= lastSnapshotIndex) {
        fileLogWorker.closeLogSegment(openSegment);
        cache.rollOpenSegment(false);
      }
    }
    return purgeImpl(lastSnapshotIndex);
  }

  @Override
  public void close() throws IOException {
    try(AutoCloseableLock writeLock = writeLock()) {
      super.close();
      cache.close();
    }
    fileLogWorker.close();
    storage.close();
    getRaftLogMetrics().unregister();
  }

  SegmentedRaftLogCache getRaftLogCache() {
    return cache;
  }

  @Override
  public String toLogEntryString(LogEntryProto logEntry) {
    return LogProtoUtils.toLogEntryString(logEntry, stateMachine::toStateMachineLogEntryString);
  }
}
