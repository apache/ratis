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
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.metrics.RaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.raftlog.segmented.LogSegment.LogRecord;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache.TruncateIndices;
import org.apache.ratis.server.storage.RaftStorageDirectory.LogPathAndIndex;
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
import java.util.function.Consumer;

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
public class SegmentedRaftLog extends RaftLog {
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
      return getClass().getSimpleName() + ":" + getEndIndex();
    }
  }

  /** The methods defined in {@link RaftServerImpl} which are used in {@link SegmentedRaftLog}. */
  interface ServerLogMethods {
    ServerLogMethods DUMMY = new ServerLogMethods() {};

    default boolean shouldEvictCache() {
      return false;
    }

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
  private ServerLogMethods newServerLogMethods(RaftServerImpl impl) {
    if (impl == null) {
      return ServerLogMethods.DUMMY;
    }

    return new ServerLogMethods() {
      @Override
      public boolean shouldEvictCache() {
        return cache.shouldEvict();
      }

      @Override
      public long[] getFollowerNextIndices() {
        return impl.getFollowerNextIndices();
      }

      @Override
      public long getLastAppliedIndex() {
        return impl.getState().getLastAppliedIndex();
      }

      @Override
      public void notifyTruncatedLogEntry(TermIndex ti) {
        try {
          final LogEntryProto entry = get(ti.getIndex());
          impl.notifyTruncatedLogEntry(entry);
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
  private final RaftLogMetrics raftLogMetrics;

  public SegmentedRaftLog(RaftGroupMemberId memberId, RaftServerImpl server,
      RaftStorage storage, long lastIndexInSnapshot, RaftProperties properties) {
    this(memberId, server, server != null? server.getStateMachine(): null,
        server != null? server::submitUpdateCommitEvent: null,
        storage, lastIndexInSnapshot, properties);
  }

  SegmentedRaftLog(RaftGroupMemberId memberId, RaftServerImpl server,
      StateMachine stateMachine, Runnable submitUpdateCommitEvent,
      RaftStorage storage, long lastIndexInSnapshot, RaftProperties properties) {
    super(memberId, lastIndexInSnapshot, properties);
    this.server = newServerLogMethods(server);
    this.storage = storage;
    this.stateMachine = stateMachine;
    segmentMaxSize = RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.raftLogMetrics = new RaftLogMetrics(memberId.getPeerId().toString());
    this.cache = new SegmentedRaftLogCache(memberId, storage, properties, raftLogMetrics);
    this.fileLogWorker = new SegmentedRaftLogWorker(memberId, stateMachine,
        submitUpdateCommitEvent, server, storage, properties, raftLogMetrics);
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
        final Timer.Context loadSegmentContext = raftLogMetrics.getRaftLogLoadSegmentTimer().time();
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
        cache.clear();
        // TODO purge all segment files
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
        raftLogMetrics.onRaftLogCacheHit();
        return entry;
      }
    }

    // the entry is not in the segment's cache. Load the cache without holding the lock.
    raftLogMetrics.onRaftLogCacheMiss();
    checkAndEvictCache();
    return segment.loadCache(record);
  }

  @Override
  public EntryWithData getEntryWithData(long index) throws RaftLogIOException {
    final LogEntryProto entry = get(index);
    if (entry == null) {
      throw new RaftLogIOException("Log entry not found: index = " + index);
    }
    if (!ServerProtoUtils.shouldReadStateMachineData(entry)) {
      return new EntryWithData(entry, null);
    }

    try {
      CompletableFuture<ByteString> future = null;
      if (stateMachine != null) {
        future = stateMachine.readStateMachineData(entry).exceptionally(ex -> {
          stateMachine.notifyLogFailed(ex, entry);
          return null;
        });
      }
      return new EntryWithData(entry, future);
    } catch (Throwable e) {
      final String err = getName() + ": Failed readStateMachineData for " +
          ServerProtoUtils.toLogEntryString(entry);
      LOG.error(err, e);
      throw new RaftLogIOException(err, JavaUtils.unwrapCompletionException(e));
    }
  }

  private void checkAndEvictCache() {
    if (server.shouldEvictCache()) {
      // TODO if the cache is hitting the maximum size and we cannot evict any
      // segment's cache, should block the new entry appending or new segment
      // allocation.
      cache.evictCache(server.getFollowerNextIndices(), fileLogWorker.getFlushIndex(), server.getLastAppliedIndex());
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
    final Timer.Context context = raftLogMetrics.getRaftLogAppendEntryTimer().time();
    checkLogState();
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: appendEntry {}", getName(), ServerProtoUtils.toLogEntryString(entry));
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
      LOG.error("{}: Failed to append {}", getName(), ServerProtoUtils.toLogEntryString(entry), throwable);
      throw throwable;
    } finally {
      context.stop();
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

  @Override
  public List<CompletableFuture<Long>> appendImpl(LogEntryProto... entries) {
    checkLogState();
    if (entries == null || entries.length == 0) {
      return Collections.emptyList();
    }
    try(AutoCloseableLock writeLock = writeLock()) {
      final TruncateIndices ti = cache.computeTruncateIndices(server::notifyTruncatedLogEntry, entries);
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
  public long getFlushIndex() {
    return fileLogWorker.getFlushIndex();
  }

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
    raftLogMetrics.unregister();
  }

  SegmentedRaftLogCache getRaftLogCache() {
    return cache;
  }

  @Override
  public String toString() {
    try(AutoCloseableLock readLock = readLock()) {
      if (isOpened()) {
        return super.toString() + ",f" + getFlushIndex()
            + ",i" + Optional.ofNullable(getLastEntryTermIndex()).map(TermIndex::getIndex).orElse(0L);
      } else {
        return super.toString();
      }
    }
  }

  @Override
  public String toLogEntryString(LogEntryProto logEntry) {
    return ServerProtoUtils.toLogEntryString(logEntry, stateMachine);
  }
}
