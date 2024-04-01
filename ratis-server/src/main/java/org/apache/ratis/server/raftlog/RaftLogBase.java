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
package org.apache.ratis.server.raftlog;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.OpenCloseState;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Base class of RaftLog. Currently we provide two types of RaftLog
 * implementation:
 * 1. MemoryRaftLog: all the log entries are stored in memory. This is only used
 *    for testing.
 * 2. Segmented RaftLog: the log entries are persisted on disk, and are stored
 *    in segments.
 */
public abstract class RaftLogBase implements RaftLog {
  private final Consumer<Object> infoIndexChange = s -> LOG.info("{}: {}", getName(), s);
  private final Consumer<Object> traceIndexChange = s -> LOG.trace("{}: {}", getName(), s);

  /** The least valid log index, i.e. the index used when writing to an empty log. */
  public static final long LEAST_VALID_LOG_INDEX = 0L;
  public static final long INVALID_LOG_INDEX = LEAST_VALID_LOG_INDEX - 1;

  private final String name;
  /**
   * The largest committed index. Note the last committed log may be included
   * in the latest snapshot file.
   */
  private final RaftLogIndex commitIndex;
  /** The last log index in snapshot */
  private final RaftLogIndex snapshotIndex;
  private final RaftLogIndex purgeIndex;
  private final int purgeGap;

  private final RaftGroupMemberId memberId;
  private final int maxBufferSize;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final Runner runner = new Runner(this::getName);
  private final OpenCloseState state;
  private final LongSupplier getSnapshotIndexFromStateMachine;
  private final TimeDuration stateMachineDataReadTimeout;
  private final long purgePreservation;

  private final AtomicReference<LogEntryProto> lastMetadataEntry = new AtomicReference<>();

  protected RaftLogBase(RaftGroupMemberId memberId,
                    LongSupplier getSnapshotIndexFromStateMachine,
                    RaftProperties properties) {
    this.name = memberId + "-" + JavaUtils.getClassSimpleName(getClass());
    this.memberId = memberId;
    long index = getSnapshotIndexFromStateMachine.getAsLong();
    this.commitIndex = new RaftLogIndex("commitIndex", index);
    this.snapshotIndex = new RaftLogIndex("snapshotIndex", index);
    this.purgeIndex = new RaftLogIndex("purgeIndex", LEAST_VALID_LOG_INDEX - 1);
    this.purgeGap = RaftServerConfigKeys.Log.purgeGap(properties);
    this.maxBufferSize = RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties).getSizeInt();
    this.state = new OpenCloseState(getName());
    this.getSnapshotIndexFromStateMachine = getSnapshotIndexFromStateMachine;
    this.stateMachineDataReadTimeout = RaftServerConfigKeys.Log.StateMachineData.readTimeout(properties);
    this.purgePreservation = RaftServerConfigKeys.Log.purgePreservationLogNum(properties);
  }

  @Override
  public long getLastCommittedIndex() {
    return commitIndex.get();
  }

  @Override
  public long getSnapshotIndex() {
    return snapshotIndex.get();
  }

  public void checkLogState() {
    state.assertOpen();
  }

  /** Is this log already opened? */
  public boolean isOpened() {
    return state.isOpened();
  }

  @Override
  public boolean updateCommitIndex(long majorityIndex, long currentTerm, boolean isLeader) {
    try(AutoCloseableLock writeLock = writeLock()) {
      final long oldCommittedIndex = getLastCommittedIndex();
      final long newCommitIndex = Math.min(majorityIndex, getFlushIndex());
      if (oldCommittedIndex < newCommitIndex) {
        if (!isLeader) {
          commitIndex.updateIncreasingly(newCommitIndex, traceIndexChange);
          return true;
        }

        // Only update last committed index for current term. See §5.4.2 in paper for details.
        final TermIndex entry = getTermIndex(newCommitIndex);
        if (entry != null && entry.getTerm() == currentTerm) {
          commitIndex.updateIncreasingly(newCommitIndex, traceIndexChange);
          return true;
        }
      }
    }
    return false;
  }

  protected void updateSnapshotIndexFromStateMachine() {
      updateSnapshotIndex(getSnapshotIndexFromStateMachine.getAsLong());
  }

  @Override
  public void updateSnapshotIndex(long newSnapshotIndex) {
    try(AutoCloseableLock writeLock = writeLock()) {
      final long oldSnapshotIndex = getSnapshotIndex();
      if (oldSnapshotIndex < newSnapshotIndex) {
        snapshotIndex.updateIncreasingly(newSnapshotIndex, infoIndexChange);
      }
      final long oldCommitIndex = getLastCommittedIndex();
      if (oldCommitIndex < newSnapshotIndex) {
        commitIndex.updateIncreasingly(newSnapshotIndex, traceIndexChange);
      }
    }
  }

  @Override
  public final long append(long term, TransactionContext transaction) throws StateMachineException {
    return runner.runSequentially(() -> appendImpl(term, transaction));
  }

  private long appendImpl(long term, TransactionContext operation) throws StateMachineException {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      final long nextIndex = getNextIndex();

      // This is called here to guarantee strict serialization of callback executions in case
      // the SM wants to attach a logic depending on ordered execution in the log commit order.
      try {
        operation = operation.preAppendTransaction();
      } catch (StateMachineException e) {
        throw e;
      } catch (IOException e) {
        throw new StateMachineException(memberId, e);
      }

      // build the log entry after calling the StateMachine
      final LogEntryProto e = operation.initLogEntry(term, nextIndex);

      int entrySize = e.getSerializedSize();
      if (entrySize > maxBufferSize) {
        throw new StateMachineException(memberId, new RaftLogIOException(
            "Log entry size " + entrySize + " exceeds the max buffer limit of " + maxBufferSize));
      }

      appendEntry(operation.wrap(e), operation).whenComplete((returned, t) -> {
        if (t != null) {
          LOG.error(name + ": Failed to write log entry " + LogProtoUtils.toLogEntryString(e), t);
        } else if (returned != nextIndex) {
          LOG.error("{}: Indices mismatched: returned index={} but nextIndex={} for log entry {}",
              name, returned, nextIndex, LogProtoUtils.toLogEntryString(e));
        } else {
          return; // no error
        }

        try {
          close(); // close due to error
        } catch (IOException ioe) {
          LOG.error("Failed to close " + name, ioe);
        }
      });
      return nextIndex;
    }
  }

  @Override
  public final long appendMetadata(long term, long newCommitIndex) {
    return runner.runSequentially(() -> appendMetadataImpl(term, newCommitIndex));
  }

  private long appendMetadataImpl(long term, long newCommitIndex) {
    checkLogState();
    if (!shouldAppendMetadata(newCommitIndex)) {
      return INVALID_LOG_INDEX;
    }

    final LogEntryProto entry;
    final long nextIndex;
    try(AutoCloseableLock writeLock = writeLock()) {
      nextIndex = getNextIndex();
      entry = LogProtoUtils.toLogEntryProto(newCommitIndex, term, nextIndex);
      appendEntry(entry);
    }
    lastMetadataEntry.set(entry);
    return nextIndex;
  }

  private boolean shouldAppendMetadata(long newCommitIndex) {
    if (newCommitIndex <= 0) {
      // do not log the first conf entry
      return false;
    } else if (Optional.ofNullable(lastMetadataEntry.get())
        .filter(e -> e.getIndex() == newCommitIndex || e.getMetadataEntry().getCommitIndex() >= newCommitIndex)
        .isPresent()) {
      //log neither lastMetadataEntry, nor entries with a smaller commit index.
      return false;
    }
    ReferenceCountedObject<LogEntryProto> ref = null;
    try {
      ref = retainLog(newCommitIndex);
      if (ref.get().hasMetadataEntry()) {
        // do not log the metadata entry
        return false;
      }
    } catch(RaftLogIOException e) {
      LOG.error("Failed to get log entry for index " + newCommitIndex, e);
    } finally {
      if (ref != null) {
        ref.release();
      }
    }
    return true;
  }

  @Override
  public final long append(long term, RaftConfiguration configuration) {
    return runner.runSequentially(() -> appendImpl(term, configuration));
  }

  private long appendImpl(long term, RaftConfiguration newConf) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      final long nextIndex = getNextIndex();
      appendEntry(LogProtoUtils.toLogEntryProto(newConf, term, nextIndex));
      return nextIndex;
    }
  }

  @Override
  public final void open(long lastIndexInSnapshot, Consumer<LogEntryProto> consumer) throws IOException {
    openImpl(lastIndexInSnapshot, e -> {
      if (e.hasMetadataEntry()) {
        lastMetadataEntry.set(e);
      } else if (consumer != null) {
        consumer.accept(e);
      }
    });
    Optional.ofNullable(lastMetadataEntry.get()).ifPresent(
        e -> commitIndex.updateToMax(e.getMetadataEntry().getCommitIndex(), infoIndexChange));
    state.open();

    final long startIndex = getStartIndex();
    if (startIndex > LEAST_VALID_LOG_INDEX) {
      purgeIndex.updateIncreasingly(startIndex - 1, infoIndexChange);
    }
  }

  protected void openImpl(long lastIndexInSnapshot, Consumer<LogEntryProto> consumer) throws IOException {
  }

  /**
   * Validate the term and index of entry w.r.t RaftLog
   */
  protected void validateLogEntry(LogEntryProto entry) {
    if (entry.hasMetadataEntry()) {
      return;
    }
    long latestSnapshotIndex = getSnapshotIndex();
    TermIndex lastTermIndex = getLastEntryTermIndex();
    if (lastTermIndex != null) {
      long lastIndex = lastTermIndex.getIndex() > latestSnapshotIndex ?
          lastTermIndex.getIndex() : latestSnapshotIndex;
      Preconditions.assertTrue(entry.getTerm() >= lastTermIndex.getTerm(),
          "Entry term less than RaftLog's last term: %d, entry: %s", lastTermIndex.getTerm(), entry);
      Preconditions.assertTrue(entry.getIndex() == lastIndex + 1,
          "Difference between entry index and RaftLog's last index %d (or snapshot index %d) " +
              "is greater than 1, entry: %s",
          lastTermIndex.getIndex(), latestSnapshotIndex, entry);
    } else {
      Preconditions.assertTrue(entry.getIndex() == latestSnapshotIndex + 1,
          "Difference between entry index and RaftLog's latest snapshot index %d is greater than 1 " +
              "and in between log entries are not present, entry: %s",
          latestSnapshotIndex, entry);
    }
  }

  @Override
  public final CompletableFuture<Long> truncate(long index) {
    return runner.runSequentially(() -> truncateImpl(index));
  }

  protected abstract CompletableFuture<Long> truncateImpl(long index);

  @Override
  public final CompletableFuture<Long> purge(long suggestedIndex) {
    if (purgePreservation > 0) {
      final long currentIndex = getNextIndex() - 1;
      suggestedIndex = Math.min(suggestedIndex, currentIndex - purgePreservation);
    }
    final long lastPurge = purgeIndex.get();
    if (suggestedIndex - lastPurge < purgeGap) {
      return CompletableFuture.completedFuture(lastPurge);
    }
    LOG.info("{}: purge {}", getName(), suggestedIndex);
    final long finalSuggestedIndex = suggestedIndex;
    return purgeImpl(suggestedIndex).whenComplete((purged, e) -> {
      if (purged != null) {
        purgeIndex.updateToMax(purged, infoIndexChange);
      }
      if (e != null) {
        LOG.warn(getName() + ": Failed to purge " + finalSuggestedIndex, e);
      }
    });
  }

  protected abstract CompletableFuture<Long> purgeImpl(long index);

  @Override
  public final CompletableFuture<Long> appendEntry(LogEntryProto entry) {
    return appendEntry(ReferenceCountedObject.wrap(entry), null);
  }

  @Override
  public final CompletableFuture<Long> appendEntry(ReferenceCountedObject<LogEntryProto> entry,
      TransactionContext context) {
    return runner.runSequentially(() -> appendEntryImpl(entry, context));
  }

  protected abstract CompletableFuture<Long> appendEntryImpl(ReferenceCountedObject<LogEntryProto> entry,
      TransactionContext context);

  @Override
  public final List<CompletableFuture<Long>> append(ReferenceCountedObject<List<LogEntryProto>> entries) {
    return runner.runSequentially(() -> appendImpl(entries));
  }

  protected List<CompletableFuture<Long>> appendImpl(List<LogEntryProto> entries) {
    throw new UnsupportedOperationException();
  }

  protected List<CompletableFuture<Long>> appendImpl(ReferenceCountedObject<List<LogEntryProto>> entriesRef) {
    try(UncheckedAutoCloseableSupplier<List<LogEntryProto>> entries = entriesRef.retainAndReleaseOnClose()) {
      return appendImpl(entries.get());
    }
  }

  @Override
  public String toString() {
    return getName() + ":" + state + ":c" + getLastCommittedIndex()
        + (isOpened()? ":last" + getLastEntryTermIndex(): "");
  }

  public AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  public AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  public boolean hasWriteLock() {
    return this.lock.isWriteLockedByCurrentThread();
  }

  public boolean hasReadLock() {
    return this.lock.getReadHoldCount() > 0 || hasWriteLock();
  }

  @Override
  public void close() throws IOException {
    state.close();
  }

  public String getName() {
    return name;
  }

  protected ReferenceCountedObject<EntryWithData> newEntryWithData(ReferenceCountedObject<LogEntryProto> retained) {
    return retained.delegate(new EntryWithDataImpl(retained.get(), null));
  }

  protected ReferenceCountedObject<EntryWithData> newEntryWithData(ReferenceCountedObject<LogEntryProto> retained,
      CompletableFuture<ReferenceCountedObject<ByteString>> stateMachineDataFuture) {
    final EntryWithDataImpl impl = new EntryWithDataImpl(retained.get(), stateMachineDataFuture);
    return new ReferenceCountedObject<EntryWithData>() {
      private CompletableFuture<ReferenceCountedObject<ByteString>> future
          = Objects.requireNonNull(stateMachineDataFuture, "stateMachineDataFuture == null");

      @Override
      public EntryWithData get() {
        return impl;
      }

      synchronized void updateFuture(Consumer<ReferenceCountedObject<?>> action) {
        future = future.whenComplete((ref, e) -> {
          if (ref != null) {
            action.accept(ref);
          }
        });
      }

      @Override
      public EntryWithData retain() {
        retained.retain();
        updateFuture(ReferenceCountedObject::retain);
        return impl;
      }

      @Override
      public boolean release() {
        updateFuture(ReferenceCountedObject::release);
        return retained.release();
      }
    };
  }

  /**
   * Holds proto entry along with future which contains read state machine data
   */
  class EntryWithDataImpl implements EntryWithData {
    private final LogEntryProto logEntry;
    private final CompletableFuture<ReferenceCountedObject<ByteString>> future;

    EntryWithDataImpl(LogEntryProto logEntry, CompletableFuture<ReferenceCountedObject<ByteString>> future) {
      this.logEntry = logEntry;
      this.future = future == null? null: future.thenApply(this::checkStateMachineData);
    }

    private ReferenceCountedObject<ByteString> checkStateMachineData(ReferenceCountedObject<ByteString> data) {
      if (data == null) {
        throw new IllegalStateException("State machine data is null for log entry " + this);
      }
      return data;
    }

    @Override
    public long getIndex() {
      return logEntry.getIndex();
    }

    @Override
    public int getSerializedSize() {
      return LogProtoUtils.getSerializedSize(logEntry);
    }

    @Override
    public LogEntryProto getEntry(TimeDuration timeout) throws RaftLogIOException, TimeoutException {
      if (future == null) {
        return logEntry;
      }

      final LogEntryProto entryProto;
      ReferenceCountedObject<ByteString> data;
      try {
        data = future.get(timeout.getDuration(), timeout.getUnit());
        entryProto = LogProtoUtils.addStateMachineData(data.get(), logEntry);
      } catch (TimeoutException t) {
        if (timeout.compareTo(stateMachineDataReadTimeout) > 0) {
          getRaftLogMetrics().onStateMachineDataReadTimeout();
        }
        discardData();
        throw t;
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        discardData();
        final String err = getName() + ": Failed readStateMachineData for " + this;
        LOG.error(err, e);
        throw new RaftLogIOException(err, JavaUtils.unwrapCompletionException(e));
      }
      // by this time we have already read the state machine data,
      // so the log entry data should be set now
      if (LogProtoUtils.isStateMachineDataEmpty(entryProto)) {
        final String err = getName() + ": State machine data not set for " + this;
        LOG.error(err);
        data.release();
        throw new RaftLogIOException(err);
      }
      return entryProto;
    }

    private void discardData() {
      future.whenComplete((r, ex) -> {
        if (r != null) {
          r.release();
        }
      });
    }

    @Override
    public String toString() {
      return toLogEntryString(logEntry);
    }
  }

  public String toLogEntryString(LogEntryProto logEntry) {
    return LogProtoUtils.toLogEntryString(logEntry);
  }
}
