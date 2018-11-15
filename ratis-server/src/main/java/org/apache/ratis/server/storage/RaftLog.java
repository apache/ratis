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
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.server.impl.LogAppender;
import org.apache.ratis.server.impl.RaftConfiguration;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.OpenCloseState;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
/**
 * Base class of RaftLog. Currently we provide two types of RaftLog
 * implementation:
 * 1. MemoryRaftLog: all the log entries are stored in memory. This is only used
 *    for testing.
 * 2. Segmented RaftLog: the log entries are persisted on disk, and are stored
 *    in segments.
 */
public abstract class RaftLog implements RaftLogSequentialOps, Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(RaftLog.class);
  public static final String LOG_SYNC = RaftLog.class.getSimpleName() + ".logSync";

  /**
   * The largest committed index. Note the last committed log may be included
   * in the latest snapshot file.
   */
  protected final AtomicLong lastCommitted =
      new AtomicLong(RaftServerConstants.INVALID_LOG_INDEX);
  private final RaftPeerId selfId;
  private final int maxBufferSize;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final Runner runner = new Runner(this::getName);
  private final OpenCloseState state;

  public RaftLog(RaftPeerId selfId, int maxBufferSize) {
    this.selfId = selfId;
    this.maxBufferSize = maxBufferSize;
    this.state = new OpenCloseState(getName());
  }

  public long getLastCommittedIndex() {
    return lastCommitted.get();
  }

  public void checkLogState() {
    state.assertOpen();
  }

  /**
   * Update the last committed index.
   * @param majorityIndex the index that has achieved majority.
   * @param currentTerm the current term.
   * @return true if update is applied; otherwise, return false, i.e. no update required.
   */
  public boolean updateLastCommitted(long majorityIndex, long currentTerm) {
    try(AutoCloseableLock writeLock = writeLock()) {
      final long oldCommittedIndex = lastCommitted.get();
      if (oldCommittedIndex < majorityIndex) {
        // Only update last committed index for current term. See §5.4.2 in
        // paper for details.
        final TermIndex entry = getTermIndex(majorityIndex);
        if (entry != null && entry.getTerm() == currentTerm) {
          final long commitIndex = Math.min(majorityIndex, getLatestFlushedIndex());
          if (commitIndex > oldCommittedIndex) {
            LOG.debug("{}: updateLastCommitted {} -> {}", selfId, oldCommittedIndex, commitIndex);
            lastCommitted.set(commitIndex);
          }
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Does the log contains the given term and index? Used to check the
   * consistency between the local log of a follower and the log entries sent
   * by the leader.
   */
  public boolean contains(TermIndex ti) {
    if (ti == null) {
      return false;
    }
    TermIndex local = getTermIndex(ti.getIndex());
    return ti.equals(local);
  }

  /**
   * @return the index of the next log entry to append.
   */
  public long getNextIndex() {
    final TermIndex last = getLastEntryTermIndex();
    if (last == null) {
      // if the log is empty, the last committed index should be consistent with
      // the last index included in the latest snapshot.
      return getLastCommittedIndex() + 1;
    }
    return last.getIndex() + 1;
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
      } catch (IOException e) {
        throw new StateMachineException(selfId, e);
      }

      // build the log entry after calling the StateMachine
      final LogEntryProto e = operation.initLogEntry(term, nextIndex);

      int entrySize = e.getSerializedSize();
      if (entrySize > maxBufferSize) {
        throw new StateMachineException(selfId, new RaftLogIOException(
            "Log entry size " + entrySize + " exceeds the max buffer limit of "
                + maxBufferSize));
      }
      appendEntry(e);
      return nextIndex;
    }
  }

  @Override
  public final long append(long term, RaftConfiguration configuration) {
    return runner.runSequentially(() -> appendImpl(term, configuration));
  }

  private long appendImpl(long term, RaftConfiguration newConf) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      final long nextIndex = getNextIndex();
      final LogEntryProto e = ServerProtoUtils.toLogEntryProto(newConf, term,
          nextIndex);
      appendEntry(e);
      return nextIndex;
    }
  }

  public void open(long lastIndexInSnapshot, Consumer<LogEntryProto> consumer)
      throws IOException {
    state.open();
  }

  public abstract long getStartIndex();

  /**
   * Get the log entry of the given index.
   *
   * @param index The given index.
   * @return The log entry associated with the given index.
   *         Null if there is no log entry with the index.
   */
  public abstract LogEntryProto get(long index) throws RaftLogIOException;

  /**
   * Get the log entry of the given index along with the state machine data.
   *
   * @param index The given index.
   * @return The log entry associated with the given index.
   *         Null if there is no log entry with the index.
   */
  public abstract EntryWithData getEntryWithData(long index) throws RaftLogIOException;

  /**
   * Get the TermIndex information of the given index.
   *
   * @param index The given index.
   * @return The TermIndex of the log entry associated with the given index.
   *         Null if there is no log entry with the index.
   */
  public abstract TermIndex getTermIndex(long index);

  /**
   * @param startIndex the starting log index (inclusive)
   * @param endIndex the ending log index (exclusive)
   * @return TermIndex of all log entries within the given index range. Null if
   *         startIndex is greater than the smallest available index.
   */
  public abstract TermIndex[] getEntries(long startIndex, long endIndex);

  /**
   * @return the last log entry's term and index.
   */
  public abstract TermIndex getLastEntryTermIndex();

  /**
   * Validate the term and index of entry w.r.t RaftLog
   */
  public void validateLogEntry(LogEntryProto entry) {
    TermIndex lastTermIndex = getLastEntryTermIndex();
    if (lastTermIndex != null) {
      Preconditions.assertTrue(entry.getTerm() >= lastTermIndex.getTerm(),
          "Entry term less than RaftLog's last term: %d, entry: %s", lastTermIndex.getTerm(), entry);
      Preconditions.assertTrue(entry.getIndex() == lastTermIndex.getIndex() + 1,
          "Difference between entry index and RaftLog's last index %d greater than 1, entry: %s", lastTermIndex.getIndex(), entry);
    }
  }

  @Override
  public final CompletableFuture<Long> truncate(long index) {
    return runner.runSequentially(() -> truncateImpl(index));
  }

  abstract CompletableFuture<Long> truncateImpl(long index);

  @Override
  public final CompletableFuture<Long> appendEntry(LogEntryProto entry) {
    return runner.runSequentially(() -> appendEntryImpl(entry));
  }

  abstract CompletableFuture<Long> appendEntryImpl(LogEntryProto entry);

  @Override
  public final List<CompletableFuture<Long>> append(LogEntryProto... entries) {
    return runner.runSequentially(() -> appendImpl(entries));
  }

  abstract List<CompletableFuture<Long>> appendImpl(LogEntryProto... entries);

  /**
   * @return the index of the latest entry that has been flushed to the local
   *         storage.
   */
  public abstract long getLatestFlushedIndex();

  /**
   * Write and flush the metadata (votedFor and term) into the meta file.
   *
   * We need to guarantee that the order of writeMetadata calls is the same with
   * that when we change the in-memory term/votedFor. Otherwise we may persist
   * stale term/votedFor in file.
   *
   * Since the leader change is not frequent, currently we simply put this call
   * in the RaftPeer's lock. Later we can use an IO task queue to enforce the
   * order.
   */
  public abstract void writeMetadata(long term, RaftPeerId votedFor)
      throws IOException;

  public abstract Metadata loadMetadata() throws IOException;

  public abstract void syncWithSnapshot(long lastSnapshotIndex);

  public abstract boolean isConfigEntry(TermIndex ti);

  @Override
  public String toString() {
    return getName() + ":" + state;
  }

  public static class Metadata {
    private final RaftPeerId votedFor;
    private final long term;

    public Metadata(RaftPeerId votedFor, long term) {
      this.votedFor = votedFor;
      this.term = term;
    }

    public RaftPeerId getVotedFor() {
      return votedFor;
    }

    public long getTerm() {
      return term;
    }
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

  public RaftPeerId getSelfId() {
    return selfId;
  }

  public String getName() {
    return selfId + "-" + getClass().getSimpleName();
  }

  /**
   * Holds proto entry along with future which contains read state machine data
   */
  public class EntryWithData {
    private LogEntryProto logEntry;
    private CompletableFuture<ByteString> future;

    EntryWithData(LogEntryProto logEntry, CompletableFuture<ByteString> future) {
      this.logEntry = logEntry;
      this.future = future;
    }

    public int getSerializedSize() {
      return ServerProtoUtils.getSerializedSize(logEntry);
    }

    public LogEntryProto getEntry() throws RaftLogIOException {
      LogEntryProto entryProto;
      if (future == null) {
        return logEntry;
      }

      try {
        entryProto = future.thenApply(data -> ServerProtoUtils.addStateMachineData(data, logEntry)).join();
      } catch (Throwable t) {
        final String err = selfId + ": Failed readStateMachineData for " +
            ServerProtoUtils.toLogEntryString(logEntry);
        LogAppender.LOG.error(err, t);
        throw new RaftLogIOException(err, JavaUtils.unwrapCompletionException(t));
      }
      // by this time we have already read the state machine data,
      // so the log entry data should be set now
      if (ServerProtoUtils.shouldReadStateMachineData(entryProto)) {
        final String err = selfId + ": State machine data not set for " +
            ServerProtoUtils.toLogEntryString(logEntry);
        LogAppender.LOG.error(err);
        throw new RaftLogIOException(err);
      }
      return entryProto;
    }
  }
}
