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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.server.impl.ConfigurationManager;
import org.apache.ratis.server.impl.RaftConfiguration;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class of RaftLog. Currently we provide two types of RaftLog
 * implementation:
 * 1. MemoryRaftLog: all the log entries are stored in memory. This is only used
 *    for testing.
 * 2. Segmented RaftLog: the log entries are persisted on disk, and are stored
 *    in segments.
 */
public abstract class RaftLog implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(RaftLog.class);
  public static final LogEntryProto[] EMPTY_LOGENTRY_ARRAY = new LogEntryProto[0];
  public static final String LOG_SYNC = RaftLog.class.getSimpleName() + ".logSync";

  /**
   * The largest committed index. Note the last committed log may be included
   * in the latest snapshot file.
   */
  protected final AtomicLong lastCommitted =
      new AtomicLong(RaftServerConstants.INVALID_LOG_INDEX);
  private final RaftPeerId selfId;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private volatile boolean isOpen = false;

  public RaftLog(RaftPeerId selfId) {
    this.selfId = selfId;
  }

  public long getLastCommittedIndex() {
    return lastCommitted.get();
  }

  public void checkLogState() {
    Preconditions.assertTrue(isOpen,
        "The RaftLog has not been opened or has been closed");
  }

  /**
   * Update the last committed index.
   * @param majorityIndex the index that has achieved majority.
   * @param currentTerm the current term.
   */
  public void updateLastCommitted(long majorityIndex, long currentTerm) {
    try(AutoCloseableLock writeLock = writeLock()) {
      if (lastCommitted.get() < majorityIndex) {
        // Only update last committed index for current term. See §5.4.2 in
        // paper for details.
        final LogEntryProto entry = get(majorityIndex);
        if (entry != null && entry.getTerm() == currentTerm) {
          LOG.debug("{}: Updating lastCommitted to {}", selfId, majorityIndex);
          lastCommitted.set(majorityIndex);
        }
      }
    }
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
    LogEntryProto entry = get(ti.getIndex());
    TermIndex local = ServerProtoUtils.toTermIndex(entry);
    return ti.equals(local);
  }

  /**
   * @return the index of the next log entry to append.
   */
  public long getNextIndex() {
    final LogEntryProto last = getLastEntry();
    if (last == null) {
      // if the log is empty, the last committed index should be consistent with
      // the last index included in the latest snapshot.
      return getLastCommittedIndex() + 1;
    }
    return last.getIndex() + 1;
  }

  /**
   * Generate a log entry for the given term and message, and append the entry.
   * Used by the leader.
   * @return the index of the new log entry.
   */
  public long append(long term, TransactionContext operation,
      ClientId clientId, long callId) throws StateMachineException {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      final long nextIndex = getNextIndex();

      // This is called here to guarantee strict serialization of callback executions in case
      // the SM wants to attach a logic depending on ordered execution in the log commit order.
      try {
        operation = operation.preAppendTransaction();
      } catch (IOException e) {
        throw new StateMachineException(selfId.toString(), e);
      }

      // build the log entry after calling the StateMachine
      final LogEntryProto e = ProtoUtils.toLogEntryProto(
          operation.getSMLogEntry().get(), term, nextIndex, clientId, callId);

      appendEntry(e);
      operation.setLogEntry(e);
      return nextIndex;
    }
  }

  /**
   * Generate a log entry for the given term and configurations,
   * and append the entry. Used by the leader.
   * @return the index of the new log entry.
   */
  public long append(long term, RaftConfiguration newConf) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      final long nextIndex = getNextIndex();
      final LogEntryProto e = ServerProtoUtils.toLogEntryProto(newConf, term,
          nextIndex);
      appendEntry(e);
      return nextIndex;
    }
  }

  public void open(ConfigurationManager confManager, long lastIndexInSnapshot)
      throws IOException {
    isOpen = true;
  }

  public abstract long getStartIndex();

  /**
   * Get the log entry of the given index.
   *
   * @param index The given index.
   * @return The log entry associated with the given index.
   *         Null if there is no log entry with the index.
   */
  public abstract LogEntryProto get(long index);

  /**
   * @param startIndex the starting log index (inclusive)
   * @param endIndex the ending log index (exclusive)
   * @return all log entries within the given index range. Null if startIndex
   *         is greater than the smallest available index.
   */
  public abstract LogEntryProto[] getEntries(long startIndex, long endIndex);

  /**
   * @return the last log entry.
   */
  public abstract LogEntryProto getLastEntry();

  /**
   * Truncate the log entries till the given index. The log with the given index
   * will also be truncated (i.e., inclusive).
   */
  abstract void truncate(long index);

  /**
   * Used by the leader when appending a new entry based on client's request
   * or configuration change.
   */
  abstract void appendEntry(LogEntryProto entry);

  /**
   * Append all the given log entries. Used by the followers.
   *
   * If an existing entry conflicts with a new one (same index but different
   * terms), delete the existing entry and all entries that follow it (§5.3).
   *
   * This method, {@link #append(long, TransactionContext, ClientId, long)},
   * {@link #append(long, RaftConfiguration)}, and {@link #truncate(long)},
   * do not guarantee the changes are persisted.
   * Need to call {@link #logSync()} to persist the changes.
   */
  public abstract void append(LogEntryProto... entries);

  /**
   * Flush and sync the log.
   * It is triggered by AppendEntries RPC request from the leader.
   */
  public abstract void logSync() throws InterruptedException;

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

  @Override
  public String toString() {
    return ServerProtoUtils.toString(getLastEntry());
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
    isOpen = false;
  }

  public RaftPeerId getSelfId() {
    return selfId;
  }
}
