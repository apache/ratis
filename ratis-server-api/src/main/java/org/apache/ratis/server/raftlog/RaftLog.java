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

import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.server.metrics.RaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorageMetadata;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * {@link RaftLog} is a transaction log of a raft service.
 */
public interface RaftLog extends RaftLogSequentialOps, Closeable {
  Logger LOG = LoggerFactory.getLogger(RaftLog.class);

  /** The least valid log index, i.e. the index used when writing to an empty log. */
  long LEAST_VALID_LOG_INDEX = 0L;
  /** Invalid log index is used to indicate that the log index is missing. */
  long INVALID_LOG_INDEX = LEAST_VALID_LOG_INDEX - 1;

  /** Does this log contains the given {@link TermIndex}? */
  default boolean contains(TermIndex ti) {
    Objects.requireNonNull(ti, "ti == null");
    return ti.equals(getTermIndex(ti.getIndex()));
  }

  /**
   * @return null if the log entry is not found in this log;
   *         otherwise, return the {@link TermIndex} of the log entry corresponding to the given index.
   */
  TermIndex getTermIndex(long index);

  /**
   * @return null if the log entry is not found in this log;
   *         otherwise, return the log entry corresponding to the given index.
   */
  LogEntryProto get(long index) throws RaftLogIOException;

  /**
   * @return null if the log entry is not found in this log;
   *         otherwise, return the {@link EntryWithData} corresponding to the given index.
   */
  EntryWithData getEntryWithData(long index) throws RaftLogIOException;

  /**
   * @param startIndex the starting log index (inclusive)
   * @param endIndex the ending log index (exclusive)
   * @return null if entries are unavailable in this log;
   *         otherwise, return the log entry headers within the given index range.
   */
  LogEntryHeader[] getEntries(long startIndex, long endIndex);

  /** @return the index of the starting entry of this log. */
  long getStartIndex();

  /** @return the index of the next entry to append. */
  default long getNextIndex() {
    final TermIndex last = getLastEntryTermIndex();
    if (last == null) {
      // if the log is empty, the last committed index should be consistent with
      // the last index included in the latest snapshot.
      return getLastCommittedIndex() + 1;
    }
    return last.getIndex() + 1;
  }

  /** @return the index of the last entry that has been committed. */
  long getLastCommittedIndex();

  /** @return the index of the latest snapshot. */
  long getSnapshotIndex();

  /** @return the index of the last entry that has been flushed to the local storage. */
  long getFlushIndex();

  /** @return the {@link TermIndex} of the last log entry. */
  TermIndex getLastEntryTermIndex();

  /** @return the {@link RaftLogMetrics}. */
  RaftLogMetrics getRaftLogMetrics();

  /**
   * Update the commit index.
   * @param majorityIndex the index that has achieved majority.
   * @param currentTerm the current term.
   * @param isLeader Is this server the leader?
   * @return true if commit index is changed; otherwise, return false.
   */
  boolean updateCommitIndex(long majorityIndex, long currentTerm, boolean isLeader);

  /**
   * Update the snapshot index with the given index.
   * Note that the commit index may also be changed by this update.
   */
  void updateSnapshotIndex(long newSnapshotIndex);

  /** Open this log for read and write. */
  void open(long lastIndexInSnapshot, Consumer<LogEntryProto> consumer) throws IOException;

  /**
   * Purge asynchronously the log transactions.
   * The implementation may choose to purge an index other than the suggested index.
   *
   * @param suggestedIndex the suggested index (inclusive) to be purged.
   * @return the future of the actual purged log index.
   */
  CompletableFuture<Long> purge(long suggestedIndex);

  /** Persist the given metadata. */
  void persistMetadata(RaftStorageMetadata metadata) throws IOException;

  /** Load metadata. */
  RaftStorageMetadata loadMetadata() throws IOException;

  /**
   * A snapshot is installed so that the indices and other information of this log must be updated.
   * This log may also purge the outdated entries.
   *
   * @return the future of the actual purged log index (inclusive).
   */
  CompletableFuture<Long> onSnapshotInstalled(long lastSnapshotIndex);

  /**
   * Log entry with state machine data.
   *
   * When both {@link LogEntryProto#hasStateMachineLogEntry()} and
   * {@link StateMachineLogEntryProto#hasStateMachineEntry()} are true,
   * the {@link StateMachineEntryProto} is removed from the original {@link LogEntryProto}
   * before appending to this log.
   * The {@link StateMachineEntryProto} is stored by the state machine but not in this log.
   * When reading the log entry, this class rebuilds the original {@link LogEntryProto}
   * containing both the log entry and the state machine data.
   */
  interface EntryWithData {
    /** @return the serialized size including both log entry and state machine data. */
    int getSerializedSize();

    /** @return the {@link LogEntryProto} containing both the log entry and the state machine data. */
    LogEntryProto getEntry(TimeDuration timeout) throws RaftLogIOException, TimeoutException;
  }
}
