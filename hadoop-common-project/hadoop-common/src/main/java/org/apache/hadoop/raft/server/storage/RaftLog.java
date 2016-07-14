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

import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.raft.util.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class of RaftLog. Currently we provide two types of RaftLog
 * implementation:
 * 1. MemoryRaftLog: all the log entries are stored in memory. This is only used
 *    for testing.
 * 2. Segmented RaftLog: the log entries are persisted on disk, and are stored
 *    in segments.
 */
public abstract class RaftLog {
  public static final Logger LOG = LoggerFactory.getLogger(RaftLog.class);
  public static final LogEntryProto[] EMPTY_LOGENTRY_ARRAY = new LogEntryProto[0];

  /**
   * The largest committed index.
   */
  private final AtomicLong lastCommitted = new AtomicLong();
  private final String selfId;

  public RaftLog(String selfId) {
    this.selfId = selfId;
  }

  /**
   * @return The last committed log entry.
   */
  public synchronized LogEntryProto getLastCommitted() {
    return get(lastCommitted.get());
  }

  public long getLastCommittedIndex() {
    return lastCommitted.get();
  }

  /**
   * Update the last committed index.
   * @param majorityIndex the index that has achieved majority.
   * @param currentTerm the current term.
   */
  public synchronized void updateLastCommitted(long majorityIndex,
      long currentTerm) {
    if (lastCommitted.get() < majorityIndex) {
      // Only update last committed index for current term. See §5.4.2 in paper
      // for details.
      final LogEntryProto entry = get(majorityIndex);
      if (entry != null && entry.getTerm() == currentTerm) {
        LOG.debug("{}: Updating lastCommitted to {}", selfId, majorityIndex);
        lastCommitted.set(majorityIndex);
      }
    }
  }

  /**
   * @return whether there is a configuration log entry between the index range
   */
  public synchronized boolean committedConfEntry(long oldCommitted) {
    for (long i = oldCommitted + 1; i <= lastCommitted.get(); i++) {
      if (RaftUtils.isConfigurationLogEntry(get(i))) {
        return true;
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
    LogEntryProto entry = get(ti.getIndex());
    TermIndex local = entry == null ? null :
        new TermIndex(entry.getTerm(), entry.getIndex());
    return ti.equals(local);
  }

  /**
   * @return the index of the next log entry to append.
   */
  public long getNextIndex() {
    final LogEntryProto last = getLastEntry();
    return last == null ? 0 : last.getIndex() + 1;
  }

  /**
   * Generate a log entry for the given term and message, and append the entry.
   * Used by the leader.
   * @return the index of the new log entry.
   */
  public long append(long term, Message message) {
    final long nextIndex = getNextIndex();
    final LogEntryProto e = RaftUtils.convertRequestToLogEntryProto(message,
        term, nextIndex);
    appendEntry(e);
    return nextIndex;
  }

  /**
   * Generate a log entry for the given term and configurations,
   * and append the entry. Used by the leader.
   * @return the index of the new log entry.
   */
  public long append(long term, RaftConfiguration newConf) {
    final long nextIndex = getNextIndex();
    final LogEntryProto e = RaftUtils.convertConfToLogEntryProto(newConf, term,
        nextIndex);
    appendEntry(e);
    return nextIndex;
  }

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
   * @return the index of the new log entry.
   */
  abstract void appendEntry(LogEntryProto entry);

  /**
   * Append all the given log entries. Used by the followers.
   *
   * If an existing entry conflicts with a new one (same index but different
   * terms), delete the existing entry and all entries that follow it (§5.3).
   *
   * This method, {@link #append(long, Message)},
   * {@link #append(long, RaftConfiguration)}, and {@link #truncate(long)},
   * do not guarantee the changes are persisted.
   * Need to call {@link #logSync(long)} to persist the changes.
   */
  public abstract void append(LogEntryProto... entries);

  /**
   * TODO persist the log. also need to persist leaderId/currentTerm.
   * is triggered by AppendEntries RPC request from the leader
   * and also votedFor for requestVote or leader election
   * @param index the index of the entry that needs to be sync'ed
   */
  public abstract void logSync(long index) throws InterruptedException;

  /**
   * @return the index of the latest entry that has been flushed to the local
   *         storage.
   */
  public abstract long getLatestFlushedIndex();
}
