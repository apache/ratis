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

import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.raft.server.protocol.RaftLogEntry;
import org.apache.hadoop.raft.server.protocol.TermIndex;
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
  public synchronized TermIndex getLastCommitted() {
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
      final TermIndex ti = get(majorityIndex);
      if (ti != null && ti.getTerm() == currentTerm) {
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
      if (get(i).isConfigurationEntry()) {
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
    return ti != null && ti.equals(get(ti.getIndex()));
  }

  /**
   * @return the index of the next log entry to append.
   */
  public long getNextIndex() {
    final RaftLogEntry last = getLastEntry();
    return last == null ? 0 : last.getIndex() + 1;
  }

  /**
   * Get the log entry of the given index.
   *
   * @param index The given index.
   * @return The log entry associated with the given index.
   *         Null if there is no log entry with the index.
   */
  public abstract RaftLogEntry get(long index);

  /**
   * @return all log entries starting from the given index.
   */
  public abstract RaftLogEntry[] getEntries(long startIndex);

  /**
   * @return the last log entry.
   */
  public abstract RaftLogEntry getLastEntry();

  /**
   * Truncate the log entries till the given index. The log with the given index
   * will also be truncated (i.e., inclusive).
   *
   * @return The old RaftConfiguration the peer should use if a configuration
   *         log entry is truncated. Null if no configuration entry is truncated.
   */
  abstract RaftConfiguration truncate(long index);

  /**
   * Generate a log entry for the given term and message, and append the entry.
   * Used by the leader.
   * @return the index of the new log entry.
   */
  public abstract long append(long term, Message message);

  /**
   * Generate a log entry for the given term and configurations,
   * and append the entry. Used by the leader.
   * @return the index of the new log entry.
   */
  public abstract long append(long term, RaftConfiguration old,
      RaftConfiguration newConf);

  /**
   * Append all the given log entries. Used by the followers.
   *
   * If an existing entry conflicts with a new one (same index but different
   * terms), delete the existing entry and all entries that follow it (§5.3).
   *
   * This method, {@link #append(long, Message)},
   * {@link #append(long, RaftConfiguration, RaftConfiguration)},
   * and {@link #truncate(long)} do not guarantee the changes are persisted.
   * Need to call {@link #logSync()} to persist the changes.
   *
   * @return the latest raft configuration the peer should use if configuration
   *         entries are appended. Null if no configuration entry is appended.
   */
  public abstract RaftConfiguration append(RaftLogEntry... entries);

  /**
   * TODO persist the log. also need to persist leaderId/currentTerm.
   * is triggered by AppendEntries RPC request from the leader
   * and also votedFor for requestVote or leader election
   */
  public abstract void logSync();
}
