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
package org.apache.hadoop.raft.server;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.server.protocol.ConfigurationEntry;
import org.apache.hadoop.raft.server.protocol.Entry;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftLog {
  public static final Logger LOG = LoggerFactory.getLogger(RaftLog.class);
  static final Entry DUMMY_ENTRY = new Entry(-1, 0, null);

  private final ServerState state;
  private final List<Entry> entries = new ArrayList<>();
  private final AtomicLong lastCommitted = new AtomicLong();

  RaftLog(ServerState state) {
    this.state = state;
    //add a dummy entry so that the first log index is 1.
    entries.add(DUMMY_ENTRY);
  }

  synchronized TermIndex getLastCommitted() {
    return get(lastCommitted.get());
  }

  /**
   * @return whether there is a configuration log entry between the index range
   */
  synchronized boolean committedConfEntry(long oldCommitted) {
    for (long i = oldCommitted + 1; i <= lastCommitted.get(); i++) {
      if (get(i).isConfigurationEntry()) {
        return true;
      }
    }
    return false;
  }

  synchronized void updateLastCommitted(long majority, long currentTerm) {
    if (lastCommitted.get() < majority) {
      // Only update last committed index for current term.
      final TermIndex ti = get(majority);
      if (ti != null && ti.getTerm() == currentTerm) {
        LOG.debug("{}: Updating lastCommitted to {}", state, majority);
        lastCommitted.set(majority);
        synchronized (lastCommitted) {
          lastCommitted.notifyAll();
        }
      }
    }
  }

  long waitLastCommitted() throws InterruptedException {
    synchronized (lastCommitted) {
      lastCommitted.wait();
    }
    return lastCommitted.get();
  }

  private int findIndex(long index) {
    Preconditions.checkArgument(index >= 0);
    return (int)index;
  }

  synchronized Entry get(long index) {
    final int i = findIndex(index);
    return i >= 0 && i < entries.size()? entries.get(i): null;
  }

  public synchronized Entry[] getEntries(long startIndex) {
    final int i = findIndex(startIndex);
    final int size = entries.size();
    return i < size? entries.subList(i, size).toArray(Entry.EMPTY_ARRAY): null;
  }

  private RaftConfiguration truncate(long index) {
    final int truncateIndex = findIndex(index);
    RaftConfiguration oldConf = null;
    for(int i = entries.size() - 1; i >= truncateIndex; i--) {
      Entry removed = entries.remove(i);
      if (removed.isConfigurationEntry()) {
        oldConf = ((ConfigurationEntry) removed).getPrev();
      }
    }
    return oldConf;
  }

  /** Does the contain the given term and index? */
  boolean contains(TermIndex ti) {
    return ti != null && ti.equals(get(ti.getIndex()));
  }

  synchronized Entry getLastEntry() {
    final int size = entries.size();
    return size == 0? null: entries.get(size - 1);
  }

  long getNextIndex() {
    return getLastEntry().getIndex() + 1;
  }

  synchronized long apply(long term, Message message) {
    final long nextIndex = getNextIndex();
    final Entry e = new Entry(term, nextIndex, message);
    Preconditions.checkState(entries.add(e));
    return nextIndex;
  }

  synchronized long apply(long term, RaftConfiguration old,
      RaftConfiguration newConf) {
    final long nextIndex = getNextIndex();
    ConfigurationEntry entry = new ConfigurationEntry(term, nextIndex, old,
        newConf);
    entries.add(entry);
    return nextIndex;
  }

  /**
   * If an existing entry conflicts with a new one (same index but different
   * terms), delete the existing entry and all that follow it (§5.3)
   *
   * This method, {@link #apply(long, Message)}, and {@link #truncate(long)}
   * do not guarantee the changes are persisted. Need to call {@link #logSync()}
   * to persist the changes.
   */
  synchronized RaftConfiguration apply(Entry... entries) {
    if (entries == null || entries.length == 0) {
      return null;
    }
    RaftConfiguration conf = truncate(entries[0].getIndex());
    for (Entry entry : entries) {
      Preconditions.checkState(this.entries.add(entry));
      if (entry.isConfigurationEntry()) {
        conf = ((ConfigurationEntry) entry).getCurrent();
      }
    }
    return conf;
  }

  @Override
  public synchronized String toString() {
    return "last=" + getLastEntry() + ", committed=" + getLastCommitted();
  }

  public synchronized void printEntries(PrintStream out) {
    out.println("entries=" + entries);
  }

  /**
   * TODO persist the log
   * TODO also need to persist leaderId/currentTerm in ServerState when logSync
   * is triggered by AppendEntries RPC request from the leader
   * and also votedFor for requestVote or leaderelection
   */
  public void logSync() {
  }
}
