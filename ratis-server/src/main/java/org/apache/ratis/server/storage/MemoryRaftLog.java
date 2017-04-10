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

import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftConfiguration;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.Preconditions;

/**
 * A simple RaftLog implementation in memory. Used only for testing.
 */
public class MemoryRaftLog extends RaftLog {
  private final List<LogEntryProto> entries = new ArrayList<>();

  public MemoryRaftLog(RaftPeerId selfId) {
    super(selfId);
  }

  @Override
  public LogEntryProto get(long index) {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      final int i = (int) index;
      return i >= 0 && i < entries.size() ? entries.get(i) : null;
    }
  }

  @Override
  public LogEntryProto[] getEntries(long startIndex, long endIndex) {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      final int i = (int) startIndex;
      if (startIndex >= entries.size()) {
        return null;
      }
      final int toIndex = (int) Math.min(entries.size(), endIndex);
      return entries.subList(i, toIndex).toArray(EMPTY_LOGENTRY_ARRAY);
    }
  }

  @Override
  void truncate(long index) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      Preconditions.assertTrue(index >= 0);
      final int truncateIndex = (int) index;
      for (int i = entries.size() - 1; i >= truncateIndex; i--) {
        entries.remove(i);
      }
    }
  }

  @Override
  public LogEntryProto getLastEntry() {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      final int size = entries.size();
      return size == 0 ? null : entries.get(size - 1);
    }
  }

  @Override
  void appendEntry(LogEntryProto entry) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      entries.add(entry);
    }
  }

  @Override
  public long append(long term, RaftConfiguration newConf) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      final long nextIndex = getNextIndex();
      final LogEntryProto e = ServerProtoUtils.toLogEntryProto(newConf, term,
          nextIndex);
      entries.add(e);
      return nextIndex;
    }
  }

  @Override
  public long getStartIndex() {
    return entries.isEmpty() ? RaftServerConstants.INVALID_LOG_INDEX :
        entries.get(0).getIndex();
  }

  @Override
  public void append(LogEntryProto... entries) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      if (entries == null || entries.length == 0) {
        return;
      }
      // Before truncating the entries, we first need to check if some
      // entries are duplicated. If the leader sends entry 6, entry 7, then
      // entry 6 again, without this check the follower may truncate entry 7
      // when receiving entry 6 again. Then before the leader detects this
      // truncation in the next appendEntries RPC, leader may think entry 7 has
      // been committed but in the system the entry has not been committed to
      // the quorum of peers' disks.
      // TODO add a unit test for this
      boolean toTruncate = false;
      int truncateIndex = (int) entries[0].getIndex();
      int index = 0;
      for (; truncateIndex < getNextIndex() && index < entries.length;
           index++, truncateIndex++) {
        if (this.entries.get(truncateIndex).getTerm() !=
            entries[index].getTerm()) {
          toTruncate = true;
          break;
        }
      }
      if (toTruncate) {
        truncate(truncateIndex);
      }
      //  Collections.addAll(this.entries, entries);
      for (int i = index; i < entries.length; i++) {
        this.entries.add(entries[i]);
      }
    }
  }

  @Override
  public String toString() {
    return "last=" + ServerProtoUtils.toString(getLastEntry())
        + ", committed="
        + ServerProtoUtils.toString(get(getLastCommittedIndex()));
  }

  public String getEntryString() {
    return "entries=" + entries;
  }

  @Override
  public void logSync() {
    CodeInjectionForTesting.execute(LOG_SYNC, getSelfId(), null);
    // do nothing
  }

  @Override
  public long getLatestFlushedIndex() {
    return getNextIndex() - 1;
  }

  @Override
  public void writeMetadata(long term, RaftPeerId votedFor) {
    // do nothing
  }

  @Override
  public Metadata loadMetadata() {
    return new Metadata(null, 0);
  }

  @Override
  public void syncWithSnapshot(long lastSnapshotIndex) {
    // do nothing
  }
}
