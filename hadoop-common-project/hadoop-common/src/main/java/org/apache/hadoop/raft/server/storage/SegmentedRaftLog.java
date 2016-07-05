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

import org.apache.commons.io.Charsets;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.protocol.RaftLogEntry;
import org.apache.hadoop.raft.server.storage.RaftStorageDirectory.PathAndIndex;

import java.io.File;
import java.io.IOException;
import java.util.List;

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
 */
public class SegmentedRaftLog extends RaftLog {
  static final byte[] HEADER = "RAFTLOG1".getBytes(Charsets.UTF_8);

  private final RaftStorage storage;
  private final RaftLogCache cache;

  public SegmentedRaftLog(String selfId, File rootDir) throws IOException {
    super(selfId);
    storage = new RaftStorage(rootDir);
    cache = new RaftLogCache();
    loadLogSegments();
  }

  private void loadLogSegments() throws IOException {
    List<PathAndIndex> paths = storage.getStorageDir().getLogSegmentFiles();
    for (PathAndIndex pi : paths) {
      final LogSegment logSegment = parseLogSegment(pi);
      cache.addSegment(logSegment);
    }
  }

  // TODO: update state machine and configuration based on a passed-in callback
  private LogSegment parseLogSegment(PathAndIndex pi) throws IOException {
    final boolean isOpen = pi.endIndex == RaftConstants.INVALID_LOG_INDEX;
    return LogSegment.loadSegment(pi.path.toFile(), pi.startIndex, pi.endIndex,
        isOpen);
  }

  @Override
  // TODO: change RaftLogEntry to LogEntryProto
  public RaftLogEntry get(long index) {
    return null;
  }

  @Override
  public RaftLogEntry[] getEntries(long startIndex) {
    return new RaftLogEntry[0];
  }

  @Override
  public RaftLogEntry getLastEntry() {
    return null;
  }

  @Override
  void truncate(long index) {
  }

  @Override
  public long append(long term, Message message) {
    return 0;
  }

  @Override
  public long append(long term, RaftConfiguration newConf) {
    return 0;
  }

  @Override
  public void append(RaftLogEntry... entries) {
  }

  @Override
  public void logSync() {

  }
}
