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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.raft.RaftTestUtil;
import org.apache.hadoop.raft.RaftTestUtil.SimpleMessage;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.server.RaftConstants.StartupOption;
import org.apache.hadoop.raft.util.RaftUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestSegmentedRaftLog {
  static {
    GenericTestUtils.setLogLevel(RaftLogWorker.LOG, Level.DEBUG);
  }

  private static final String peerId = "s0";

  private static class SegmentRange {
    final long start;
    final long end;
    final long term;
    final boolean isOpen;

    SegmentRange(long s, long e, long term, boolean isOpen) {
      this.start = s;
      this.end = e;
      this.term = term;
      this.isOpen = isOpen;
    }
  }

  private File storageDir;

  @Before
  public void setup() throws Exception {
    storageDir = RaftTestUtil.getTestDir(TestSegmentedRaftLog.class);
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtil.fullyDelete(storageDir.getParentFile());
    }
  }

  private LogEntryProto[] prepareLog(List<SegmentRange> list) throws IOException {
    List<LogEntryProto> entryList = new ArrayList<>();
    RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
    for (SegmentRange range : list) {
      File file = range.isOpen ?
          storage.getStorageDir().getOpenLogFile(range.start) :
          storage.getStorageDir().getClosedLogFile(range.start, range.end);

      final int size = (int) (range.end - range.start + 1);
      LogEntryProto[] entries = new LogEntryProto[size];
      try (LogOutputStream out = new LogOutputStream(file, false)) {
        for (int i = 0; i < size; i++) {
          SimpleMessage m = new SimpleMessage("m" + (i + range.start));
          entries[i] = RaftUtils.convertRequestToLogEntryProto(m, range.term,
              i + range.start);
          out.write(entries[i]);
        }
      }
      Collections.addAll(entryList, entries);
    }
    storage.close();
    return entryList.toArray(new LogEntryProto[entryList.size()]);
  }

  private List<SegmentRange> prepareRanges(int number, int segmentSize,
      long startIndex) {
    List<SegmentRange> list = new ArrayList<>(number);
    for (int i = 0; i < number; i++) {
      list.add(new SegmentRange(startIndex, startIndex + segmentSize - 1, i,
          i == number - 1));
      startIndex += segmentSize;
    }
    return list;
  }

  @Test
  public void testLoadLogSegments() throws Exception {
    // first generate log files
    List<SegmentRange> ranges = prepareRanges(5, 100, 0);
    LogEntryProto[] entries = prepareLog(ranges);

    // create RaftLog object and load log file
    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, storageDir)) {
      // check if log entries are loaded correctly
      for (LogEntryProto e : entries) {
        LogEntryProto entry = raftLog.get(e.getIndex());
        Assert.assertEquals(e, entry);
      }

      Assert.assertArrayEquals(entries, raftLog.getEntries(0, 500));
      Assert.assertEquals(entries[entries.length - 1], raftLog.getLastEntry());
    }
  }

  List<LogEntryProto> prepareLogEntries(List<SegmentRange> slist) {
    List<LogEntryProto> eList = new ArrayList<>();
    for (SegmentRange range : slist) {
      for (long index = range.start; index <= range.end; index++) {
        SimpleMessage m = new SimpleMessage("m" + index);
        eList.add(RaftUtils.convertRequestToLogEntryProto(m, range.term, index));
      }
    }
    return eList;
  }

  /**
   * Append entry one by one and check if log state is correct.
   */
  @Test
  public void testAppendEntry() throws Exception {
    List<SegmentRange> ranges = prepareRanges(5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges);

    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, storageDir)) {
      // append entries to the raftlog
      entries.forEach(raftLog::appendEntry);
      raftLog.logSync(entries.get(entries.size() - 1).getIndex());
    }

    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, storageDir)) {
      // check if the raft log is correct
      for (LogEntryProto e : entries) {
        LogEntryProto entry = raftLog.get(e.getIndex());
        Assert.assertEquals(e, entry);
      }
    }
  }

  /**
   * Test append with inconsistent entries
   */
  @Test
  public void testAppendEntriesWithInconsistency() throws Exception {

  }

}
