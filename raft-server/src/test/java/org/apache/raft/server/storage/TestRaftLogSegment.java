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
package org.apache.raft.server.storage;

import org.apache.hadoop.fs.FileUtil;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.RaftTestUtil.SimpleMessage;
import org.apache.raft.RaftTestUtil.SimpleOperation;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.protocol.Message;
import org.apache.raft.util.ProtoUtils;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.RaftServerConstants.StartupOption;
import org.apache.raft.server.RaftServerConfigKeys;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.raft.server.RaftServerConfigKeys.RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT;
import static org.apache.raft.server.storage.LogSegment.getEntrySize;

/**
 * Test basic functionality of {@link LogSegment}
 */
public class TestRaftLogSegment {
  private File storageDir;
  private final RaftProperties properties = new RaftProperties();

  @Before
  public void setup() throws Exception {
    storageDir = RaftTestUtil.getTestDir(TestRaftLogSegment.class);
    properties.set(RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_KEY,
        storageDir.getCanonicalPath());
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtil.fullyDelete(storageDir.getParentFile());
    }
  }

  private File prepareLog(boolean isOpen, long start, int size, long term)
      throws IOException {
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File file = isOpen ? storage.getStorageDir().getOpenLogFile(start) :
        storage.getStorageDir().getClosedLogFile(start, start + size - 1);

    LogEntryProto[] entries = new LogEntryProto[size];
    int maxSize = properties.getInt(
        RaftServerConfigKeys.RAFT_LOG_SEGMENT_MAX_SIZE_KEY,
        RaftServerConfigKeys.RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT);
    try (LogOutputStream out = new LogOutputStream(file, false, maxSize)) {
      for (int i = 0; i < size; i++) {
        SimpleOperation op = new SimpleOperation("m" + i);
        entries[i] = ProtoUtils.toLogEntryProto(op.getLogEntryContent(),
            term, i + start);
        out.write(entries[i]);
      }
    }
    storage.close();
    return file;
  }

  private void checkLogSegment(LogSegment segment, long start, long end,
      boolean isOpen, long totalSize, long term) {
    Assert.assertEquals(start, segment.getStartIndex());
    Assert.assertEquals(end, segment.getEndIndex());
    Assert.assertEquals(isOpen, segment.isOpen());
    Assert.assertEquals(totalSize, segment.getTotalSize());

    long offset = SegmentedRaftLog.HEADER_BYTES.length;
    for (long i = start; i <= end; i++) {
      LogSegment.LogRecord record = segment.getLogRecord(i);
      Assert.assertEquals(i, record.entry.getIndex());
      Assert.assertEquals(term, record.entry.getTerm());
      Assert.assertEquals(offset, record.offset);

      offset += getEntrySize(record.entry);
    }
  }

  @Test
  public void testLoadLogSegment() throws Exception {
    // load an open segment
    File openSegmentFile = prepareLog(true, 0, 100, 0);
    LogSegment openSegment = LogSegment.loadSegment(openSegmentFile, 0,
        RaftServerConstants.INVALID_LOG_INDEX, true, null);
    checkLogSegment(openSegment, 0, 99, true, openSegmentFile.length(), 0);

    // load a closed segment (1000-1099)
    File closedSegmentFile = prepareLog(false, 1000, 100, 1);
    LogSegment closedSegment = LogSegment.loadSegment(closedSegmentFile, 1000,
        1099, false, null);
    checkLogSegment(closedSegment, 1000, 1099, false,
        closedSegment.getTotalSize(), 1);
  }

  @Test
  public void testAppendEntries() throws Exception {
    final long start = 1000;
    LogSegment segment = LogSegment.newOpenSegment(start);
    long size = SegmentedRaftLog.HEADER_BYTES.length;
    checkLogSegment(segment, start, start - 1, true, size, 0);

    // append till full
    long term = 0;
    int i = 0;
    List<LogEntryProto> list = new ArrayList<>();
    while (size < RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT) {
      SimpleOperation op = new SimpleOperation("m" + i);
      LogEntryProto entry = ProtoUtils.toLogEntryProto(op.getLogEntryContent(),
          term, i++ + start);
      size += getEntrySize(entry);
      list.add(entry);
    }

    segment.appendToOpenSegment(list.toArray(new LogEntryProto[list.size()]));
    Assert.assertTrue(segment.getTotalSize() >= RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT);
    checkLogSegment(segment, start, i - 1 + start, true, size, term);
  }

  @Test
  public void testAppendWithGap() throws Exception {
    LogSegment segment = LogSegment.newOpenSegment(1000);
    SimpleOperation op = new SimpleOperation("m");
    final RaftProtos.SMLogEntryProto m = op.getLogEntryContent();
    try {
      LogEntryProto entry = ProtoUtils.toLogEntryProto(m, 0, 1001);
      segment.appendToOpenSegment(entry);
      Assert.fail("should fail since the entry's index needs to be 1000");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
    }

    LogEntryProto entry = ProtoUtils.toLogEntryProto(m, 0, 1000);
    segment.appendToOpenSegment(entry);

    try {
      entry = ProtoUtils.toLogEntryProto(m, 0, 1002);
      segment.appendToOpenSegment(entry);
      Assert.fail("should fail since the entry's index needs to be 1001");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
    }

    LogEntryProto[] entries = new LogEntryProto[2];
    for (int i = 0; i < 2; i++) {
      entries[i] = ProtoUtils.toLogEntryProto(m, 0, 1001 + i * 2);
    }
    try {
      segment.appendToOpenSegment(entries);
      Assert.fail("should fail since there is gap between entries");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testTruncate() throws Exception {
    final long term = 1;
    final long start = 1000;
    LogSegment segment = LogSegment.newOpenSegment(start);
    for (int i = 0; i < 100; i++) {
      LogEntryProto entry = ProtoUtils.toLogEntryProto(
          new SimpleOperation("m" + i).getLogEntryContent(), term, i + start);
      segment.appendToOpenSegment(entry);
    }

    // truncate an open segment (remove 1080~1099)
    long newSize = segment.getLogRecord(start + 80).offset;
    segment.truncate(start + 80);
    Assert.assertEquals(80, segment.numOfEntries());
    checkLogSegment(segment, start, start + 79, false, newSize, term);

    // truncate a closed segment (remove 1050~1079)
    newSize = segment.getLogRecord(start + 50).offset;
    segment.truncate(start + 50);
    Assert.assertEquals(50, segment.numOfEntries());
    checkLogSegment(segment, start, start + 49, false, newSize, term);

    // truncate all the remaining entries
    segment.truncate(start);
    Assert.assertEquals(0, segment.numOfEntries());
    checkLogSegment(segment, start, start - 1, false,
        SegmentedRaftLog.HEADER_BYTES.length, term);
  }

  @Test
  public void testPreallocateSegment() throws Exception {
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    final File file = storage.getStorageDir().getOpenLogFile(0);
    final int[] maxSizes = new int[]{1024, 1025, 1024 * 1024 - 1, 1024 * 1024,
        1024 * 1024 + 1, 2 * 1024 * 1024 - 1, 2 * 1024 * 1024,
        2 * 1024 * 1024 + 1, 8 * 1024 * 1024};

    // make sure preallocation is correct with different max segment size
    for (int max : maxSizes) {
      try (LogOutputStream ignored = new LogOutputStream(file, false, max)) {
        Assert.assertEquals(file.length(), max);
      }
      try (LogInputStream in = new LogInputStream(file, 0,
          RaftServerConstants.INVALID_LOG_INDEX, true)) {
        LogEntryProto entry = in.nextEntry();
        Assert.assertNull(entry);
      }
    }

    // test the scenario where an entry's size is larger than the max size
    final byte[] content = new byte[1024 * 2];
    Arrays.fill(content, (byte) 1);
    final long size;
    try (LogOutputStream out = new LogOutputStream(file, false, 1024)) {
      SimpleOperation op = new SimpleOperation(new String(content));
      LogEntryProto entry = ProtoUtils.toLogEntryProto(op.getLogEntryContent(),
          0, 0);
      size = LogSegment.getEntrySize(entry);
      out.write(entry);
    }
    Assert.assertEquals(file.length(),
        size + SegmentedRaftLog.HEADER_BYTES.length);
    try (LogInputStream in = new LogInputStream(file, 0,
        RaftServerConstants.INVALID_LOG_INDEX, true)) {
      LogEntryProto entry = in.nextEntry();
      Assert.assertArrayEquals(content,
          entry.getSmLogEntry().getData().toByteArray());
      Assert.assertNull(in.nextEntry());
    }
  }
}
