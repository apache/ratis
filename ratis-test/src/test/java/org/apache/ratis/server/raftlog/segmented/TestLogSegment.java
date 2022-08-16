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
package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil.SimpleOperation;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.server.storage.RaftStorageTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ratis.server.raftlog.RaftLog.INVALID_LOG_INDEX;
import static org.apache.ratis.server.raftlog.segmented.LogSegment.getEntrySize;

import com.codahale.metrics.Timer;

/**
 * Test basic functionality of {@link LogSegment}
 */
public class TestLogSegment extends BaseTest {
  private File storageDir;
  private long segmentMaxSize;
  private long preallocatedSize;
  private int bufferSize;

  @Before
  public void setup() {
    RaftProperties properties = new RaftProperties();
    storageDir = getTestDir();
    RaftServerConfigKeys.setStorageDir(properties,  Collections.singletonList(storageDir));
    this.segmentMaxSize =
        RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.preallocatedSize =
        RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
    this.bufferSize =
        RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtils.deleteFully(storageDir.getParentFile());
    }
  }

  File prepareLog(boolean isOpen, long startIndex, int numEntries, long term, boolean isLastEntryPartiallyWritten)
      throws IOException {
    if (!isOpen) {
      Preconditions.assertTrue(!isLastEntryPartiallyWritten, "For closed log, the last entry cannot be partially written.");
    }
    RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final File file = LogSegmentStartEnd.valueOf(startIndex, startIndex + numEntries - 1, isOpen).getFile(storage);

    final LogEntryProto[] entries = new LogEntryProto[numEntries];
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(file, false,
        segmentMaxSize, preallocatedSize, ByteBuffer.allocateDirect(bufferSize))) {
      for (int i = 0; i < entries.length; i++) {
        SimpleOperation op = new SimpleOperation("m" + i);
        entries[i] = LogProtoUtils.toLogEntryProto(op.getLogEntryContent(), term, i + startIndex);
        out.write(entries[i]);
      }
    }

    if (isLastEntryPartiallyWritten) {
      final int entrySize = size(entries[entries.length - 1]);
      final int truncatedEntrySize = ThreadLocalRandom.current().nextInt(entrySize - 1) + 1;
      // 0 < truncatedEntrySize < entrySize
      final long fileLength = file.length();
      final long truncatedFileLength = fileLength - (entrySize - truncatedEntrySize);
      LOG.info("truncate last entry: entry(size={}, truncated={}), file(length={}, truncated={})",
          entrySize, truncatedEntrySize, fileLength, truncatedFileLength);
      FileUtils.truncateFile(file, truncatedFileLength);
    }

    storage.close();
    return file;
  }

  static int size(LogEntryProto entry) {
    final int n = entry.getSerializedSize();
    return CodedOutputStream.computeUInt32SizeNoTag(n) + n + 4;
  }

  static void checkLogSegment(LogSegment segment, long start, long end,
      boolean isOpen, long totalSize, long term) throws Exception {
    Assert.assertEquals(start, segment.getStartIndex());
    Assert.assertEquals(end, segment.getEndIndex());
    Assert.assertEquals(isOpen, segment.isOpen());
    Assert.assertEquals(totalSize, segment.getTotalFileSize());

    long offset = SegmentedRaftLogFormat.getHeaderLength();
    for (long i = start; i <= end; i++) {
      LogSegment.LogRecord record = segment.getLogRecord(i);
      final TermIndex ti = record.getTermIndex();
      Assert.assertEquals(i, ti.getIndex());
      Assert.assertEquals(term, ti.getTerm());
      Assert.assertEquals(offset, record.getOffset());

      LogEntryProto entry = segment.getEntryFromCache(ti);
      if (entry == null) {
        entry = segment.loadCache(record);
      }
      offset += getEntrySize(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
    }
  }

  @Test
  public void testLoadLogSegment() throws Exception {
    testLoadSegment(true, false);
  }

  @Test
  public void testLoadLogSegmentLastEntryPartiallyWritten() throws Exception {
    testLoadSegment(true, true);
  }

  @Test
  public void testLoadCache() throws Exception {
    testLoadSegment(false, false);
  }

  @Test
  public void testLoadCacheLastEntryPartiallyWritten() throws Exception {
    testLoadSegment(false, true);
  }

  private void testLoadSegment(boolean loadInitial, boolean isLastEntryPartiallyWritten) throws Exception {
    // load an open segment
    final File openSegmentFile = prepareLog(true, 0, 100, 0, isLastEntryPartiallyWritten);
    RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final LogSegment openSegment = LogSegment.loadSegment(storage, openSegmentFile,
        LogSegmentStartEnd.valueOf(0), loadInitial, null, null);
    final int delta = isLastEntryPartiallyWritten? 1: 0;
    checkLogSegment(openSegment, 0, 99 - delta, true, openSegmentFile.length(), 0);
    storage.close();
    // for open segment we currently always keep log entries in the memory
    Assert.assertEquals(0, openSegment.getLoadingTimes());

    // load a closed segment (1000-1099)
    final File closedSegmentFile = prepareLog(false, 1000, 100, 1, false);
    LogSegment closedSegment = LogSegment.loadSegment(storage, closedSegmentFile,
        LogSegmentStartEnd.valueOf(1000, 1099L), loadInitial, null, null);
    checkLogSegment(closedSegment, 1000, 1099, false,
        closedSegment.getTotalFileSize(), 1);
    Assert.assertEquals(loadInitial ? 0 : 1, closedSegment.getLoadingTimes());
  }

  @Test
  public void testAppendEntries() throws Exception {
    final long start = 1000;
    LogSegment segment = LogSegment.newOpenSegment(null, start, null);
    long size = SegmentedRaftLogFormat.getHeaderLength();
    final long max = 8 * 1024 * 1024;
    checkLogSegment(segment, start, start - 1, true, size, 0);

    // append till full
    long term = 0;
    int i = 0;
    while (size < max) {
      SimpleOperation op = new SimpleOperation("m" + i);
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(op.getLogEntryContent(), term, i++ + start);
      size += getEntrySize(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
      segment.appendToOpenSegment(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
    }

    Assert.assertTrue(segment.getTotalFileSize() >= max);
    checkLogSegment(segment, start, i - 1 + start, true, size, term);
  }

  @Test
  public void testAppendEntryMetric() throws Exception {
    final SegmentedRaftLogMetrics raftLogMetrics = new SegmentedRaftLogMetrics(RaftServerTestUtil.TEST_MEMBER_ID);

    final File openSegmentFile = prepareLog(true, 0, 100, 0, true);
    RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final LogSegment openSegment = LogSegment.loadSegment(storage, openSegmentFile,
        LogSegmentStartEnd.valueOf(0), true, null, raftLogMetrics);
    checkLogSegment(openSegment, 0, 98, true, openSegmentFile.length(), 0);
    storage.close();

    Timer readEntryTimer = raftLogMetrics.getRaftLogReadEntryTimer();
    Assert.assertNotNull(readEntryTimer);
    Assert.assertEquals(100, readEntryTimer.getCount());
    Assert.assertTrue(readEntryTimer.getMeanRate() > 0);
  }


  @Test
  public void testAppendWithGap() throws Exception {
    LogSegment segment = LogSegment.newOpenSegment(null, 1000, null);
    SimpleOperation op = new SimpleOperation("m");
    final StateMachineLogEntryProto m = op.getLogEntryContent();
    try {
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(m, 0, 1001);
      segment.appendToOpenSegment(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
      Assert.fail("should fail since the entry's index needs to be 1000");
    } catch (IllegalStateException e) {
      // the exception is expected.
    }

    LogEntryProto entry = LogProtoUtils.toLogEntryProto(m, 0, 1000);
    segment.appendToOpenSegment(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);

    try {
      entry = LogProtoUtils.toLogEntryProto(m, 0, 1002);
      segment.appendToOpenSegment(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
      Assert.fail("should fail since the entry's index needs to be 1001");
    } catch (IllegalStateException e) {
      // the exception is expected.
    }
  }

  @Test
  public void testTruncate() throws Exception {
    final long term = 1;
    final long start = 1000;
    LogSegment segment = LogSegment.newOpenSegment(null, start, null);
    for (int i = 0; i < 100; i++) {
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(
          new SimpleOperation("m" + i).getLogEntryContent(), term, i + start);
      segment.appendToOpenSegment(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
    }

    // truncate an open segment (remove 1080~1099)
    long newSize = segment.getLogRecord(start + 80).getOffset();
    segment.truncate(start + 80);
    Assert.assertEquals(80, segment.numOfEntries());
    checkLogSegment(segment, start, start + 79, false, newSize, term);

    // truncate a closed segment (remove 1050~1079)
    newSize = segment.getLogRecord(start + 50).getOffset();
    segment.truncate(start + 50);
    Assert.assertEquals(50, segment.numOfEntries());
    checkLogSegment(segment, start, start + 49, false, newSize, term);

    // truncate all the remaining entries
    segment.truncate(start);
    Assert.assertEquals(0, segment.numOfEntries());
    checkLogSegment(segment, start, start - 1, false,
        SegmentedRaftLogFormat.getHeaderLength(), term);
  }

  @Test
  public void testPreallocateSegment() throws Exception {
    RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final File file = LogSegmentStartEnd.valueOf(0).getFile(storage);
    final int[] maxSizes = new int[]{1024, 1025, 1024 * 1024 - 1, 1024 * 1024,
        1024 * 1024 + 1, 2 * 1024 * 1024 - 1, 2 * 1024 * 1024,
        2 * 1024 * 1024 + 1, 8 * 1024 * 1024};
    final int[] preallocated = new int[]{512, 1024, 1025, 1024 * 1024,
        1024 * 1024 + 1, 2 * 1024 * 1024};

    // make sure preallocation is correct with different max/pre-allocated size
    for (int max : maxSizes) {
      for (int a : preallocated) {
        try(SegmentedRaftLogOutputStream ignored = new SegmentedRaftLogOutputStream(file, false, max, a, ByteBuffer.allocateDirect(bufferSize))) {
          Assert.assertEquals("max=" + max + ", a=" + a, file.length(), Math.min(max, a));
        }
        try(SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(file, 0, INVALID_LOG_INDEX, true)) {
          LogEntryProto entry = in.nextEntry();
          Assert.assertNull(entry);
        }
      }
    }

    // test the scenario where an entry's size is larger than the max size
    final byte[] content = new byte[1024 * 2];
    Arrays.fill(content, (byte) 1);
    final long size;
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(file, false,
        1024, 1024, ByteBuffer.allocateDirect(bufferSize))) {
      SimpleOperation op = new SimpleOperation(new String(content));
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(op.getLogEntryContent(), 0, 0);
      size = LogSegment.getEntrySize(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
      out.write(entry);
    }
    Assert.assertEquals(file.length(),
        size + SegmentedRaftLogFormat.getHeaderLength());
    try (SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(file, 0,
        INVALID_LOG_INDEX, true)) {
      LogEntryProto entry = in.nextEntry();
      Assert.assertArrayEquals(content,
          entry.getStateMachineLogEntry().getLogData().toByteArray());
      Assert.assertNull(in.nextEntry());
    }
  }

  /**
   * Keep appending and check if pre-allocation is correct
   */
  @Test
  public void testPreallocationAndAppend() throws Exception {
    final SizeInBytes max = SizeInBytes.valueOf(2, TraditionalBinaryPrefix.MEGA);
    RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final File file = LogSegmentStartEnd.valueOf(0).getFile(storage);

    final byte[] content = new byte[1024];
    Arrays.fill(content, (byte) 1);
    SimpleOperation op = new SimpleOperation(new String(content));
    LogEntryProto entry = LogProtoUtils.toLogEntryProto(op.getLogEntryContent(), 0, 0);
    final long entrySize = LogSegment.getEntrySize(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);

    long totalSize = SegmentedRaftLogFormat.getHeaderLength();
    long preallocated = 16 * 1024;
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(file, false,
        max.getSize(), 16 * 1024, ByteBuffer.allocateDirect(10 * 1024))) {
      Assert.assertEquals(preallocated, file.length());
      while (totalSize + entrySize < max.getSize()) {
        totalSize += entrySize;
        out.write(entry);
        if (totalSize > preallocated) {
          Assert.assertEquals("totalSize==" + totalSize,
              preallocated + 16 * 1024, file.length());
          preallocated += 16 * 1024;
        }
      }
    }

    Assert.assertEquals(totalSize, file.length());
  }

  @Test
  public void testZeroSizeInProgressFile() throws Exception {
    final RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final File file = LogSegmentStartEnd.valueOf(0).getFile(storage);
    storage.close();

    // create zero size in-progress file
    LOG.info("file: " + file);
    Assert.assertTrue(file.createNewFile());
    final Path path = file.toPath();
    Assert.assertTrue(Files.exists(path));
    Assert.assertEquals(0, Files.size(path));

    // getLogSegmentPaths should remove it.
    final List<LogSegmentPath> logs = LogSegmentPath.getLogSegmentPaths(storage);
    Assert.assertEquals(0, logs.size());
    Assert.assertFalse(Files.exists(path));
  }
}
