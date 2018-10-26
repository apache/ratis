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

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil.SimpleOperation;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerConstants.StartupOption;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.storage.LogSegment.LogRecordWithEntry;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ratis.server.impl.RaftServerConstants.INVALID_LOG_INDEX;
import static org.apache.ratis.server.storage.LogSegment.getEntrySize;

/**
 * Test basic functionality of {@link LogSegment}
 */
public class TestRaftLogSegment extends BaseTest {
  private File storageDir;
  private long segmentMaxSize;
  private long preallocatedSize;
  private int bufferSize;

  @Before
  public void setup() throws Exception {
    RaftProperties properties = new RaftProperties();
    storageDir = getTestDir();
    RaftServerConfigKeys.setStorageDirs(properties,  Collections.singletonList(storageDir));
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
    RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
    final File file = isOpen ?
        storage.getStorageDir().getOpenLogFile(startIndex) :
        storage.getStorageDir().getClosedLogFile(startIndex, startIndex + numEntries - 1);

    final LogEntryProto[] entries = new LogEntryProto[numEntries];
    try (LogOutputStream out = new LogOutputStream(file, false,
        segmentMaxSize, preallocatedSize, bufferSize)) {
      for (int i = 0; i < entries.length; i++) {
        SimpleOperation op = new SimpleOperation("m" + i);
        entries[i] = ServerProtoUtils.toLogEntryProto(op.getLogEntryContent(), term, i + startIndex);
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
    Assert.assertEquals(totalSize, segment.getTotalSize());

    long offset = SegmentedRaftLog.HEADER_BYTES.length;
    for (long i = start; i <= end; i++) {
      LogSegment.LogRecord record = segment.getLogRecord(i);
      LogRecordWithEntry lre = segment.getEntryWithoutLoading(i);
      Assert.assertEquals(i, lre.getRecord().getTermIndex().getIndex());
      Assert.assertEquals(term, lre.getRecord().getTermIndex().getTerm());
      Assert.assertEquals(offset, record.getOffset());

      LogEntryProto entry = lre.hasEntry() ?
          lre.getEntry() : segment.loadCache(lre.getRecord());
      offset += getEntrySize(entry);
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
    RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
    LogSegment openSegment = LogSegment.loadSegment(storage, openSegmentFile, 0,
        INVALID_LOG_INDEX, true, loadInitial, null);
    final int delta = isLastEntryPartiallyWritten? 1: 0;
    checkLogSegment(openSegment, 0, 99 - delta, true, openSegmentFile.length(), 0);
    storage.close();
    // for open segment we currently always keep log entries in the memory
    Assert.assertEquals(0, openSegment.getLoadingTimes());

    // load a closed segment (1000-1099)
    final File closedSegmentFile = prepareLog(false, 1000, 100, 1, false);
    LogSegment closedSegment = LogSegment.loadSegment(storage, closedSegmentFile,
        1000, 1099, false, loadInitial, null);
    checkLogSegment(closedSegment, 1000, 1099, false,
        closedSegment.getTotalSize(), 1);
    Assert.assertEquals(loadInitial ? 0 : 1, closedSegment.getLoadingTimes());
  }

  @Test
  public void testAppendEntries() throws Exception {
    final long start = 1000;
    LogSegment segment = LogSegment.newOpenSegment(null, start);
    long size = SegmentedRaftLog.HEADER_BYTES.length;
    final long max = 8 * 1024 * 1024;
    checkLogSegment(segment, start, start - 1, true, size, 0);

    // append till full
    long term = 0;
    int i = 0;
    List<LogEntryProto> list = new ArrayList<>();
    while (size < max) {
      SimpleOperation op = new SimpleOperation("m" + i);
      LogEntryProto entry = ServerProtoUtils.toLogEntryProto(op.getLogEntryContent(), term, i++ + start);
      size += getEntrySize(entry);
      list.add(entry);
    }

    segment.appendToOpenSegment(list.toArray(new LogEntryProto[list.size()]));
    Assert.assertTrue(segment.getTotalSize() >= max);
    checkLogSegment(segment, start, i - 1 + start, true, size, term);
  }

  @Test
  public void testAppendWithGap() throws Exception {
    LogSegment segment = LogSegment.newOpenSegment(null, 1000);
    SimpleOperation op = new SimpleOperation("m");
    final StateMachineLogEntryProto m = op.getLogEntryContent();
    try {
      LogEntryProto entry = ServerProtoUtils.toLogEntryProto(m, 0, 1001);
      segment.appendToOpenSegment(entry);
      Assert.fail("should fail since the entry's index needs to be 1000");
    } catch (IllegalStateException e) {
      // the exception is expected.
    }

    LogEntryProto entry = ServerProtoUtils.toLogEntryProto(m, 0, 1000);
    segment.appendToOpenSegment(entry);

    try {
      entry = ServerProtoUtils.toLogEntryProto(m, 0, 1002);
      segment.appendToOpenSegment(entry);
      Assert.fail("should fail since the entry's index needs to be 1001");
    } catch (IllegalStateException e) {
      // the exception is expected.
    }

    LogEntryProto[] entries = new LogEntryProto[2];
    for (int i = 0; i < 2; i++) {
      entries[i] = ServerProtoUtils.toLogEntryProto(m, 0, 1001 + i * 2);
    }
    try {
      segment.appendToOpenSegment(entries);
      Assert.fail("should fail since there is gap between entries");
    } catch (IllegalStateException e) {
      // the exception is expected.
    }
  }

  @Test
  public void testTruncate() throws Exception {
    final long term = 1;
    final long start = 1000;
    LogSegment segment = LogSegment.newOpenSegment(null, start);
    for (int i = 0; i < 100; i++) {
      LogEntryProto entry = ServerProtoUtils.toLogEntryProto(
          new SimpleOperation("m" + i).getLogEntryContent(), term, i + start);
      segment.appendToOpenSegment(entry);
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
        SegmentedRaftLog.HEADER_BYTES.length, term);
  }

  @Test
  public void testPreallocateSegment() throws Exception {
    RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
    final File file = storage.getStorageDir().getOpenLogFile(0);
    final int[] maxSizes = new int[]{1024, 1025, 1024 * 1024 - 1, 1024 * 1024,
        1024 * 1024 + 1, 2 * 1024 * 1024 - 1, 2 * 1024 * 1024,
        2 * 1024 * 1024 + 1, 8 * 1024 * 1024};
    final int[] preallocated = new int[]{512, 1024, 1025, 1024 * 1024,
        1024 * 1024 + 1, 2 * 1024 * 1024};

    // make sure preallocation is correct with different max/pre-allocated size
    for (int max : maxSizes) {
      for (int a : preallocated) {
        try (LogOutputStream ignored =
                 new LogOutputStream(file, false, max, a, bufferSize)) {
          Assert.assertEquals("max=" + max + ", a=" + a, file.length(), Math.min(max, a));
        }
        try (LogInputStream in =
                 new LogInputStream(file, 0, INVALID_LOG_INDEX, true)) {
          LogEntryProto entry = in.nextEntry();
          Assert.assertNull(entry);
        }
      }
    }

    // test the scenario where an entry's size is larger than the max size
    final byte[] content = new byte[1024 * 2];
    Arrays.fill(content, (byte) 1);
    final long size;
    try (LogOutputStream out = new LogOutputStream(file, false,
        1024, 1024, bufferSize)) {
      SimpleOperation op = new SimpleOperation(new String(content));
      LogEntryProto entry = ServerProtoUtils.toLogEntryProto(op.getLogEntryContent(), 0, 0);
      size = LogSegment.getEntrySize(entry);
      out.write(entry);
    }
    Assert.assertEquals(file.length(),
        size + SegmentedRaftLog.HEADER_BYTES.length);
    try (LogInputStream in = new LogInputStream(file, 0,
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
    RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
    final File file = storage.getStorageDir().getOpenLogFile(0);

    final byte[] content = new byte[1024];
    Arrays.fill(content, (byte) 1);
    SimpleOperation op = new SimpleOperation(new String(content));
    LogEntryProto entry = ServerProtoUtils.toLogEntryProto(op.getLogEntryContent(), 0, 0);
    final long entrySize = LogSegment.getEntrySize(entry);

    long totalSize = SegmentedRaftLog.HEADER_BYTES.length;
    long preallocated = 16 * 1024;
    try (LogOutputStream out = new LogOutputStream(file, false,
        max.getSize(), 16 * 1024, 10 * 1024)) {
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
    final RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
    final File file = storage.getStorageDir().getOpenLogFile(0);
    storage.close();

    // create zero size in-progress file
    LOG.info("file: " + file);
    Assert.assertTrue(file.createNewFile());
    final Path path = file.toPath();
    Assert.assertTrue(Files.exists(path));
    Assert.assertEquals(0, Files.size(path));

    // getLogSegmentFiles should remove it.
    final List<RaftStorageDirectory.LogPathAndIndex> logs = storage.getStorageDir().getLogSegmentFiles();
    Assert.assertEquals(0, logs.size());
    Assert.assertFalse(Files.exists(path));
  }
}
