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
import org.apache.ratis.protocol.exceptions.ChecksumException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test basic functionality of LogReader, SegmentedRaftLogInputStream, and SegmentedRaftLogOutputStream.
 */
public class TestRaftLogReadWrite extends BaseTest {
  private File storageDir;
  private long segmentMaxSize;
  private long preallocatedSize;
  private int bufferSize;

  @Before
  public void setup() {
    storageDir = getTestDir();
    RaftProperties properties = new RaftProperties();
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

  private LogEntryProto[] readLog(File file, long startIndex, long endIndex,
      boolean isOpen) throws IOException {
    List<LogEntryProto> list = new ArrayList<>();
    try (SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(file, startIndex, endIndex, isOpen)) {
      LogEntryProto entry;
      while ((entry = in.nextEntry()) != null) {
        list.add(entry);
      }
    }
    return list.toArray(new LogEntryProto[list.size()]);
  }

  private long writeMessages(LogEntryProto[] entries, SegmentedRaftLogOutputStream out)
      throws IOException {
    long size = 0;
    for (int i = 0; i < entries.length; i++) {
      SimpleOperation m = new SimpleOperation("m" + i);
      entries[i] = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
      final int s = entries[i].getSerializedSize();
      size += CodedOutputStream.computeUInt32SizeNoTag(s) + s + 4;
      out.write(entries[i]);
    }
    return size;
  }

  /**
   * Test basic functionality: write several log entries, then read
   */
  @Test
  public void testReadWriteLog() throws IOException {
    final RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final File openSegment = LogSegmentStartEnd.valueOf(0).getFile(storage);
    long size = SegmentedRaftLogFormat.getHeaderLength();

    final LogEntryProto[] entries = new LogEntryProto[100];
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(openSegment, false,
        segmentMaxSize, preallocatedSize, ByteBuffer.allocateDirect(bufferSize))) {
      size += writeMessages(entries, out);
    } finally {
      storage.close();
    }

    Assert.assertEquals(size, openSegment.length());

    final LogEntryProto[] readEntries = readLog(openSegment, 0, RaftLog.INVALID_LOG_INDEX, true);
    Assert.assertArrayEquals(entries, readEntries);
  }

  @Test
  public void testAppendLog() throws IOException {
    final RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final File openSegment = LogSegmentStartEnd.valueOf(0).getFile(storage);
    LogEntryProto[] entries = new LogEntryProto[200];
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(openSegment, false,
        segmentMaxSize, preallocatedSize, ByteBuffer.allocateDirect(bufferSize))) {
      for (int i = 0; i < 100; i++) {
        SimpleOperation m = new SimpleOperation("m" + i);
        entries[i] = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
        out.write(entries[i]);
      }
    }

    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(openSegment, true,
        segmentMaxSize, preallocatedSize, ByteBuffer.allocateDirect(bufferSize))) {
      for (int i = 100; i < 200; i++) {
        SimpleOperation m = new SimpleOperation("m" + i);
        entries[i] = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
        out.write(entries[i]);
      }
    }

    final LogEntryProto[] readEntries = readLog(openSegment, 0, RaftLog.INVALID_LOG_INDEX, true);
    Assert.assertArrayEquals(entries, readEntries);

    storage.close();
  }

  /**
   * Simulate the scenario that the peer is shutdown without truncating
   * log segment file padding. Make sure the reader can correctly handle this.
   */
  @Test
  public void testReadWithPadding() throws IOException {
    final RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final File openSegment = LogSegmentStartEnd.valueOf(0).getFile(storage);
    long size = SegmentedRaftLogFormat.getHeaderLength();

    LogEntryProto[] entries = new LogEntryProto[100];
    final SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(openSegment, false,
        segmentMaxSize, preallocatedSize, ByteBuffer.allocateDirect(bufferSize));
    size += writeMessages(entries, out);
    out.flush();

    // make sure the file contains padding
    Assert.assertEquals(
        RaftServerConfigKeys.Log.PREALLOCATED_SIZE_DEFAULT.getSize(),
        openSegment.length());

    // check if the reader can correctly read the log file
    final LogEntryProto[] readEntries = readLog(openSegment, 0, RaftLog.INVALID_LOG_INDEX, true);
    Assert.assertArrayEquals(entries, readEntries);

    out.close();
    Assert.assertEquals(size, openSegment.length());
  }

  /**
   * corrupt the padding by inserting non-zero bytes. Make sure the reader
   * throws exception.
   */
  @Test
  public void testReadWithCorruptPadding() throws IOException {
    final RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final File openSegment = LogSegmentStartEnd.valueOf(0).getFile(storage);

    LogEntryProto[] entries = new LogEntryProto[10];
    final SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(openSegment, false,
        16 * 1024 * 1024, 4 * 1024 * 1024, ByteBuffer.allocateDirect(bufferSize));
    for (int i = 0; i < 10; i++) {
      SimpleOperation m = new SimpleOperation("m" + i);
      entries[i] = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
      out.write(entries[i]);
    }
    out.flush();

    // make sure the file contains padding
    Assert.assertEquals(4 * 1024 * 1024, openSegment.length());

    try (FileOutputStream fout = new FileOutputStream(openSegment, true)) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{-1, 1});
      fout.getChannel()
          .write(byteBuffer, 16 * 1024 * 1024 - 10);
    }

    List<LogEntryProto> list = new ArrayList<>();
    try (SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(openSegment, 0,
        RaftLog.INVALID_LOG_INDEX, true)) {
      LogEntryProto entry;
      while ((entry = in.nextEntry()) != null) {
        list.add(entry);
      }
      Assert.fail("should fail since we corrupt the padding");
    } catch (IOException e) {
      boolean findVerifyTerminator = false;
      for (StackTraceElement s : e.getStackTrace()) {
        if (s.getMethodName().equals("verifyTerminator")) {
          findVerifyTerminator = true;
          break;
        }
      }
      Assert.assertTrue(findVerifyTerminator);
    }
    Assert.assertArrayEquals(entries,
        list.toArray(new LogEntryProto[list.size()]));
  }

  /**
   * Test the log reader to make sure it can detect the checksum mismatch.
   */
  @Test
  public void testReadWithEntryCorruption() throws IOException {
    RaftStorage storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    final File openSegment = LogSegmentStartEnd.valueOf(0).getFile(storage);
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(openSegment, false,
        segmentMaxSize, preallocatedSize, ByteBuffer.allocateDirect(bufferSize))) {
      for (int i = 0; i < 100; i++) {
        LogEntryProto entry = LogProtoUtils.toLogEntryProto(
            new SimpleOperation("m" + i).getLogEntryContent(), 0, i);
        out.write(entry);
      }
    } finally {
      storage.close();
    }

    // corrupt the log file
    try (RandomAccessFile raf = new RandomAccessFile(openSegment.getCanonicalFile(),
        "rw")) {
      raf.seek(100);
      int correctValue = raf.read();
      raf.seek(100);
      raf.write(correctValue + 1);
    }

    try {
      readLog(openSegment, 0, RaftLog.INVALID_LOG_INDEX, true);
      Assert.fail("The read of corrupted log file should fail");
    } catch (ChecksumException e) {
      LOG.info("Caught ChecksumException as expected", e);
    }
  }
}
