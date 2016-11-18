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

import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.RaftTestUtil.SimpleOperation;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.RaftServerConstants.StartupOption;
import org.apache.raft.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.raft.util.ProtoUtils;
import org.apache.raft.util.RaftUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.raft.server.RaftServerConfigKeys.RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT;

/**
 * Test basic functionality of LogReader, LogInputStream, and LogOutputStream.
 */
public class TestRaftLogReadWrite {
  private static final Logger LOG = LoggerFactory.getLogger(TestRaftLogReadWrite.class);

  private File storageDir;
  private final RaftProperties properties = new RaftProperties();
  private int segmentMaxSize;

  @Before
  public void setup() throws Exception {
    storageDir = RaftTestUtil.getTestDir(TestRaftLogReadWrite.class);
    properties.set(RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_KEY,
        RaftUtils.fileAsURI(storageDir).toString());
    segmentMaxSize = properties.getInt(
        RaftServerConfigKeys.RAFT_LOG_SEGMENT_MAX_SIZE_KEY,
        RaftServerConfigKeys.RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT);
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtil.fullyDelete(storageDir.getParentFile());
    }
  }

  private LogEntryProto[] readLog(File file, long startIndex, long endIndex,
      boolean isOpen) throws IOException {
    List<LogEntryProto> list = new ArrayList<>();
    try (LogInputStream in =
             new LogInputStream(file, startIndex, endIndex, isOpen)) {
      LogEntryProto entry;
      while ((entry = in.nextEntry()) != null) {
        list.add(entry);
      }
    }
    return list.toArray(new LogEntryProto[list.size()]);
  }

  private long writeMessages(LogEntryProto[] entries, LogOutputStream out)
      throws IOException {
    long size = 0;
    for (int i = 0; i < entries.length; i++) {
      SimpleOperation m = new SimpleOperation("m" + i);
      entries[i] = ProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
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
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File openSegment = storage.getStorageDir().getOpenLogFile(0);
    long size = SegmentedRaftLog.HEADER_BYTES.length;

    final LogEntryProto[] entries = new LogEntryProto[100];
    try (LogOutputStream out =
             new LogOutputStream(openSegment, false, segmentMaxSize)) {
      size += writeMessages(entries, out);
    } finally {
      storage.close();
    }

    Assert.assertEquals(size, openSegment.length());

    LogEntryProto[] readEntries = readLog(openSegment, 0,
        RaftServerConstants.INVALID_LOG_INDEX, true);
    Assert.assertArrayEquals(entries, readEntries);
  }

  @Test
  public void testAppendLog() throws IOException {
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File openSegment = storage.getStorageDir().getOpenLogFile(0);
    LogEntryProto[] entries = new LogEntryProto[200];
    try (LogOutputStream out =
             new LogOutputStream(openSegment, false, segmentMaxSize)) {
      for (int i = 0; i < 100; i++) {
        SimpleOperation m = new SimpleOperation("m" + i);
        entries[i] = ProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
        out.write(entries[i]);
      }
    }

    try (LogOutputStream out =
             new LogOutputStream(openSegment, true, segmentMaxSize)) {
      for (int i = 100; i < 200; i++) {
        SimpleOperation m = new SimpleOperation("m" + i);
        entries[i] = ProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
        out.write(entries[i]);
      }
    }

    LogEntryProto[] readEntries = readLog(openSegment, 0,
        RaftServerConstants.INVALID_LOG_INDEX, true);
    Assert.assertArrayEquals(entries, readEntries);

    storage.close();
  }

  /**
   * Simulate the scenario that the peer is shutdown without truncating
   * log segment file padding. Make sure the reader can correctly handle this.
   */
  @Test
  public void testReadWithPadding() throws IOException {
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File openSegment = storage.getStorageDir().getOpenLogFile(0);
    long size = SegmentedRaftLog.HEADER_BYTES.length;

    LogEntryProto[] entries = new LogEntryProto[100];
    LogOutputStream out = new LogOutputStream(openSegment, false, segmentMaxSize);
    size += writeMessages(entries, out);
    out.flush();

    // make sure the file contains padding
    Assert.assertEquals(RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT, openSegment.length());

    // check if the reader can correctly read the log file
    LogEntryProto[] readEntries = readLog(openSegment, 0,
        RaftServerConstants.INVALID_LOG_INDEX, true);
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
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File openSegment = storage.getStorageDir().getOpenLogFile(0);

    LogEntryProto[] entries = new LogEntryProto[10];
    LogOutputStream out = new LogOutputStream(openSegment, false, segmentMaxSize);
    for (int i = 0; i < 10; i++) {
      SimpleOperation m = new SimpleOperation("m" + i);
      entries[i] = ProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
      out.write(entries[i]);
    }
    out.flush();

    // make sure the file contains padding
    Assert.assertEquals(RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT, openSegment.length());

    try (FileOutputStream fout = new FileOutputStream(openSegment, true)) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{-1, 1});
      fout.getChannel()
          .write(byteBuffer, RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT - 10);
    }

    List<LogEntryProto> list = new ArrayList<>();
    try (LogInputStream in = new LogInputStream(openSegment, 0,
        RaftServerConstants.INVALID_LOG_INDEX, true)) {
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
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File openSegment = storage.getStorageDir().getOpenLogFile(0);
    try (LogOutputStream out =
             new LogOutputStream(openSegment, false, segmentMaxSize)) {
      for (int i = 0; i < 100; i++) {
        LogEntryProto entry = ProtoUtils.toLogEntryProto(
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
      readLog(openSegment, 0, RaftServerConstants.INVALID_LOG_INDEX, true);
      Assert.fail("The read of corrupted log file should fail");
    } catch (ChecksumException e) {
      LOG.info("Caught ChecksumException as expected", e);
    }
  }
}
