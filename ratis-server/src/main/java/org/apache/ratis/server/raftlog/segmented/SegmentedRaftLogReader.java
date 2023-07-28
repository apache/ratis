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

import org.apache.ratis.io.CorruptedFileException;
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.protocol.exceptions.ChecksumException;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.PureJavaCrc32C;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Optional;
import java.util.zip.Checksum;

class SegmentedRaftLogReader implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogReader.class);
  /**
   * InputStream wrapper that keeps track of the current stream position.
   *
   * This stream also allows us to set a limit on how many bytes we can read
   * without getting an exception.
   */
  static class LimitedInputStream extends FilterInputStream {
    private long curPos = 0;
    private volatile long markPos = -1;
    private long limitPos = Long.MAX_VALUE;

    LimitedInputStream(InputStream is) {
      super(is);
    }

    private void checkLimit(long amt) throws IOException {
      long extra = (curPos + amt) - limitPos;
      if (extra > 0) {
        throw new IOException("Tried to read " + amt + " byte(s) past " +
            "the limit at offset " + limitPos);
      }
    }

    @Override
    public int read() throws IOException {
      checkLimit(1);
      int ret = super.read();
      if (ret != -1) {
        curPos++;
      }
      return ret;
    }

    @Override
    public int read(byte[] data) throws IOException {
      checkLimit(data.length);
      int ret = super.read(data);
      if (ret > 0) {
        curPos += ret;
      }
      return ret;
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      checkLimit(length);
      int ret = super.read(data, offset, length);
      if (ret > 0) {
        curPos += ret;
      }
      return ret;
    }

    public void setLimit(long limit) {
      limitPos = curPos + limit;
    }

    public void clearLimit() {
      limitPos = Long.MAX_VALUE;
    }

    @Override
    public synchronized void mark(int limit) {
      super.mark(limit);
      markPos = curPos;
    }

    @Override
    public synchronized void reset() throws IOException {
      if (markPos == -1) {
        throw new IOException("Not marked!");
      }
      super.reset();
      curPos = markPos;
      markPos = -1;
    }

    public long getPos() {
      return curPos;
    }

    @Override
    public long skip(long amt) throws IOException {
      long extra = (curPos + amt) - limitPos;
      if (extra > 0) {
        throw new IOException("Tried to skip " + extra + " bytes past " +
            "the limit at offset " + limitPos);
      }
      long ret = super.skip(amt);
      curPos += ret;
      return ret;
    }
  }

  private final File file;
  private final LimitedInputStream limiter;
  private final DataInputStream in;
  private byte[] temp = new byte[4096];
  private final Checksum checksum;
  private final SegmentedRaftLogMetrics raftLogMetrics;
  private final SizeInBytes maxOpSize;

  SegmentedRaftLogReader(File file, SizeInBytes maxOpSize, SegmentedRaftLogMetrics raftLogMetrics)
      throws FileNotFoundException {
    this.file = file;
    this.limiter = new LimitedInputStream(
        new BufferedInputStream(new FileInputStream(file)));
    in = new DataInputStream(limiter);
    checksum = new PureJavaCrc32C();
    this.maxOpSize = maxOpSize;
    this.raftLogMetrics = raftLogMetrics;
  }

  /**
   * Read header from the log file:
   *
   * (1) The header in file is verified successfully.
   *     Then, return true.
   *
   * (2) The header in file is partially written.
   *     Then, return false.
   *
   * (3) The header in file is corrupted or there is some other {@link IOException}.
   *     Then, throw an exception.
   */
  boolean verifyHeader() throws IOException {
    final int headerLength = SegmentedRaftLogFormat.getHeaderLength();
    final int readLength = in.read(temp, 0, headerLength);
    Preconditions.assertTrue(readLength <= headerLength);
    final int matchLength = SegmentedRaftLogFormat.matchHeader(temp, 0, readLength);
    Preconditions.assertTrue(matchLength <= readLength);

    if (readLength == headerLength && matchLength == readLength) {
      // The header is matched successfully
      return true;
    } else if (SegmentedRaftLogFormat.isTerminator(temp, matchLength, readLength - matchLength)) {
      // The header is partially written
      return false;
    }
    // The header is corrupted
    throw new CorruptedFileException(file, "Log header mismatched: expected header length="
        + SegmentedRaftLogFormat.getHeaderLength() + ", read length=" + readLength + ", match length=" + matchLength
        + ", header in file=" + StringUtils.bytes2HexString(temp, 0, readLength)
        + ", expected header=" + SegmentedRaftLogFormat.applyHeaderTo(StringUtils::bytes2HexString));
  }

  /**
   * Read a log entry from the input stream.
   *
   * @return the operation read from the stream, or null at the end of the
   *         file
   * @throws IOException on error.  This function should only throw an
   *         exception when skipBrokenEdits is false.
   */
  LogEntryProto readEntry() throws IOException {
    final Timekeeper timekeeper = Optional.ofNullable(raftLogMetrics)
        .map(SegmentedRaftLogMetrics::getReadEntryTimer)
        .orElse(null);
    try(AutoCloseable readEntryContext = Timekeeper.start(timekeeper)) {
      return decodeEntry();
    } catch (EOFException eof) {
      in.reset();
      // The last entry is partially written.
      // It is okay to ignore it since this entry is never committed in this server.
      if (LOG.isWarnEnabled()) {
        LOG.warn("Ignoring the last partial written log entry in " + file + ": " + eof);
      } else if (LOG.isTraceEnabled()) {
        LOG.trace("Ignoring the last partial written log entry in " + file , eof);
      }
      return null;
    } catch (IOException e) {
      in.reset();

      throw e;
    } catch (Exception e) {
      // raft log requires no gap between any two entries. thus if an entry is
      // broken, throw the exception instead of skipping broken entries
      in.reset();
      throw new IOException("got unexpected exception " + e.getMessage(), e);
    }
  }

  /**
   * Scan and validate a log entry.
   * @return the index of the log entry
   */
  long scanEntry() throws IOException {
    return Optional.ofNullable(decodeEntry()).map(LogEntryProto::getIndex).orElse(RaftLog.INVALID_LOG_INDEX);
  }

  void verifyTerminator() throws IOException {
     // The end of the log should contain 0x00 bytes.
     // If it contains other bytes, the log itself may be corrupt.
    limiter.clearLimit();
    int numRead = -1, idx = 0;
    while (true) {
      try {
        numRead = in.read(temp);
        if (numRead == -1) {
          return;
        }
        for (idx = 0; idx < numRead; idx++) {
          if (!SegmentedRaftLogFormat.isTerminator(temp[idx])) {
            throw new IOException("Read extra bytes after the terminator!");
          }
        }
      } finally {
        // After reading each group of bytes, we reposition the mark one
        // byte before the next group. Similarly, if there is an error, we
        // want to reposition the mark one byte before the error
        if (numRead != -1) {
          in.reset();
          IOUtils.skipFully(in, idx);
          in.mark(temp.length + 1);
          IOUtils.skipFully(in, 1);
        }
      }
    }
  }

  /**
   * Decode the log entry "frame". This includes reading the log entry, and
   * validating the checksum.
   *
   * The input stream will be advanced to the end of the op at the end of this
   * function.
   *
   * @return The log entry, or null if we hit EOF.
   */
  private LogEntryProto decodeEntry() throws IOException {
    final int max = maxOpSize.getSizeInt();
    limiter.setLimit(max);
    in.mark(max);

    byte nextByte;
    try {
      nextByte = in.readByte();
    } catch (EOFException eof) {
      // EOF at an opcode boundary is expected.
      return null;
    }
    // Each log entry starts with a var-int. Thus a valid entry's first byte
    // should not be 0. So if the terminate byte is 0, we should hit the end
    // of the segment.
    if (SegmentedRaftLogFormat.isTerminator(nextByte)) {
      verifyTerminator();
      return null;
    }

    // Here, we verify that the Op size makes sense and that the
    // data matches its checksum before attempting to construct an Op.
    int entryLength = CodedInputStream.readRawVarint32(nextByte, in);
    if (entryLength > max) {
      throw new IOException("Entry has size " + entryLength
          + ", but MAX_OP_SIZE = " + maxOpSize);
    }

    final int varintLength = CodedOutputStream.computeUInt32SizeNoTag(
        entryLength);
    final int totalLength = varintLength + entryLength;
    checkBufferSize(totalLength, max);
    in.reset();
    in.mark(max);
    IOUtils.readFully(in, temp, 0, totalLength);

    // verify checksum
    checksum.reset();
    checksum.update(temp, 0, totalLength);
    int expectedChecksum = in.readInt();
    int calculatedChecksum = (int) checksum.getValue();
    if (expectedChecksum != calculatedChecksum) {
      final String s = StringUtils.format("Log entry corrupted: Calculated checksum is %08X but read checksum is %08X.",
          calculatedChecksum, expectedChecksum);
      throw new ChecksumException(s, limiter.markPos);
    }

    // parse the buffer
    return LogEntryProto.parseFrom(
        CodedInputStream.newInstance(temp, varintLength, entryLength));
  }

  private void checkBufferSize(int entryLength, int max) {
    Preconditions.assertTrue(entryLength <= max);
    int length = temp.length;
    if (length < entryLength) {
      while (length < entryLength) {
        length = Math.min(length * 2, max);
      }
      temp = new byte[length];
    }
  }

  long getPos() {
    return limiter.getPos();
  }

  void skipFully(long length) throws IOException {
    limiter.clearLimit();
    IOUtils.skipFully(limiter, length);
  }

  @Override
  public void close() {
    IOUtils.cleanup(LOG, in);
  }
}
