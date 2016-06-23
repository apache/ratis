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

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.util.DataChecksum;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

public class LogReader {
  /**
   * InputStream wrapper that keeps track of the current stream position.
   *
   * This stream also allows us to set a limit on how many bytes we can read
   * without getting an exception.
   */
  public static class LimitedInputStream extends FilterInputStream {
    private long curPos = 0;
    private long markPos = -1;
    private long limitPos = Long.MAX_VALUE;

    public LimitedInputStream(InputStream is) {
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
      if (ret != -1) curPos++;
      return ret;
    }

    @Override
    public int read(byte[] data) throws IOException {
      checkLimit(data.length);
      int ret = super.read(data);
      if (ret > 0) curPos += ret;
      return ret;
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      checkLimit(length);
      int ret = super.read(data, offset, length);
      if (ret > 0) curPos += ret;
      return ret;
    }

    public void setLimit(long limit) {
      limitPos = curPos + limit;
    }

    public void clearLimit() {
      limitPos = Long.MAX_VALUE;
    }

    @Override
    public void mark(int limit) {
      super.mark(limit);
      markPos = curPos;
    }

    @Override
    public void reset() throws IOException {
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

  private static final int maxOpSize = 32 * 1024 * 1024;

  private final LimitedInputStream limiter;
  private final DataInputStream in;
  private byte[] temp = new byte[4096];
  private final Checksum checksum;

  LogReader(LimitedInputStream limiter) {
    this.limiter = limiter;
    in = new DataInputStream(limiter);
    checksum = DataChecksum.newCrc32();
  }

  /**
   * Read a log entry from the input stream.
   *
   * Note that the objects returned from this method may be re-used by future
   * calls to the same method.
   *
   * @param skipBrokenEdits    If true, attempt to skip over damaged parts of
   * the input stream, rather than throwing an IOException
   * @return the operation read from the stream, or null at the end of the
   *         file
   * @throws IOException on error.  This function should only throw an
   *         exception when skipBrokenEdits is false.
   */
  public LogEntryProto read(boolean skipBrokenEdits) throws IOException {
    while (true) {
      try {
        return decodeEntry();
      } catch (IOException e) {
        in.reset();
        if (!skipBrokenEdits) {
          throw e;
        }
      } catch (Throwable e) {
        in.reset();
        if (!skipBrokenEdits) {
          throw new IOException("got unexpected exception " +
              e.getMessage(), e);
        }
      }
      // Move ahead one byte and re-try the decode process.
      if (in.skip(1) < 1) {
        return null;
      }
    }
  }

  void verifyTerminator() throws IOException {
     // The end of the log should contain 0x00 bytes.
     // If it contains other bytes, the log itself may be corrupt.
    limiter.clearLimit();
    int numRead = -1, idx = 0;
    while (true) {
      try {
        numRead = -1;
        numRead = in.read(temp);
        if (numRead == -1) {
          return;
        }
        for (idx = 0; idx < numRead; idx++) {
          if (temp[idx] != RaftConstants.LOG_TERMINATE_BYTE) {
            throw new IOException("Read extra bytes after " +
                "the terminator!");
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
    limiter.setLimit(maxOpSize);
    in.mark(maxOpSize);

    byte nextByte;
    try {
      nextByte = in.readByte();
    } catch (EOFException eof) {
      // EOF at an opcode boundary is expected.
      return null;
    }
    // Each log entry starts with a varint. Thus a valid entry's first byte
    // should not be 0. So if the terminate byte is 0, we should hit the end
    // of the segment.
    if (nextByte == RaftConstants.LOG_TERMINATE_BYTE) {
      verifyTerminator();
      return null;
    }

    // Here, we verify that the Op size makes sense and that the
    // data matches its checksum before attempting to construct an Op.
    int entryLength = CodedInputStream.readRawVarint32(nextByte, in);
    if (entryLength > maxOpSize) {
      throw new IOException("Entry has size " + entryLength
          + ", but maxOpSize = " + maxOpSize);
    }

    final int varintLength = CodedOutputStream.computeRawVarint32Size(
        entryLength);
    final int totalLength = varintLength + entryLength;
    checkBufferSize(totalLength);
    in.reset();
    in.mark(maxOpSize);
    IOUtils.readFully(in, temp, 0, totalLength);

    // verify checksum
    checksum.reset();
    checksum.update(temp, 0, totalLength);
    int expectedChecksum = in.readInt();
    int calculatedChecksum = (int) checksum.getValue();
    if (expectedChecksum != calculatedChecksum) {
      throw new ChecksumException("LogEntry is corrupt. Calculated checksum is "
          + calculatedChecksum + " but read checksum " + expectedChecksum,
          limiter.markPos);
    }

    // parse the buffer
    return LogEntryProto.parseFrom(
        CodedInputStream.newInstance(temp, varintLength, entryLength));
  }

  private void checkBufferSize(int entryLength) {
    Preconditions.checkArgument(entryLength <= maxOpSize);
    int length = temp.length;
    if (length < entryLength) {
      while (length < entryLength) {
        length = Math.min(length * 2, maxOpSize);
      }
      temp = new byte[length];
    }
  }

}
