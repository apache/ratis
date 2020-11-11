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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.PureJavaCrc32C;
import org.apache.ratis.util.function.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.Checksum;

public class SegmentedRaftLogOutputStream implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogOutputStream.class);

  private static final ByteBuffer FILL;
  private static final int BUFFER_SIZE = 1024 * 1024; // 1 MB
  static {
    FILL = ByteBuffer.allocateDirect(BUFFER_SIZE);
    for (int i = 0; i < FILL.capacity(); i++) {
      FILL.put(SegmentedRaftLogFormat.getTerminator());
    }
    FILL.flip();
  }

  private final File file;
  private final BufferedWriteChannel out; // buffered FileChannel for writing
  private final Checksum checksum;

  private final long segmentMaxSize;
  private final long preallocatedSize;

  public SegmentedRaftLogOutputStream(File file, boolean append, long segmentMaxSize,
      long preallocatedSize, ByteBuffer byteBuffer)
      throws IOException {
    this.file = file;
    this.checksum = new PureJavaCrc32C();
    this.segmentMaxSize = segmentMaxSize;
    this.preallocatedSize = preallocatedSize;
    this.out = BufferedWriteChannel.open(file, append, byteBuffer);

    if (!append) {
      // write header
      preallocateIfNecessary(SegmentedRaftLogFormat.getHeaderLength());
      SegmentedRaftLogFormat.applyHeaderTo(CheckedConsumer.asCheckedFunction(out::write));
      out.flush();
    }
  }

  /**
   * Write the given entry to this output stream.
   *
   * Format:
   *   (1) The serialized size of the entry.
   *   (2) The entry.
   *   (3) 4-byte checksum of the entry.
   *
   * Size in bytes to be written:
   *   (size to encode n) + n + (checksum size),
   *   where n is the entry serialized size and the checksum size is 4.
   */
  public void write(LogEntryProto entry) throws IOException {
    final int serialized = entry.getSerializedSize();
    final int proto = CodedOutputStream.computeUInt32SizeNoTag(serialized) + serialized;
    final byte[] buf = new byte[proto + 4]; // proto and 4-byte checksum
    preallocateIfNecessary(buf.length);

    CodedOutputStream cout = CodedOutputStream.newInstance(buf);
    cout.writeUInt32NoTag(serialized);
    entry.writeTo(cout);

    checksum.reset();
    checksum.update(buf, 0, proto);
    ByteBuffer.wrap(buf, proto, 4).putInt((int) checksum.getValue());

    out.write(buf);
  }

  @Override
  public void close() throws IOException {
    try {
      flush();
    } finally {
      IOUtils.cleanup(LOG, out);
    }
  }

  /**
   * Flush data to persistent store.
   * Collect sync metrics.
   */
  public void flush() throws IOException {
    try {
      out.flush();
    } catch (IOException ioe) {
      throw new IOException("Failed to flush " + this, ioe);
    }
  }

  private static long actualPreallocateSize(long outstandingData, long remainingSpace, long preallocate) {
    return outstandingData > remainingSpace? outstandingData
        : outstandingData > preallocate? outstandingData
        : Math.min(preallocate, remainingSpace);
  }

  private long preallocate(FileChannel fc, long outstanding) throws IOException {
    final long actual = actualPreallocateSize(outstanding, segmentMaxSize - fc.size(), preallocatedSize);
    Preconditions.assertTrue(actual >= outstanding);
    final long allocated = IOUtils.preallocate(fc, actual, FILL);
    LOG.debug("Pre-allocated {} bytes for {}", allocated, this);
    return allocated;
  }

  private void preallocateIfNecessary(int size) throws IOException {
    out.preallocateIfNecessary(size, this::preallocate);
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + "(" + file + ")";
  }
}
