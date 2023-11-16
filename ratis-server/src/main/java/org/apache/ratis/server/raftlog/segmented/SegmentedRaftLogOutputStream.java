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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class SegmentedRaftLogOutputStream implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogOutputStream.class);

  private static final ByteBuffer FILL;
  private static final int BUFFER_SIZE = 1024 * 1024; // 1 MB
  static {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    for (int i = 0; i < BUFFER_SIZE; i++) {
      buffer.put(SegmentedRaftLogFormat.getTerminator());
    }
    buffer.flip();
    FILL = buffer.asReadOnlyBuffer();
  }

  private final String name;
  private final BufferedWriteChannel out; // buffered FileChannel for writing
  private final PureJavaCrc32C checksum = new PureJavaCrc32C();

  private final long segmentMaxSize;
  private final long preallocatedSize;

  public SegmentedRaftLogOutputStream(File file, boolean append, long segmentMaxSize,
      long preallocatedSize, ByteBuffer byteBuffer)
      throws IOException {
    this.name = JavaUtils.getClassSimpleName(getClass()) + "(" + file.getName() + ")";
    this.segmentMaxSize = segmentMaxSize;
    this.preallocatedSize = preallocatedSize;
    this.out = BufferedWriteChannel.open(file, append, byteBuffer);

    if (!append) {
      // write header
      preallocateIfNecessary(SegmentedRaftLogFormat.getHeaderLength());
      out.writeToChannel(SegmentedRaftLogFormat.getHeaderBytebuffer());
      out.flush();
    }
  }

  /**
   * Write the given entry to this output stream.
   * <p>
   * Format:
   *   (1) The serialized size of the entry.
   *   (2) The entry.
   *   (3) 4-byte checksum of the entry.
   * <p>
   * Size in bytes to be written:
   *   (size to encode n) + n + (checksum size),
   *   where n is the entry serialized size and the checksum size is 4.
   */
  public void write(LogEntryProto entry) throws IOException {
    final int serialized = entry.getSerializedSize();
    final int proto = CodedOutputStream.computeUInt32SizeNoTag(serialized) + serialized;
    final int total = proto + 4; // proto and 4-byte checksum
    preallocateIfNecessary(total);

    out.writeToBuffer(total, buf -> {
      final int pos = buf.position();
      final int protoEndPos= pos + proto;

      final CodedOutputStream encoder = CodedOutputStream.newInstance(buf);
      encoder.writeUInt32NoTag(serialized);
      entry.writeTo(encoder);

      // compute checksum
      final ByteBuffer duplicated = buf.duplicate();
      duplicated.position(pos).limit(protoEndPos);
      checksum.reset();
      checksum.update(duplicated);

      buf.position(protoEndPos);
      buf.putInt((int) checksum.getValue());
      Preconditions.assertSame(pos + total, buf.position(), "buf.position()");
    });
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
      String msg = "Failed to flush " + this;
      LOG.error(msg, ioe);
      throw new IOException(msg, ioe);
    }
  }

  CompletableFuture<Void> asyncFlush(ExecutorService executor) throws IOException {
    try {
      return out.asyncFlush(executor);
    } catch (IOException ioe) {
      String msg = "Failed to asyncFlush " + this;
      LOG.error(msg, ioe);
      throw new IOException(msg, ioe);
    }
  }

  private static long actualPreallocateSize(long outstandingData, long remainingSpace, long preallocate) {
    return outstandingData > remainingSpace? outstandingData
        : outstandingData > preallocate? outstandingData
        : Math.min(preallocate, remainingSpace);
  }

  private long preallocate(FileChannel fc, long outstanding) throws IOException {
    final long size = fc.size();
    final long actual = actualPreallocateSize(outstanding, segmentMaxSize - size, preallocatedSize);
    Preconditions.assertTrue(actual >= outstanding);
    final long pos = fc.position();
    LOG.debug("Preallocate {} bytes (pos={}, size={}) for {}", actual, pos, size, this);
    final long allocated = IOUtils.preallocate(fc, actual, FILL);
    Preconditions.assertSame(pos, fc.position(), "fc.position()");
    Preconditions.assertSame(actual, allocated, "allocated");
    Preconditions.assertSame(size + allocated, fc.size(), "fc.size()");
    return allocated;
  }

  private void preallocateIfNecessary(int size) throws IOException {
    out.preallocateIfNecessary(size, this::preallocate);
  }

  @Override
  public String toString() {
    return name;
  }
}
