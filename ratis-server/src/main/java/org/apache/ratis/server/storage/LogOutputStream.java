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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.shaded.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.PureJavaCrc32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.Checksum;

public class LogOutputStream implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LogOutputStream.class);

  private static final ByteBuffer fill;
  private static final int BUFFER_SIZE = 1024 * 1024; // 1 MB
  static {
    fill = ByteBuffer.allocateDirect(BUFFER_SIZE);
    fill.position(0);
    for (int i = 0; i < fill.capacity(); i++) {
      fill.put(RaftServerConstants.LOG_TERMINATE_BYTE);
    }
  }

  private File file;
  private FileChannel fc; // channel of the file stream for sync
  private BufferedWriteChannel out; // buffered FileChannel for writing
  private final Checksum checksum;

  private final long segmentMaxSize;
  private final long preallocatedSize;
  private long preallocatedPos;

  public LogOutputStream(File file, boolean append, RaftProperties properties)
      throws IOException {
    this.file = file;
    this.checksum = new PureJavaCrc32C();
    this.segmentMaxSize = RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.preallocatedSize = RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
    RandomAccessFile rp = new RandomAccessFile(file, "rw");
    fc = rp.getChannel();
    fc.position(fc.size());
    preallocatedPos = fc.size();

    final int bufferSize = RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();
    out = new BufferedWriteChannel(fc, bufferSize);

    if (!append) {
      create();
    }
  }

  /**
   * Format:
   * LogEntryProto's protobuf
   * 4-byte checksum of the above protobuf
   */
  public void write(LogEntryProto entry) throws IOException {
    final int serialized = entry.getSerializedSize();
    final int bufferSize = CodedOutputStream.computeUInt32SizeNoTag(serialized)
        + serialized;

    preallocateIfNecessary(bufferSize + 4);

    byte[] buf = new byte[bufferSize];
    CodedOutputStream cout = CodedOutputStream.newInstance(buf);
    cout.writeUInt32NoTag(serialized);
    entry.writeTo(cout);

    checksum.reset();
    checksum.update(buf, 0, buf.length);
    final int sum = (int) checksum.getValue();

    out.write(buf);
    writeInt(sum);
  }

  private void writeInt(int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v) & 0xFF);
  }

  private void create() throws IOException {
    fc.truncate(0);
    fc.position(0);
    preallocatedPos = 0;
    preallocate(); // preallocate file

    out.write(SegmentedRaftLog.HEADER_BYTES);
    flush();
  }

  @Override
  public void close() throws IOException {
    try {
      out.flush(false);
      if (fc != null && fc.isOpen()) {
        fc.truncate(fc.position());
      }
    } finally {
      IOUtils.cleanup(LOG, fc, out);
      fc = null;
      out = null;
    }
  }

  /**
   * Flush data to persistent store.
   * Collect sync metrics.
   */
  public void flush() throws IOException {
    if (out == null) {
      throw new IOException("Trying to use aborted output stream");
    }
    out.flush(true);
  }

  private void preallocate() throws IOException {
    fill.position(0);
    long targetSize = Math.min(segmentMaxSize - fc.size(), preallocatedSize);
    int allocated = 0;
    while (allocated < targetSize) {
      int size = (int) Math.min(BUFFER_SIZE, targetSize - allocated);
      ByteBuffer buffer = fill.slice();
      buffer.limit(size);
      IOUtils.writeFully(fc, buffer, preallocatedPos);
      preallocatedPos += size;
      allocated += size;
    }
    LOG.debug("Pre-allocated {} bytes for the log segment", allocated);
  }

  private void preallocateIfNecessary(int size) throws IOException {
    if (out.position() + size > preallocatedPos) {
      preallocate();
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(" + file + ")";
  }
}
