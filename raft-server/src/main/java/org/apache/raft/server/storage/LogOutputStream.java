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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.server.RaftServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
  private FileOutputStream out; // file stream for storing edit logs
  private FileChannel fc; // channel of the file stream for sync
  private final Checksum checksum;
  private final int segmentMaxSize;

  public LogOutputStream(File file, boolean append, int segmentMaxSize)
      throws IOException {
    this.file = file;
    this.checksum = DataChecksum.newCrc32();
    this.segmentMaxSize = segmentMaxSize;
    RandomAccessFile rp = new RandomAccessFile(file, "rw");
    out = new FileOutputStream(rp.getFD());
    fc = rp.getChannel();
    fc.position(fc.size());

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

    preallocate(); // preallocate file
    out.write(SegmentedRaftLog.HEADER_BYTES);
    flush();
  }

  @Override
  public void close() throws IOException {
    try {
      if (fc != null && fc.isOpen()) {
        fc.truncate(fc.position());
      }
    } finally {
      IOUtils.cleanup(null, fc, out);
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
    fc.force(false); // metadata updates not needed
  }

  private void preallocate() throws IOException {
    fill.position(0);
    // preallocate a segment with max size
    int allocated = 0;
    while (allocated < segmentMaxSize) {
      int size = Math.min(BUFFER_SIZE, segmentMaxSize - allocated);
      ByteBuffer buffer = fill.slice();
      buffer.limit(size);
      IOUtils.writeFully(fc, buffer, fc.size());
      allocated += size;
    }
    LOG.debug("Pre-allocated {} bytes for the log segment", fill.capacity());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(" + file + ")";
  }
}
