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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Provides a buffering layer in front of a FileChannel for writing.
 *
 * This class is NOT threadsafe.
 */
public class BufferedWriteChannel extends BufferedChannelBase {
  // The buffer used to write operations.
  private final ByteBuffer writeBuffer;
  // The absolute position of the next write operation.
  private volatile long position;
  /** Are all the data already flushed? */
  private boolean flushed = true;

  public BufferedWriteChannel(FileChannel fc, ByteBuffer byteBuffer)
      throws IOException {
    super(fc);
    this.position = fc.position();
    this.writeBuffer = byteBuffer;
  }

  /**
   * Write the specified byte.
   * @param b the byte to be written
   */
  public void write(int b) throws IOException {
    writeBuffer.put((byte) b);
    if (writeBuffer.remaining() == 0) {
      flushInternal();
    }
    flushed = false;
    position++;
  }

  public void write(byte[] b) throws IOException {
    int offset = 0;
    while (offset < b.length) {
      int toPut = Math.min(b.length - offset, writeBuffer.remaining());
      writeBuffer.put(b, offset, toPut);
      offset += toPut;
      if (writeBuffer.remaining() == 0) {
        flushInternal();
      }
    }
    flushed = false;
    position += b.length;
  }

  /**
   * Get the position where the next write operation will begin writing from.
   */
  public long position() {
    return position;
  }

  /**
   * Write any data in the buffer to the file and force a
   * sync operation so that data is persisted to the disk.
   *
   * @throws IOException if the write or sync operation fails.
   */
  void flush() throws IOException {
    if (flushed) {
      return; // flushed previously
    }

    flushInternal();
    fileChannel.force(false);
    flushed = true;
  }

  /**
   * Write any data in the buffer to the file and advance the writeBufferPosition
   * Callers are expected to synchronize appropriately
   *
   * @throws IOException if the write fails.
   */
  private void flushInternal() throws IOException {
    if (writeBuffer.position() == 0) {
      return; // nothing to flush
    }

    writeBuffer.flip();
    do {
      fileChannel.write(writeBuffer);
    } while (writeBuffer.hasRemaining());
    writeBuffer.clear();
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
    writeBuffer.clear();
  }
}
