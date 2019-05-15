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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides a buffering layer in front of a FileChannel for writing.
 */
public class BufferedWriteChannel extends BufferedChannelBase {
  // The capacity of the write buffer.
  private final int writeCapacity;
  // The position of the file channel's write pointer.
  private AtomicLong writeBufferStartPosition = new AtomicLong(0);
  // The buffer used to write operations.
  private final ByteBuffer writeBuffer;
  // The absolute position of the next write operation.
  private volatile long position;

  public BufferedWriteChannel(FileChannel fc, int writeCapacity)
      throws IOException {
    super(fc);
    this.writeCapacity = writeCapacity;
    this.position = fc.position();
    this.writeBufferStartPosition.set(position);
    this.writeBuffer = ByteBuffer.allocateDirect(writeCapacity);
  }

  /**
   * Write all the data in src to the {@link FileChannel}. Note that this function can
   * buffer or re-order writes based on the implementation. These writes will be flushed
   * to the disk only when flush() is invoked.
   *
   * @param src The source ByteBuffer which contains the data to be written.
   * @throws IOException if a write operation fails.
   */
  public void write(ByteBuffer src) throws IOException {
    int copied = 0;
    while (src.remaining() > 0) {
      int truncated = 0;
      if (writeBuffer.remaining() < src.remaining()) {
        truncated = src.remaining() - writeBuffer.remaining();
        src.limit(src.limit() - truncated);
      }
      copied += src.remaining();
      writeBuffer.put(src);
      src.limit(src.limit() + truncated);
      // if we have run out of buffer space, we should flush to the file
      if (writeBuffer.remaining() == 0) {
        flushInternal();
      }
    }
    position += copied;
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
    position += b.length;
  }

  /**
   * Get the position where the next write operation will begin writing from.
   */
  public long position() {
    return position;
  }

  /**
   * Get the position of the file channel's write pointer.
   */
  public long getFileChannelPosition() {
    return writeBufferStartPosition.get();
  }


  /**
   * Write any data in the buffer to the file. If sync is set to true, force a
   * sync operation so that data is persisted to the disk.
   *
   * @throws IOException if the write or sync operation fails.
   */
  public void flush(boolean shouldForceWrite) throws IOException {
    synchronized (this) {
      flushInternal();
    }
    if (shouldForceWrite) {
      forceWrite(false);
    }
  }

  /**
   * Write any data in the buffer to the file and advance the writeBufferPosition
   * Callers are expected to synchronize appropriately
   *
   * @throws IOException if the write fails.
   */
  private void flushInternal() throws IOException {
    writeBuffer.flip();
    do {
      fileChannel.write(writeBuffer);
    } while (writeBuffer.hasRemaining());
    writeBuffer.clear();
    writeBufferStartPosition.set(fileChannel.position());
  }

  public long forceWrite(boolean forceMetadata) throws IOException {
    // This is the point up to which we had flushed to the file system page cache
    // before issuing this force write hence is guaranteed to be made durable by
    // the force write, any flush that happens after this may or may
    // not be flushed
    long positionForceWrite = writeBufferStartPosition.get();
    fileChannel.force(forceMetadata);
    return positionForceWrite;
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
    writeBuffer.clear();
  }
}
