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

import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.apache.ratis.util.function.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides a buffering layer in front of a FileChannel for writing.
 * <p>
 * This class is NOT threadsafe.
 */
class BufferedWriteChannel implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(BufferedWriteChannel.class);

  @SuppressWarnings("java:S2095") // return Closable
  static BufferedWriteChannel open(File file, boolean append, ByteBuffer buffer) throws IOException {
    final RandomAccessFile raf = new RandomAccessFile(file, "rw");
    final FileChannel fc = raf.getChannel();
    final long size = file.length(); // 0L if the file does not exist.
    if (append) {
      fc.position(size);
      Preconditions.assertSame(size, fc.size(), "fc.size");
    } else {
      if (size > 0) {
        fc.truncate(0);
      }
      Preconditions.assertSame(0, fc.size(), "fc.size");
    }
    Preconditions.assertSame(fc.size(), fc.position(), "fc.position");
    final String name = file.getName() + (append? " (append)": "");
    LOG.info("open {} at position {}", name, fc.position());
    return new BufferedWriteChannel(name, fc, buffer);
  }

  private final String name;
  private final FileChannel fileChannel;
  private final ByteBuffer writeBuffer;
  private boolean forced = true;
  private final AtomicReference<CompletableFuture<Void>> flushFuture
      = new AtomicReference<>(CompletableFuture.completedFuture(null));


  BufferedWriteChannel(String name, FileChannel fileChannel, ByteBuffer byteBuffer) {
    this.name = name;
    this.fileChannel = fileChannel;
    this.writeBuffer = byteBuffer;
  }

  int writeBufferPosition() {
    return writeBuffer.position();
  }

  /**
   * Write to buffer.
   *
   * @param writeSize the size to write.
   * @param writeMethod write exactly the writeSize of bytes to the buffer and advance buffer position.
   */
  void writeToBuffer(int writeSize, CheckedConsumer<ByteBuffer, IOException> writeMethod) throws IOException {
    if (writeSize > writeBuffer.capacity()) {
      throw new IOException("writeSize = " + writeSize
          + " > writeBuffer.capacity() = " + writeBuffer.capacity());
    }
    if (writeSize > writeBuffer.remaining()) {
      flushBuffer();
    }
    final int pos = writeBufferPosition();
    final int lim = writeBuffer.limit();
    writeMethod.accept(writeBuffer);
    final int written = writeBufferPosition() - pos;
    Preconditions.assertSame(writeSize, written, "written");
    Preconditions.assertSame(lim, writeBuffer.limit(), "writeBuffer.limit()");
  }

  /** Write the content of the given buffer to {@link #fileChannel}. */
  void writeToChannel(ByteBuffer buffer) throws IOException {
    Preconditions.assertSame(0, writeBufferPosition(), "writeBuffer.position()");
    final int length = buffer.remaining();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Write {} bytes (pos={}, size={}) to channel {}",
          length, fileChannel.position(), fileChannel.size(), this);
    }
    int written = 0;
    for(; written < length; ) {
      written += fileChannel.write(buffer);
    }
    Preconditions.assertSame(length, written, "written");
    forced = false;
  }

  void preallocateIfNecessary(long size, CheckedBiFunction<FileChannel, Long, Long, IOException> preallocate)
      throws IOException {
    final long outstanding = writeBufferPosition() + size;
    if (fileChannel.position() + outstanding > fileChannel.size()) {
      preallocate.apply(fileChannel, outstanding);
    }
  }

  /**
   * Write any data in the buffer to the file and force a
   * sync operation so that data is persisted to the disk.
   *
   * @throws IOException if the write or sync operation fails.
   */
  void flush() throws IOException {
    flushBuffer();
    if (!forced) {
      fileChannel.force(false);
      forced = true;
    }
  }

  CompletableFuture<Void> asyncFlush(ExecutorService executor) throws IOException {
    flushBuffer();
    if (forced) {
      return flushFuture.get();
    }
    final CompletableFuture<Void> f = CompletableFuture.supplyAsync(this::fileChannelForce, executor);
    forced = true;
    return flushFuture.updateAndGet(previous -> f.thenCombine(previous, (current, prev) -> current));
  }

  private Void fileChannelForce() {
    try {
      fileChannel.force(false);
    } catch (IOException e) {
      throw new CompletionException("Failed to force channel " + this, e);
    }
    return null;
  }

  /** Flush the data from the {@link #writeBuffer} to {@link #fileChannel}. */
  private void flushBuffer() throws IOException {
    if (writeBufferPosition() == 0) {
      return; // nothing to flush
    }

    writeBuffer.flip();
    writeToChannel(writeBuffer);
    writeBuffer.clear();
    forced = false;
  }

  boolean isOpen() {
    return fileChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (!isOpen()) {
      return;
    }

    try {
      flushFuture.get().join();
      fileChannel.truncate(fileChannel.position());
    } finally {
      fileChannel.close();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
