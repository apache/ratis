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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedBiFunction;

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
 *
 * This class is NOT threadsafe.
 */
class BufferedWriteChannel implements Closeable {

  @SuppressWarnings("java:S2095") // return Closable
  static BufferedWriteChannel open(File file, boolean append, ByteBuffer buffer) throws IOException {
    final RandomAccessFile raf = new RandomAccessFile(file, "rw");
    final FileChannel fc = raf.getChannel();
    if (append) {
      fc.position(fc.size());
    } else {
      fc.truncate(0);
    }
    Preconditions.assertSame(fc.size(), fc.position(), "fc.position");
    return new BufferedWriteChannel(fc, buffer);
  }

  private final FileChannel fileChannel;
  private final ByteBuffer writeBuffer;
  private boolean forced = true;
  private final AtomicReference<CompletableFuture<Void>> flushFuture
      = new AtomicReference<>(CompletableFuture.completedFuture(null));


  BufferedWriteChannel(FileChannel fileChannel, ByteBuffer byteBuffer) {
    this.fileChannel = fileChannel;
    this.writeBuffer = byteBuffer;
  }

  void write(byte[] b) throws IOException {
    write(b, b.length);
  }
  void write(byte[] b, int len) throws IOException {
    int offset = 0;
    while (offset < len) {
      int toPut = Math.min(len - offset, writeBuffer.remaining());
      writeBuffer.put(b, offset, toPut);
      offset += toPut;
      if (writeBuffer.remaining() == 0) {
        flushBuffer();
      }
    }
  }

  void preallocateIfNecessary(long size, CheckedBiFunction<FileChannel, Long, Long, IOException> preallocate)
      throws IOException {
    final long outstanding = writeBuffer.position() + size;
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
      LogSegment.LOG.error("Failed to flush channel", e);
      throw new CompletionException(e);
    }
    return null;
  }

  /**
   * Write any data in the buffer to the file.
   *
   * @throws IOException if the write fails.
   */
  private void flushBuffer() throws IOException {
    if (writeBuffer.position() == 0) {
      return; // nothing to flush
    }

    writeBuffer.flip();
    do {
      fileChannel.write(writeBuffer);
    } while (writeBuffer.hasRemaining());
    writeBuffer.clear();
    forced = false;
  }

  boolean isOpen() {
    return fileChannel.isOpen();
  }

  @Override
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
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
}
