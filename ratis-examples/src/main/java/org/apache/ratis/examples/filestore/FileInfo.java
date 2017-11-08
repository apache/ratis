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
package org.apache.ratis.examples.filestore;

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

abstract class FileInfo {
  public static final Logger LOG = LoggerFactory.getLogger(FileInfo.class);

  private final Path relativePath;

  FileInfo(Path relativePath) {
    this.relativePath = relativePath;
  }

  Path getRelativePath() {
    return relativePath;
  }

  long getSize() {
    throw new UnsupportedOperationException(
        "File " + getRelativePath() + " size is unknown.");
  }

  void flush() throws IOException {
    // no-op
  }

  ByteString read(CheckedFunction<Path, Path, IOException> resolver, long offset, long length)
      throws IOException {
    flush();
    if (offset + length > getSize()) {
      throw new IOException("Failed to read: offset (=" + offset
          + " + length (=" + length + ") > size = " + getSize()
          + ", path=" + getRelativePath());
    }

    try(final SeekableByteChannel in = Files.newByteChannel(
        resolver.apply(getRelativePath()), StandardOpenOption.READ)) {
      final ByteBuffer buffer = ByteBuffer.allocateDirect(FileStoreCommon.getChunkSize(length));
      in.position(offset).read(buffer);
      buffer.flip();
      return ByteString.copyFrom(buffer);
    }
  }

  UnderConstruction asUnderConstruction() {
    throw new UnsupportedOperationException(
        "File " + getRelativePath() + " is not under construction.");
  }

  static class ReadOnly extends FileInfo {
    private final long size;

    ReadOnly(UnderConstruction f) {
      super(f.getRelativePath());
      this.size = f.getSize();
    }

    @Override
    long getSize() {
      return size;
    }
  }

  static class FileOut implements Closeable {
    private final OutputStream out;
    private final WritableByteChannel channel;

    FileOut(Path p) throws IOException {
      this.out = FileUtils.createNewFile(p);
      this.channel = Channels.newChannel(out);
    }

    int write(ByteBuffer data) throws IOException {
      return channel.write(data);
    }

    void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      channel.close();
      out.close();
    }
  }

  static class UnderConstruction extends FileInfo {
    private FileOut out;

    /** The size written to a local file. */
    private volatile long writeSize;
    /** The size committed to client. */
    private volatile long committedSize;
    /** The size at last flush. */
    private volatile long flushSize;

    /** A queue to make sure that the writes are in order. */
    private final TaskQueue writeQueue = new TaskQueue("writeQueue");
    /** A queue to make sure that the commits are in order. */
    private final TaskQueue commitQueue = new TaskQueue("commitQueue");
    /** Futures to make sure that each commit is executed the corresponding write. */
    private final Map<Long, CompletableFuture<Integer>> writeFutures = new ConcurrentHashMap<>();

    UnderConstruction(Path relativePath) {
      super(relativePath);
    }

    @Override
    UnderConstruction asUnderConstruction() {
      return this;
    }

    @Override
    long getSize() {
      return committedSize;
    }

    CompletableFuture<Integer> submitCreate(
        CheckedFunction<Path, Path, IOException> resolver, ByteString data, boolean close,
        ExecutorService executor, RaftPeerId id, long index) {
      final Supplier<String> name = () -> "create(" + getRelativePath() + ", "
          + close + ") @" + id + ":" + index;
      final CheckedSupplier<Integer, IOException> task = LogUtils.newCheckedSupplier(LOG, () -> {
        if (out == null) {
          out = new FileOut(resolver.apply(getRelativePath()));
        }
        return write(0L, data, close);
      }, name);
      return submitWrite(task, executor, id, index);
    }

    CompletableFuture<Integer> submitWrite(
        long offset, ByteString data, boolean close, ExecutorService executor,
        RaftPeerId id, long index) {
      final Supplier<String> name = () -> "write(" + getRelativePath() + ", "
          + offset + ", " + close + ") @" + id + ":" + index;
      final CheckedSupplier<Integer, IOException> task = LogUtils.newCheckedSupplier(LOG,
          () -> write(offset, data, close), name);
      return submitWrite(task, executor, id, index);
    }

    private CompletableFuture<Integer> submitWrite(
        CheckedSupplier<Integer, IOException> task, ExecutorService executor,
      RaftPeerId id, long index) {
      final CompletableFuture<Integer> f = writeQueue.submit(task, executor,
          e -> new IOException("Failed " + task, e));
      CollectionUtils.putNew(index, f, writeFutures, () ->  id + ":writeFutures");
      return f;
    }

    private int write(long offset, ByteString data, boolean close) throws IOException {
      if (offset != writeSize) {
        throw new IOException("Offset/size mismatched: offset = " + offset
            + " != writeSize = " + writeSize + ", path=" + getRelativePath());
      }
      if (out == null) {
        throw new IOException("File output is not initialized, path=" + getRelativePath());
      }

      synchronized (out) {
        int n = 0;
        if (data != null) {
          final ByteBuffer buffer = data.asReadOnlyByteBuffer();
          try {
            for (; buffer.remaining() > 0; ) {
              n += out.write(buffer);
            }
          } finally {
            writeSize += n;
          }
        }

        if (close) {
          out.close();
        }
        return n;
      }
    }

    void flush() throws IOException {
      if (flushSize >= committedSize) {
        return;
      }
      synchronized (out) {
        if (flushSize >= committedSize) {
          return;
        }
        out.flush();
        flushSize = writeSize;
      }
    }

    CompletableFuture<Integer> submitCommit(
        long offset, int size, Function<UnderConstruction, ReadOnly> converter,
        ExecutorService executor, RaftPeerId id, long index) {
      final Supplier<String> name = () -> "commit(" + getRelativePath() + ", "
          + offset + ", " + size + ") @" + id + ":" + index;
      final CheckedSupplier<Integer, IOException> task = LogUtils.newCheckedSupplier(LOG, () -> {
        if (offset != committedSize) {
          throw new IOException("Offset/size mismatched: offset = "
              + offset + " != committedSize = " + committedSize
              + ", path=" + getRelativePath());
        } else if (committedSize + size > writeSize) {
          throw new IOException("Offset/size mismatched: committed (=" + committedSize
              + ") + size (=" + size + ") > writeSize = " + writeSize);
        }
        committedSize += size;

        if (converter != null) {
          converter.apply(this);
        }
        return size;
      }, name);

      final CompletableFuture<Integer> write = writeFutures.remove(index);
      if (write == null) {
        return JavaUtils.completeExceptionally(
            new IOException(name.get() + " is already committed."));
      }
      return write.thenComposeAsync(writeSize -> {
        Preconditions.assertTrue(size == writeSize);
        return commitQueue.submit(task, executor,
            e -> new IOException("Failed " + task, e));
      }, executor);
    }
  }
}
