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

import org.apache.ratis.examples.filestore.FileInfo.ReadOnly;
import org.apache.ratis.examples.filestore.FileInfo.UnderConstruction;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.proto.ExamplesProtos.*;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

public class FileStore implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

  static class FileMap {
    private final Object name;
    private final Map<Path, FileInfo> map = new ConcurrentHashMap<>();

    FileMap(Supplier<String> name) {
      this.name = StringUtils.stringSupplierAsObject(name);
    }

    FileInfo get(String relative) throws FileNotFoundException {
      return applyFunction(relative, map::get);
    }

    FileInfo remove(String relative) throws FileNotFoundException {
      LOG.trace("{}: remove {}", name, relative);
      return applyFunction(relative, map::remove);
    }

    private FileInfo applyFunction(String relative, Function<Path, FileInfo> f)
        throws FileNotFoundException {
      final FileInfo info = f.apply(normalize(relative));
      if (info == null) {
        throw new FileNotFoundException("File " + relative + " not found in " + name);
      }
      return info;
    }

    void putNew(UnderConstruction uc) {
      LOG.trace("{}: putNew {}", name, uc.getRelativePath());
      CollectionUtils.putNew(uc.getRelativePath(), uc, map, name::toString);
    }

    ReadOnly close(UnderConstruction uc) {
      LOG.trace("{}: close {}", name, uc.getRelativePath());
      final ReadOnly ro = new ReadOnly(uc);
      CollectionUtils.replaceExisting(uc.getRelativePath(), uc, ro, map, name::toString);
      return ro;
    }
  }

  private final Supplier<RaftPeerId> idSupplier;
  private final Supplier<Path> rootSupplier;
  private final FileMap files;

  private final ExecutorService writer = Executors.newFixedThreadPool(10);
  private final ExecutorService committer = Executors.newFixedThreadPool(3);
  private final ExecutorService reader = Executors.newFixedThreadPool(10);
  private final ExecutorService deleter = Executors.newFixedThreadPool(3);

  public FileStore(Supplier<RaftPeerId> idSupplier, Path dir) {
    this.idSupplier = idSupplier;
    this.rootSupplier = JavaUtils.memoize(
        () -> dir.resolve(getId().toString()).normalize().toAbsolutePath());
    this.files = new FileMap(JavaUtils.memoize(() -> idSupplier.get() + ":files"));
  }

  public RaftPeerId getId() {
    return Objects.requireNonNull(idSupplier.get(), getClass().getSimpleName() + " is not initialized.");
  }

  public Path getRoot() {
    return rootSupplier.get();
  }

  static Path normalize(String path) {
    Objects.requireNonNull(path, "path == null");
    return Paths.get(path).normalize();
  }

  Path resolve(Path relative) throws IOException {
    final Path root = getRoot();
    final Path full = root.resolve(relative).normalize().toAbsolutePath();
    if (full.equals(root)) {
      throw new IOException("The file path " + relative + " resolved to " + full
          + " is the root directory " + root);
    } else if (!full.startsWith(root)) {
      throw new IOException("The file path " + relative + " resolved to " + full
          + " is not a sub-path under root directory " + root);
    }
    return full;
  }

  CompletableFuture<ReadReplyProto> read(String relative, long offset, long length) {
    final Supplier<String> name = () -> "read(" + relative
        + ", " + offset + ", " + length + ") @" + getId();
    final CheckedSupplier<ReadReplyProto, IOException> task = LogUtils.newCheckedSupplier(LOG, () -> {
      final FileInfo info = files.get(relative);
      final ReadReplyProto.Builder reply = ReadReplyProto.newBuilder()
          .setResolvedPath(FileStoreCommon.toByteString(info.getRelativePath()))
          .setOffset(offset);

      final ByteString bytes = info.read(this::resolve, offset, length);
      return reply.setData(bytes).build();
    }, name);
    return submit(task, reader);
  }

  CompletableFuture<Path> delete(long index, String relative) {
    final Supplier<String> name = () -> "delete(" + relative + ") @" + getId() + ":" + index;
    final CheckedSupplier<Path, IOException> task = LogUtils.newCheckedSupplier(LOG, () -> {
      final FileInfo info = files.remove(relative);
      FileUtils.delete(resolve(info.getRelativePath()));
      return info.getRelativePath();
    }, name);
    return submit(task, deleter);
  }

  static <T> CompletableFuture<T> submit(
      CheckedSupplier<T, IOException> task, ExecutorService executor) {
    final CompletableFuture<T> f = new CompletableFuture<>();
    executor.submit(() -> {
      try {
        f.complete(task.get());
      } catch (IOException e) {
        f.completeExceptionally(new IOException("Failed " + task, e));
      }
    });
    return f;
  }

  CompletableFuture<WriteReplyProto> submitCommit(
      long index, String relative, boolean close, long offset, int size) {
    final Function<UnderConstruction, ReadOnly> converter = close ? files::close: null;
    final UnderConstruction uc;
    try {
      uc = files.get(relative).asUnderConstruction();
    } catch (FileNotFoundException e) {
      return FileStoreCommon.completeExceptionally(
          index, "Failed to write to " + relative, e);
    }

    return uc.submitCommit(offset, size, converter, committer, getId(), index)
        .thenApply(n -> WriteReplyProto.newBuilder()
            .setResolvedPath(FileStoreCommon.toByteString(uc.getRelativePath()))
            .setOffset(offset)
            .setLength(n)
            .build());
  }

  CompletableFuture<Integer> write(
      long index, String relative, boolean close, long offset, ByteString data) {
    final int size = data != null? data.size(): 0;
    LOG.trace("write {}, offset={}, size={}, close? {} @{}:{}",
        relative, offset, size, close, getId(), index);
    final boolean createNew = offset == 0L;
    final UnderConstruction uc;
    if (createNew) {
      uc = new UnderConstruction(normalize(relative));
      files.putNew(uc);
    } else {
      try {
        uc = files.get(relative).asUnderConstruction();
      } catch (FileNotFoundException e) {
        return FileStoreCommon.completeExceptionally(
            index, "Failed to write to " + relative, e);
      }
    }

    return size == 0 && !close? CompletableFuture.completedFuture(0)
        : createNew? uc.submitCreate(this::resolve, data, close, writer, getId(), index)
        : uc.submitWrite(offset, data, close, writer, getId(), index);
  }

  @Override
  public void close() {
    writer.shutdownNow();
    committer.shutdownNow();
    reader.shutdownNow();
    deleter.shutdownNow();
  }
}
