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
package org.apache.ratis.examples.filestore;

import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.filestore.FileInfo.ReadOnly;
import org.apache.ratis.examples.filestore.FileInfo.UnderConstruction;
import org.apache.ratis.proto.ExamplesProtos.ReadReplyProto;
import org.apache.ratis.proto.ExamplesProtos.StreamWriteReplyProto;
import org.apache.ratis.proto.ExamplesProtos.WriteReplyProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private final List<Supplier<Path>> rootSuppliers;
  private final FileMap files;

  private final ExecutorService writer;
  private final ExecutorService committer;
  private final ExecutorService reader;
  private final ExecutorService deleter;

  public FileStore(Supplier<RaftPeerId> idSupplier, RaftProperties properties) {
    this.idSupplier = idSupplier;
    this.rootSuppliers = new ArrayList<>();

    int writeThreadNum = ConfUtils.getInt(properties::getInt, FileStoreCommon.STATEMACHINE_WRITE_THREAD_NUM,
        1, LOG::info);
    int readThreadNum = ConfUtils.getInt(properties::getInt, FileStoreCommon.STATEMACHINE_READ_THREAD_NUM,
        1, LOG::info);
    int commitThreadNum = ConfUtils.getInt(properties::getInt, FileStoreCommon.STATEMACHINE_COMMIT_THREAD_NUM,
        1, LOG::info);
    int deleteThreadNum = ConfUtils.getInt(properties::getInt, FileStoreCommon.STATEMACHINE_DELETE_THREAD_NUM,
        1, LOG::info);
    writer = Executors.newFixedThreadPool(writeThreadNum);
    reader = Executors.newFixedThreadPool(readThreadNum);
    committer = Executors.newFixedThreadPool(commitThreadNum);
    deleter = Executors.newFixedThreadPool(deleteThreadNum);

    final List<File> dirs = ConfUtils.getFiles(properties::getFiles, FileStoreCommon.STATEMACHINE_DIR_KEY,
        null, LOG::info);
    Objects.requireNonNull(dirs, FileStoreCommon.STATEMACHINE_DIR_KEY + " is not set.");
    for (File dir : dirs) {
      this.rootSuppliers.add(
          JavaUtils.memoize(() -> dir.toPath().resolve(getId().toString()).normalize().toAbsolutePath()));
    }
    this.files = new FileMap(JavaUtils.memoize(() -> idSupplier.get() + ":files"));
  }

  public RaftPeerId getId() {
    return Objects.requireNonNull(idSupplier.get(),
        () -> JavaUtils.getClassSimpleName(getClass()) + " is not initialized.");
  }

  private Path getRoot(Path relative) {
    int hash = relative.toAbsolutePath().toString().hashCode() % rootSuppliers.size();
    return rootSuppliers.get(Math.abs(hash)).get();
  }

  public List<Path> getRoots() {
    List<Path> roots = new ArrayList<>();
    for (Supplier<Path> s : rootSuppliers) {
      roots.add(s.get());
    }
    return roots;
  }

  static Path normalize(String path) {
    Objects.requireNonNull(path, "path == null");
    return Paths.get(path).normalize();
  }

  Path resolve(Path relative) throws IOException {
    final Path root = getRoot(relative);
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

  CompletableFuture<ReadReplyProto> read(String relative, long offset, long length, boolean readCommitted) {
    final Supplier<String> name = () -> "read(" + relative
        + ", " + offset + ", " + length + ") @" + getId();
    final CheckedSupplier<ReadReplyProto, IOException> task = LogUtils.newCheckedSupplier(LOG, () -> {
      final FileInfo info = files.get(relative);
      final ReadReplyProto.Builder reply = ReadReplyProto.newBuilder()
          .setResolvedPath(FileStoreCommon.toByteString(info.getRelativePath()))
          .setOffset(offset);

      final ByteString bytes = info.read(this::resolve, offset, length, readCommitted);
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
      long index, String relative, boolean close, boolean sync, long offset, ByteString data) {
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
        : createNew? uc.submitCreate(this::resolve, data, close, sync, writer, getId(), index)
        : uc.submitWrite(offset, data, close, sync, writer, getId(), index);
  }

  @Override
  public void close() {
    writer.shutdownNow();
    committer.shutdownNow();
    reader.shutdownNow();
    deleter.shutdownNow();
  }

  CompletableFuture<StreamWriteReplyProto> streamCommit(String p, long bytesWritten) {
    return CompletableFuture.supplyAsync(() -> {
      long len = 0;
      try {
        final Path full = resolve(normalize(p));
        RandomAccessFile file = new RandomAccessFile(full.toFile(), "r");
        len = file.length();
        return StreamWriteReplyProto.newBuilder().setIsSuccess(len == bytesWritten).setByteWritten(len).build();
      } catch (IOException e) {
        throw new CompletionException("Failed to commit stream write on file:" + p +
        ", expected written bytes:" + bytesWritten + ", actual written bytes:" + len, e);
      }
    }, committer);
  }

  CompletableFuture<?> streamLink(DataStream dataStream) {
    return CompletableFuture.supplyAsync(() -> {
      if (dataStream == null) {
        return JavaUtils.completeExceptionally(new IllegalStateException("Null stream"));
      }
      if (dataStream.getDataChannel().isOpen()) {
        return JavaUtils.completeExceptionally(
            new IllegalStateException("DataStream: " + dataStream + " is not closed properly"));
      } else {
        return CompletableFuture.completedFuture(null);
      }
    }, committer);
  }

  public CompletableFuture<FileStoreDataChannel> createDataChannel(String p) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        final Path full = resolve(normalize(p));
        return new FileStoreDataChannel(full);
      } catch (IOException e) {
        throw new CompletionException("Failed to create " + p, e);
      }
    }, writer);
  }

  static class FileStoreDataChannel implements StateMachine.DataChannel {
    private final Path path;
    private final RandomAccessFile randomAccessFile;

    FileStoreDataChannel(Path path) throws FileNotFoundException {
      this.path = path;
      this.randomAccessFile = new RandomAccessFile(path.toFile(), "rw");
    }

    @Override
    public void force(boolean metadata) throws IOException {
      LOG.debug("force({}) at {}", metadata, path);
      randomAccessFile.getChannel().force(metadata);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      return randomAccessFile.getChannel().write(src);
    }

    @Override
    public boolean isOpen() {
      return randomAccessFile.getChannel().isOpen();
    }

    @Override
    public void close() throws IOException {
      randomAccessFile.close();
    }
  }
}
