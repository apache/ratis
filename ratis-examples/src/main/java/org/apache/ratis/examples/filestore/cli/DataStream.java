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
package org.apache.ratis.examples.filestore.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.examples.filestore.FileStoreClient;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

/**
 * Subcommand to generate load in filestore data stream state machine.
 */
@Parameters(commandDescription = "Load Generator for FileStore DataStream")
public class DataStream extends Client {
  enum Type {
    DirectByteBuffer(DirectByteBufferType::new),
    MappedByteBuffer(MappedByteBufferType::new),
    NettyFileRegion(NettyFileRegionType::new);

    private final BiFunction<String, DataStream, TransferType> constructor;

    Type(BiFunction<String, DataStream, TransferType> constructor) {
      this.constructor = constructor;
    }

    BiFunction<String, DataStream, TransferType> getConstructor() {
      return constructor;
    }

    static Type valueOfIgnoreCase(String s) {
      for (Type type : values()) {
        if (type.name().equalsIgnoreCase(s)) {
          return type;
        }
      }
      return null;
    }
  }

  // To be used as a Java annotation attribute value
  private static final String DESCRIPTION = "[DirectByteBuffer, MappedByteBuffer, NettyFileRegion]";

  {
    // Assert if the description is correct.
    final String expected = Arrays.asList(Type.values()).toString();
    Preconditions.assertTrue(expected.equals(DESCRIPTION),
        () -> "Unexpected description: " + DESCRIPTION + " does not equal to the expected string " + expected);
  }

  @Parameter(names = {"--type"}, description = DESCRIPTION, required = true)
  private String dataStreamType = Type.NettyFileRegion.name();

  @Parameter(names = {"--syncSize"}, description = "Sync every syncSize, syncSize % bufferSize should be zero," +
      "-1 means on sync", required = true)
  private int syncSize = -1;

  int getSyncSize() {
    return syncSize;
  }

  private boolean checkParam() {
    if (syncSize != -1 && syncSize % getBufferSizeInBytes() != 0) {
      System.err.println("Error: syncSize % bufferSize should be zero");
      return false;
    }

    if (Type.valueOfIgnoreCase(dataStreamType) == null) {
      System.err.println("Error: dataStreamType should be one of " + DESCRIPTION);
      return false;
    }

    return true;
  }

  @Override
  protected void operation(RaftClient client) throws IOException, ExecutionException, InterruptedException {
    if (!checkParam()) {
      stop(client);
    }

    final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
    List<String> paths = generateFiles(executor);
    dropCache();
    FileStoreClient fileStoreClient = new FileStoreClient(client);
    System.out.println("Starting DataStream write now ");

    RoutingTable routingTable = getRoutingTable(Arrays.asList(getPeers()), getPrimary());
    long startTime = System.currentTimeMillis();

    long totalWrittenBytes = waitStreamFinish(streamWrite(paths, fileStoreClient, routingTable, executor));

    long endTime = System.currentTimeMillis();

    System.out.println("Total files written: " + getNumFiles());
    System.out.println("Each files size: " + getFileSizeInBytes());
    System.out.println("Total data written: " + totalWrittenBytes + " bytes");
    System.out.println("Total time taken: " + (endTime - startTime) + " millis");

    stop(client);
  }

  private Map<String, CompletableFuture<List<CompletableFuture<DataStreamReply>>>> streamWrite(
      List<String> paths, FileStoreClient fileStoreClient, RoutingTable routingTable,
      ExecutorService executor) {
    Map<String, CompletableFuture<List<CompletableFuture<DataStreamReply>>>> fileMap = new HashMap<>();

    for(String path : paths) {
      final CompletableFuture<List<CompletableFuture<DataStreamReply>>> future = new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        File file = new File(path);
        final long fileLength = file.length();
        Preconditions.assertTrue(fileLength == getFileSizeInBytes(), "Unexpected file size: expected size is "
            + getFileSizeInBytes() + " but actual size is " + fileLength);

        final Type type = Optional.ofNullable(Type.valueOfIgnoreCase(dataStreamType))
            .orElseThrow(IllegalStateException::new);
        final TransferType writer = type.getConstructor().apply(path, this);
        try {
          future.complete(writer.transfer(fileStoreClient, routingTable));
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
        return future;
      }, executor);
      fileMap.put(path, future);
    }
    return fileMap;
  }

  private long waitStreamFinish(Map<String, CompletableFuture<List<CompletableFuture<DataStreamReply>>>> fileMap)
      throws ExecutionException, InterruptedException {
    long totalBytes = 0;
    for (CompletableFuture<List<CompletableFuture<DataStreamReply>>> futures : fileMap.values()) {
      long writtenLen = 0;
      for (CompletableFuture<DataStreamReply> future : futures.get()) {
        writtenLen += future.join().getBytesWritten();
      }

      if (writtenLen != getFileSizeInBytes()) {
        System.out.println("File written:" + writtenLen + " does not match expected:" + getFileSizeInBytes());
      }

      totalBytes += writtenLen;
    }
    return totalBytes;
  }

  abstract static class TransferType {
    private final String path;
    private final File file;
    private final long fileSize;
    private final int bufferSize;
    private final long syncSize;
    private long syncPosition = 0;

    TransferType(String path, DataStream cli) {
      this.path = path;
      this.file = new File(path);
      this.fileSize = cli.getFileSizeInBytes();
      this.bufferSize = cli.getBufferSizeInBytes();
      this.syncSize = cli.getSyncSize();

      final long actualSize = file.length();
      Preconditions.assertTrue(actualSize == fileSize, () -> "Unexpected file size: expected size is "
          + fileSize + " but actual size is " + actualSize + ", path=" + path);
    }

    File getFile() {
      return file;
    }

    int getBufferSize() {
      return bufferSize;
    }

    long getPacketSize(long offset) {
      return Math.min(bufferSize, fileSize - offset);
    }

    boolean isSync(long position) {
      if (syncSize > 0) {
        if (position >= fileSize || position - syncPosition >= syncSize) {
          syncPosition = position;
          return true;
        }
      }
      return false;
    }

    List<CompletableFuture<DataStreamReply>> transfer(
        FileStoreClient client, RoutingTable routingTable) throws IOException {
      if (fileSize <= 0) {
        return Collections.emptyList();
      }

      final List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
      final DataStreamOutput out = client.getStreamOutput(file.getName(), fileSize, routingTable);
      try (FileInputStream fis = new FileInputStream(file)) {
        final FileChannel in = fis.getChannel();
        for (long offset = 0L; offset < fileSize; ) {
          offset += write(in, out, offset, futures);
        }
      } catch (Throwable e) {
        throw new IOException("Failed to transfer " + path);
      } finally {
        futures.add(out.closeAsync());
      }
      return futures;
    }

    abstract long write(FileChannel in, DataStreamOutput out, long offset,
        List<CompletableFuture<DataStreamReply>> futures) throws IOException;

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + "{" + path + ", size=" + fileSize + "}";
    }
  }

  static class DirectByteBufferType extends TransferType {
    DirectByteBufferType(String path, DataStream cli) {
      super(path, cli);
    }

    @Override
    long write(FileChannel in, DataStreamOutput out, long offset, List<CompletableFuture<DataStreamReply>> futures)
        throws IOException {
      final int bufferSize = getBufferSize();
      final ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(bufferSize);
      final int bytesRead = buf.writeBytes(in, bufferSize);
      if (bytesRead < 0) {
        throw new IllegalStateException("Failed to read " + bufferSize + " byte(s) from " + this
            + ". The channel has reached end-of-stream at " + offset);
      } else if (bytesRead > 0) {
        final CompletableFuture<DataStreamReply> f = isSync(offset + bytesRead) ?
            out.writeAsync(buf.nioBuffer(), StandardWriteOption.SYNC) : out.writeAsync(buf.nioBuffer());
        f.thenRun(buf::release);
        futures.add(f);
      }
      return bytesRead;
    }
  }

  static class MappedByteBufferType extends TransferType {
    MappedByteBufferType(String path, DataStream cli) {
      super(path, cli);
    }

    @Override
    long write(FileChannel in, DataStreamOutput out, long offset, List<CompletableFuture<DataStreamReply>> futures)
        throws IOException {
      final long packetSize = getPacketSize(offset);
      final MappedByteBuffer mappedByteBuffer = in.map(FileChannel.MapMode.READ_ONLY, offset, packetSize);
      final int remaining = mappedByteBuffer.remaining();
      futures.add(isSync(offset + remaining) ?
          out.writeAsync(mappedByteBuffer, StandardWriteOption.SYNC) : out.writeAsync(mappedByteBuffer));
      return remaining;
    }
  }

  static class NettyFileRegionType extends TransferType {
    NettyFileRegionType(String path, DataStream cli) {
      super(path, cli);
    }

    @Override
    long write(FileChannel in, DataStreamOutput out, long offset, List<CompletableFuture<DataStreamReply>> futures) {
      final long packetSize = getPacketSize(offset);
      futures.add(isSync(offset + packetSize) ?
          out.writeAsync(getFile(), offset, packetSize, StandardWriteOption.SYNC) :
          out.writeAsync(getFile(), offset, packetSize));
      return packetSize;
    }
  }
}
