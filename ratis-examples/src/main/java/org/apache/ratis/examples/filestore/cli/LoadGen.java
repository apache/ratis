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
import org.apache.ratis.examples.filestore.FileStoreClient;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Subcommand to generate load in filestore state machine.
 */
@Parameters(commandDescription = "Load Generator for FileStore")
public class LoadGen extends Client {

  @Parameter(names = {"--sync"}, description = "Whether sync every bufferSize", required = false)
  private int sync = 0;

  @Override
  protected void operation(RaftClient client) throws IOException, ExecutionException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
    List<String> paths = generateFiles(executor);
    dropCache();
    FileStoreClient fileStoreClient = new FileStoreClient(client);
    System.out.println("Starting Async write now ");

    long startTime = System.currentTimeMillis();

    long totalWrittenBytes = waitWriteFinish(writeByHeapByteBuffer(paths, fileStoreClient, executor));

    long endTime = System.currentTimeMillis();

    System.out.println("Total files written: " + getNumFiles());
    System.out.println("Each files size: " + getFileSizeInBytes());
    System.out.println("Total data written: " + totalWrittenBytes + " bytes");
    System.out.println("Total time taken: " + (endTime - startTime) + " millis");

    stop(client);
  }

  long write(FileChannel in, long offset, FileStoreClient fileStoreClient, String path,
      List<CompletableFuture<Long>> futures) throws IOException {
    final int bufferSize = getBufferSizeInBytes();
    final ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(bufferSize);
    final int bytesRead = buf.writeBytes(in, bufferSize);

    if (bytesRead < 0) {
      throw new IllegalStateException("Failed to read " + bufferSize + " byte(s) from " + this
          + ". The channel has reached end-of-stream at " + offset);
    } else if (bytesRead > 0) {
      final CompletableFuture<Long> f = fileStoreClient.writeAsync(
          path, offset, offset + bytesRead == getFileSizeInBytes(), buf.nioBuffer(),
          sync == 1);
      f.thenRun(buf::release);
      futures.add(f);
    }
    return bytesRead;
  }

  private Map<String, CompletableFuture<List<CompletableFuture<Long>>>> writeByHeapByteBuffer(
      List<String> paths, FileStoreClient fileStoreClient, ExecutorService executor) {
    Map<String, CompletableFuture<List<CompletableFuture<Long>>>> fileMap = new HashMap<>();

    for(String path : paths) {
      final CompletableFuture<List<CompletableFuture<Long>>> future = new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        File file = new File(path);
        try (FileInputStream fis = new FileInputStream(file)) {
          final FileChannel in = fis.getChannel();
          for (long offset = 0L; offset < getFileSizeInBytes(); ) {
            offset += write(in, offset, fileStoreClient, file.getName(), futures);
          }
        } catch (Throwable e) {
          future.completeExceptionally(e);
        }

        future.complete(futures);
        return future;
      }, executor);

      fileMap.put(path, future);
    }

    return fileMap;
  }

  private long waitWriteFinish(Map<String, CompletableFuture<List<CompletableFuture<Long>>>> fileMap)
      throws ExecutionException, InterruptedException {
    long totalBytes = 0;
    for (CompletableFuture<List<CompletableFuture<Long>>> futures : fileMap.values()) {
      long writtenLen = 0;
      for (CompletableFuture<Long> future : futures.get()) {
        writtenLen += future.join();
      }

      if (writtenLen != getFileSizeInBytes()) {
        System.out.println("File written:" + writtenLen + " does not match expected:" + getFileSizeInBytes());
      }

      totalBytes += writtenLen;
    }
    return totalBytes;
  }
}
