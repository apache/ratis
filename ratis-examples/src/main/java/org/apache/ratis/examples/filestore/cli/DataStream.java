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
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.util.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Subcommand to generate load in filestore data stream state machine.
 */
@Parameters(commandDescription = "Load Generator for FileStore DataStream")
public class DataStream extends Client {

  @Parameter(names = {"--type"}, description = "DirectByteBuffer, MappedByteBuffer, NettyFileRegion", required = true)
  private String dataStreamType = "NettyFileRegion";

  @Override
  protected void operation(RaftClient client) throws IOException {
    List<String> paths = generateFiles();
    FileStoreClient fileStoreClient = new FileStoreClient(client);
    System.out.println("Starting DataStream write now ");

    long startTime = System.currentTimeMillis();

    long totalWrittenBytes = waitStreamFinish(streamWrite(paths, fileStoreClient));

    long endTime = System.currentTimeMillis();

    System.out.println("Total files written: " + getNumFiles());
    System.out.println("Each files size: " + getFileSizeInBytes());
    System.out.println("Total data written: " + totalWrittenBytes + " bytes");
    System.out.println("Total time taken: " + (endTime - startTime) + " millis");

    client.close();
    System.exit(0);
  }

  private Map<String, List<CompletableFuture<DataStreamReply>>> streamWrite(
      List<String> paths, FileStoreClient fileStoreClient) throws IOException {
    Map<String, List<CompletableFuture<DataStreamReply>>> fileMap = new HashMap<>();
    for(String path : paths) {
      File file = new File(path);
      final long fileLength = file.length();
      Preconditions.assertTrue(fileLength == getFileSizeInBytes(), "Unexpected file size: expected size is "
          + getFileSizeInBytes() + " but actual size is " + fileLength);
      FileInputStream fis = new FileInputStream(file);
      final DataStreamOutput dataStreamOutput = fileStoreClient.getStreamOutput(path, (int) file.length());

      if (dataStreamType.equals("DirectByteBuffer")) {
        fileMap.put(path, writeByDirectByteBuffer(dataStreamOutput, fis.getChannel()));
      } else if (dataStreamType.equals("MappedByteBuffer")) {
        fileMap.put(path, writeByMappedByteBuffer(dataStreamOutput, fis.getChannel()));
      } else if (dataStreamType.equals("NettyFileRegion")) {
        fileMap.put(path, writeByNettyFileRegion(dataStreamOutput, file));
      } else {
        System.err.println("Error: dataStreamType should be one of DirectByteBuffer, MappedByteBuffer, transferTo");
      }
    }
    return fileMap;
  }

  private long waitStreamFinish(Map<String, List<CompletableFuture<DataStreamReply>>> fileMap) {
    long totalBytes = 0;
    for (List<CompletableFuture<DataStreamReply>> futures : fileMap.values()) {
      long writtenLen = 0;
      for (CompletableFuture<DataStreamReply> future : futures) {
        writtenLen += future.join().getBytesWritten();
      }

      if (writtenLen != getFileSizeInBytes()) {
        System.out.println("File written:" + writtenLen + " does not match expected:" + getFileSizeInBytes());
      }

      totalBytes += writtenLen;
    }
    return totalBytes;
  }

  private List<CompletableFuture<DataStreamReply>> writeByDirectByteBuffer(DataStreamOutput dataStreamOutput,
      FileChannel fileChannel) throws IOException {
    final int fileSize = getFileSizeInBytes();
    final int bufferSize = getBufferSizeInBytes();
    if (fileSize <= 0) {
      return Collections.emptyList();
    }
    List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
    final ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;

    for(long offset = 0L; offset < fileSize;) {
      final ByteBuf buf = alloc.directBuffer(bufferSize);
      final int bytesRead = buf.writeBytes(fileChannel, bufferSize);
      if (bytesRead < 0) {
        throw new IllegalStateException("Failed to read " + fileSize
            + " byte(s). The channel has reached end-of-stream at " + offset);
      } else if (bytesRead > 0) {
        offset += bytesRead;

        final CompletableFuture<DataStreamReply> f = dataStreamOutput.writeAsync(buf.nioBuffer(), offset == fileSize);
        f.thenRun(buf::release);
        futures.add(f);
      }
    }

    return futures;
  }

  private List<CompletableFuture<DataStreamReply>> writeByMappedByteBuffer(DataStreamOutput dataStreamOutput,
      FileChannel fileChannel) throws IOException {
    List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
    MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, getFileSizeInBytes());
    futures.add(dataStreamOutput.writeAsync(mappedByteBuffer, true));
    return futures;
  }

  private List<CompletableFuture<DataStreamReply>> writeByNettyFileRegion(
      DataStreamOutput dataStreamOutput, File file) {
    List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
    futures.add(dataStreamOutput.writeAsync(file));
    return futures;
  }
}
