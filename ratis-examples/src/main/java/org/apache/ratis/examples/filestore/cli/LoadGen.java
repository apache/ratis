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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Subcommand to generate load in filestore state machine.
 */
@Parameters(commandDescription = "Load Generator for FileStore")
public class LoadGen extends Client {

  @Parameter(names = {"--sync"}, description = "Whether sync every bufferSize", required = true)
  private int sync = 0;

  @Override
  protected void operation(RaftClient client) throws IOException {
    List<String> paths = generateFiles();
    FileStoreClient fileStoreClient = new FileStoreClient(client);
    System.out.println("Starting Async write now ");

    long startTime = System.currentTimeMillis();

    long totalWrittenBytes = waitWriteFinish(writeByHeapByteBuffer(paths, fileStoreClient));

    long endTime = System.currentTimeMillis();

    System.out.println("Total files written: " + getNumFiles());
    System.out.println("Each files size: " + getFileSizeInBytes());
    System.out.println("Total data written: " + totalWrittenBytes + " bytes");
    System.out.println("Total time taken: " + (endTime - startTime) + " millis");

    stop(client);
  }

  private Map<String, List<CompletableFuture<Long>>> writeByHeapByteBuffer(
      List<String> paths, FileStoreClient fileStoreClient) throws IOException {
    Map<String, List<CompletableFuture<Long>>> fileMap = new HashMap<>();

    for(String path : paths) {
      List<CompletableFuture<Long>> futures = new ArrayList<>();
      File file = new File(path);
      FileInputStream fis = new FileInputStream(file);

      int bytesToRead = getBufferSizeInBytes();
      if (getFileSizeInBytes() > 0L && getFileSizeInBytes() < (long)getBufferSizeInBytes()) {
        bytesToRead = getFileSizeInBytes();
      }

      byte[] buffer = new byte[bytesToRead];
      long offset = 0L;
      while(fis.read(buffer, 0, bytesToRead) > 0) {
        ByteBuffer b = ByteBuffer.wrap(buffer);
        futures.add(fileStoreClient.writeAsync(path, offset, offset + bytesToRead == getFileSizeInBytes(), b,
            sync == 1));
        offset += bytesToRead;
        bytesToRead = (int)Math.min(getFileSizeInBytes() - offset, getBufferSizeInBytes());
        if (bytesToRead > 0) {
          buffer = new byte[bytesToRead];
        }
      }

      fileMap.put(path, futures);
    }

    return fileMap;
  }

  private long waitWriteFinish(Map<String, List<CompletableFuture<Long>>> fileMap) {
    long totalBytes = 0;
    for (List<CompletableFuture<Long>> futures : fileMap.values()) {
      long writtenLen = 0;
      for (CompletableFuture<Long> future : futures) {
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
