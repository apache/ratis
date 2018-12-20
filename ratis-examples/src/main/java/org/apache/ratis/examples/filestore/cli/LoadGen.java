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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.examples.filestore.FileStoreClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Subcommand to generate load in filestore state machine.
 */
@Parameters(commandDescription = "Load Generator for FileStore")
public class LoadGen extends Client {

  private static final String UTF8_CSN = StandardCharsets.UTF_8.name();

  @Parameter(names = {"--size"}, description = "Size of each file", required = true)
  String size;

  @Parameter(names = {"--numFiles"}, description = "Number of files", required = true)
  String numFiles;

  private static byte[] string2Bytes(String str) {
    try {
      return str.getBytes(UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("UTF8 decoding is not supported", e);
    }
  }

  @Override
  protected void operation(RaftClient client) throws IOException {
    int length = Integer.parseInt(size);
    int num = Integer.parseInt(numFiles);
    AtomicLong totalBytes = new AtomicLong(0);
    String entropy = RandomStringUtils.randomAlphanumeric(10);

    byte[] fileValue = string2Bytes(RandomStringUtils.randomAscii(length));
    FileStoreClient fileStoreClient = new FileStoreClient(client);

    System.out.println("Starting load now ");
    long startTime = System.currentTimeMillis();
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      String path = "file-" + entropy + "-" + i;
      ByteBuffer b = ByteBuffer.wrap(fileValue);
      futures.add(fileStoreClient.writeAsync(path, 0, true, b));
    }

    for (CompletableFuture<Long> future : futures) {
      Long writtenLen = future.join();
      totalBytes.addAndGet(writtenLen);
      if (writtenLen != length) {
        System.out.println("File length written is wrong: " + writtenLen + length);
      }
    }
    long endTime = System.currentTimeMillis();

    System.out.println("Total files written: " + futures.size());
    System.out.println("Each files size: " + length);
    System.out.println("Total data written: " + totalBytes + " bytes");
    System.out.println("Total time taken: " + (endTime - startTime) + " millis");

    client.close();
    System.exit(0);
  }
}
