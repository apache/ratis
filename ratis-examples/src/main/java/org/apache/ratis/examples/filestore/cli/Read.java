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
package org.apache.ratis.examples.filestore.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.examples.common.ExampleLauncher;
import org.apache.ratis.examples.common.SubCommandBase;
import org.apache.ratis.examples.filestore.FileStoreClient;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Subcommand to read files from FileStore using streaming read.
 */
@Parameters(commandDescription = "Read files from FileStore using streaming read")
public class Read extends SubCommandBase {

  @Parameter(names = {"--files"}, description = "Comma-separated file names on the server", required = true)
  private String files;

  @Parameter(names = {"--size"}, description = "Number of bytes to read per file", required = true)
  private long fileSizeInBytes;

  @Parameter(names = {"--offset"}, description = "Offset to start reading from", required = false)
  private long offset;

  @Override
  public void run() throws Exception {
    final List<String> fileNames = Arrays.asList(files.split(","));
    if (fileNames.isEmpty()) {
      throw new IllegalArgumentException("No files specified in --files");
    }

    final RaftProperties raftProperties = newRaftProperties();

    System.out.println("Starting streaming read now");

    final long startTime = System.currentTimeMillis();
    long totalReadBytes = 0;
    final List<ReadResult> results = new ArrayList<>();
    try (FileStoreClient client = newClient(raftProperties)) {
      for (String fileName : fileNames) {
        final String name = fileName.trim();
        final ReadResult result = readFile(client, name);
        results.add(result);
        if (result.bytesRead != fileSizeInBytes) {
          System.err.println("Error: file:" + name + " read:" + result.bytesRead
              + " mismatch expected size:" + fileSizeInBytes);
        }
        totalReadBytes += result.bytesRead;
      }
    }

    final long endTime = System.currentTimeMillis();
    System.out.println("Total files read: " + fileNames.size());
    for (ReadResult result : results) {
      System.out.println("  " + result.fileName + " -> " + result.tempFile
          + " (" + result.bytesRead + " bytes)");
    }
    System.out.println("Each file size: " + fileSizeInBytes);
    System.out.println("Total data read: " + totalReadBytes + " bytes");
    System.out.println("Total time taken: " + (endTime - startTime) + " millis");
  }

  private ReadResult readFile(FileStoreClient client, String fileName) throws IOException {
    final File tempFile = Files.createTempFile("filestore-read-", "-" + fileName).toFile();
    try (FileChannel out = FileUtils.newFileChannel(tempFile,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
      final long bytesRead = client.streamRead(fileName, offset, fileSizeInBytes, out);
      return new ReadResult(fileName, tempFile.getAbsolutePath(), bytesRead);
    } finally {
      FileUtils.deleteFully(tempFile);
    }
  }

  private static final class ReadResult {
    private final String fileName;
    private final String tempFile;
    private final long bytesRead;

    private ReadResult(String fileName, String tempFile, long bytesRead) {
      this.fileName = fileName;
      this.tempFile = tempFile;
      this.bytesRead = bytesRead;
    }
  }

  @Override
  protected RaftProperties newRaftProperties() {
    final int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;
    final RaftProperties raftProperties = ExampleLauncher.newRaftProperties();
    RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
    GrpcConfigKeys.setMessageSizeMax(raftProperties, SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties, SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);
    RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
        TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
    return raftProperties;
  }

  private FileStoreClient newClient(RaftProperties raftProperties) throws IOException {
    final RaftGroup raftGroup = RaftGroup.valueOf(
        RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())), getPeers());
    final RaftClient client = RaftClient.newBuilder()
        .setProperties(raftProperties)
        .setRaftGroup(raftGroup)
        .setClientRpc(new GrpcFactory(new org.apache.ratis.conf.Parameters())
            .newRaftClientRpc(ClientId.randomId(), raftProperties))
        .setPrimaryDataStreamServer(getPeers()[0])
        .build();
    return new FileStoreClient(client);
  }
}
