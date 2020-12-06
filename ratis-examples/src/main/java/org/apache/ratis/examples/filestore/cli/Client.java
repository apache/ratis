/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples.filestore.cli;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.examples.common.SubCommandBase;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Client to connect filestore example cluster.
 */
public abstract class Client extends SubCommandBase {

  @Parameter(names = {"--size"}, description = "Size of each file in bytes", required = true)
  private int fileSizeInBytes;

  @Parameter(names = {"--bufferSize"}, description = "Size of buffer in bytes, should less than 4MB, " +
      "i.e BUFFER_BYTE_LIMIT_DEFAULT", required = true)
  private int bufferSizeInBytes;

  @Parameter(names = {"--numFiles"}, description = "Number of files to be written", required = true)
  private int numFiles;

  public int getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public int getBufferSizeInBytes() {
    return bufferSizeInBytes;
  }

  public int getNumFiles() {
    return numFiles;
  }

  @Override
  public void run() throws Exception {
    int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;
    RaftProperties raftProperties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
    GrpcConfigKeys.setMessageSizeMax(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
        SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties,
        SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
    RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);

    RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);

    RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
        TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
    RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 1000);


    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
            getPeers());

    RaftClient.Builder builder =
        RaftClient.newBuilder().setProperties(raftProperties);
    builder.setRaftGroup(raftGroup);
    builder.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));
    builder.setPrimaryDataStreamServer(getPrimary());
    RaftClient client = builder.build();

    operation(client);
  }

  public List<String> generateFiles() throws IOException {
    String entropy = RandomStringUtils.randomAlphanumeric(numFiles);
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      String path = "file-" + entropy + "-" + i;
      paths.add(path);
      writeFile(path, fileSizeInBytes, bufferSizeInBytes);
    }

    return paths;
  }

  public void writeFile(String path, int fileSize, int bufferSize) throws IOException {
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(path, "rw");
      int offset = 0;
      while (offset < fileSize) {
        final int remaining = fileSize - offset;
        final int chunkSize = Math.min(remaining, bufferSize);
        byte[] buffer = new byte[chunkSize];
        for (int i = 0; i < chunkSize; i ++) {
          buffer[i]= (byte) ('A' + i % 23);
        }
        raf.write(buffer);
        offset += chunkSize;
      }
    } finally {
      if (raf != null) {
        raf.close();
      }
    }
  }

  protected abstract void operation(RaftClient client) throws IOException;
}
