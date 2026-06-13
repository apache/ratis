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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.api.DataStreamInput;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicies;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class TestDataStreamClientImpl {
  private static RaftPeer newPeer(String id) {
    return RaftPeer.newBuilder().setId(id).build();
  }

  private static class NoOpRaftClientRpc implements RaftClientRpc {
    @Override
    public void addRaftPeers(Collection<RaftPeer> peers) {
    }

    @Override
    public RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }
  }

  private static class RecordingDataStreamClientRpc implements DataStreamClientRpc {
    private final AtomicReference<RaftClientRequest> request = new AtomicReference<>();

    @Override
    public CompletableFuture<DataStreamReply> streamAsync(
        DataStreamRequest dataStreamRequest, Consumer<DataStreamReply> replyConsumer) {
      try {
        final ByteBuffer buffer = ((DataStreamRequestByteBuffer) dataStreamRequest).slice();
        request.set(ClientProtoUtils.toRaftClientRequest(RaftClientRequestProto.parseFrom(buffer)));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      return new CompletableFuture<>();
    }

    RaftClientRequest getRequest() {
      return request.get();
    }

    @Override
    public void close() {
    }
  }

  @Test
  public void testStreamReadOnlyUsesPrimaryDataStreamServer() throws Exception {
    final RaftProperties properties = new RaftProperties();
    final RaftPeer leader = newPeer("leader");
    final RaftPeer follower = newPeer("follower");
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), leader, follower);
    final RecordingDataStreamClientRpc dataStreamClientRpc = new RecordingDataStreamClientRpc();

    try (RaftClient client = RaftClient.newBuilder()
        .setClientId(ClientId.randomId())
        .setRaftGroup(group)
        .setLeaderId(leader.getId())
        .setPrimaryDataStreamServer(follower)
        .setClientRpc(new NoOpRaftClientRpc())
        .setProperties(properties)
        .setRetryPolicy(RetryPolicies.noRetry())
        .build();
         DataStreamClient dataStreamClient = DataStreamClient.newBuilder(client)
             .setDataStreamServer(follower)
             .setDataStreamClientRpc(dataStreamClientRpc)
             .setProperties(properties)
             .build();
         DataStreamInput input = dataStreamClient.streamReadOnly(ByteBuffer.wrap(new byte[] {1}))) {
      Assertions.assertNotNull(input);
      Assertions.assertEquals(follower.getId(), dataStreamClientRpc.getRequest().getServerId());
    }
  }
}
