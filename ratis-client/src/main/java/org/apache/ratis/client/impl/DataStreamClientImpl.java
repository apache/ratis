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
import org.apache.ratis.client.DataStreamOutputRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamPacketByteBuffer;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Streaming client implementation
 * allows client to create streams and send asynchronously.
 */
public class DataStreamClientImpl implements DataStreamClient {
  private final ClientId clientId;
  private final RaftGroupId groupId;

  private final RaftPeer dataStreamServer;
  private final DataStreamClientRpc dataStreamClientRpc;
  private final OrderedStreamAsync orderedStreamAsync;

  DataStreamClientImpl(ClientId clientId, RaftGroupId groupId, RaftPeer dataStreamServer,
      DataStreamClientRpc dataStreamClientRpc, RaftProperties properties) {
    this.clientId = clientId;
    this.groupId = groupId;
    this.dataStreamServer = dataStreamServer;
    this.dataStreamClientRpc = dataStreamClientRpc;
    this.orderedStreamAsync = new OrderedStreamAsync(clientId, dataStreamClientRpc, properties);
  }

  public final class DataStreamOutputImpl implements DataStreamOutputRpc {
    private final RaftClientRequest header;
    private final CompletableFuture<DataStreamReply> headerFuture;

    private long streamOffset = 0;

    private DataStreamOutputImpl(RaftClientRequest request) {
      this.header = request;
      this.headerFuture = orderedStreamAsync.sendRequest(request.getCallId(), -1,
          ClientProtoUtils.toRaftClientRequestProto(header).toByteString().asReadOnlyByteBuffer(), Type.STREAM_HEADER);
    }

    private CompletableFuture<DataStreamReply> send(Type type, ByteBuffer buffer) {
      return orderedStreamAsync.sendRequest(header.getCallId(), streamOffset, buffer, type);
    }

    private CompletableFuture<DataStreamReply> send(Type type) {
      return send(type, DataStreamPacketByteBuffer.EMPTY_BYTE_BUFFER);
    }

    // send to the attached dataStreamClientRpc
    @Override
    public CompletableFuture<DataStreamReply> writeAsync(ByteBuffer buf) {
      final CompletableFuture<DataStreamReply> f = send(Type.STREAM_DATA, buf);
      streamOffset += buf.remaining();
      return f;
    }

    @Override
    public CompletableFuture<DataStreamReply> closeAsync() {
      return send(Type.STREAM_CLOSE);
    }

    @Override
    public CompletableFuture<DataStreamReply> closeForwardAsync() {
      return send(Type.STREAM_CLOSE_FORWARD);
    }

    @Override
    public CompletableFuture<DataStreamReply> startTransactionAsync() {
      return send(Type.START_TRANSACTION);
    }

    public RaftClientRequest getHeader() {
      return header;
    }

    @Override
    public CompletableFuture<DataStreamReply> getHeaderFuture() {
      return headerFuture;
    }
  }

  @Override
  public DataStreamClientRpc getClientRpc() {
    return dataStreamClientRpc;
  }

  @Override
  public DataStreamOutputRpc stream() {
    RaftClientRequest request = new RaftClientRequest(
        clientId, dataStreamServer.getId(), groupId, RaftClientImpl.nextCallId(), RaftClientRequest.writeRequestType());
    return new DataStreamOutputImpl(request);
  }

  @Override
  public DataStreamOutputRpc stream(RaftClientRequest request) {
    return new DataStreamOutputImpl(request);
  }

  @Override
  public void close() throws IOException {
    dataStreamClientRpc.close();
  }
}
