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

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.DataStreamClientFactory;
import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Streaming client implementation
 * allows client to create streams and send asynchronously.
 */
public class DataStreamClientImpl implements DataStreamClient {
  // TODO Similar to RaftClientImpl, pass ClientId and RaftGroupId/RaftGroup in constructor.
  private final ClientId clientId = ClientId.randomId();
  private final RaftGroupId groupId =  RaftGroupId.randomId();

  private final RaftPeer raftServer;
  private final DataStreamClientRpc dataStreamClientRpc;
  private final OrderedStreamAsync orderedStreamAsync;

  public DataStreamClientImpl(RaftPeer server, RaftProperties properties, Parameters parameters) {
    this.raftServer = Objects.requireNonNull(server, "server == null");

    final SupportedDataStreamType type = RaftConfigKeys.DataStream.type(properties, LOG::info);
    this.dataStreamClientRpc = DataStreamClientFactory.newInstance(type, parameters)
                               .newDataStreamClientRpc(raftServer, properties);

    this.orderedStreamAsync = new OrderedStreamAsync(clientId, dataStreamClientRpc, properties);
  }

  public class DataStreamOutputImpl implements DataStreamOutput {
    private final RaftClientRequest header;
    private final CompletableFuture<DataStreamReply> headerFuture;

    private long streamOffset = 0;

    public DataStreamOutputImpl(RaftGroupId groupId) {
      final long streamId = RaftClientImpl.nextCallId();
      this.header = new RaftClientRequest(clientId, raftServer.getId(), groupId, streamId,
          RaftClientRequest.writeRequestType());
      this.headerFuture = orderedStreamAsync.sendRequest(streamId, -1,
          ClientProtoUtils.toRaftClientRequestProto(header).toByteString().asReadOnlyByteBuffer(), Type.STREAM_HEADER);
    }

    long getStreamId() {
      return header.getCallId();
    }

    // send to the attached dataStreamClientRpc
    @Override
    public CompletableFuture<DataStreamReply> writeAsync(ByteBuffer buf) {
      final CompletableFuture<DataStreamReply> f = orderedStreamAsync.sendRequest(getStreamId(), streamOffset, buf,
          Type.STREAM_DATA);
      streamOffset += buf.remaining();
      return f;
    }

    @Override
    public CompletableFuture<DataStreamReply> closeAsync() {
      return orderedStreamAsync.sendRequest(getStreamId(), streamOffset, Unpooled.EMPTY_BUFFER.nioBuffer(),
          Type.STREAM_CLOSE);
    }

    @Override
    public CompletableFuture<DataStreamReply> closeForwardAsync() {
      return orderedStreamAsync.sendRequest(getStreamId(), streamOffset, Unpooled.EMPTY_BUFFER.nioBuffer(),
          Type.STREAM_CLOSE_FORWARD);
    }

    @Override
    public CompletableFuture<DataStreamReply> startTransactionAsync() {
      return orderedStreamAsync.sendRequest(getStreamId(), streamOffset, Unpooled.EMPTY_BUFFER.nioBuffer(),
          Type.START_TRANSACTION);
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
  public DataStreamOutput stream() {
    return stream(groupId);
  }

  @Override
  public DataStreamOutput stream(RaftGroupId gid) {
    return new DataStreamOutputImpl(gid);
  }

  @Override
  public void close() throws IOException {
    dataStreamClientRpc.close();
  }
}
