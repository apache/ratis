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
import org.apache.ratis.io.FilePositionCount;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.SlidingWindow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;
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
    this.orderedStreamAsync = new OrderedStreamAsync(dataStreamClientRpc, properties);
  }

  public final class DataStreamOutputImpl implements DataStreamOutputRpc {
    private final RaftClientRequest header;
    private final CompletableFuture<DataStreamReply> headerFuture;
    private final SlidingWindow.Client<OrderedStreamAsync.DataStreamWindowRequest, DataStreamReply> slidingWindow;
    private final CompletableFuture<RaftClientReply> raftClientReplyFuture = new CompletableFuture<>();
    private CompletableFuture<DataStreamReply> closeFuture;
    private final MemoizedSupplier<WritableByteChannel> writableByteChannelSupplier
        = JavaUtils.memoize(() -> new WritableByteChannel() {
      @Override
      public int write(ByteBuffer src) throws IOException {
        final int remaining = src.remaining();
        final DataStreamReply reply = IOUtils.getFromFuture(writeAsync(src),
            () -> "write(" + remaining + " bytes for " + ClientInvocationId.valueOf(header) + ")");
        return Math.toIntExact(reply.getBytesWritten());
      }

      @Override
      public boolean isOpen() {
        return !isClosed();
      }

      @Override
      public void close() throws IOException {
        if (isClosed()) {
          return;
        }
        IOUtils.getFromFuture(writeAsync(DataStreamPacketByteBuffer.EMPTY_BYTE_BUFFER, StandardWriteOption.CLOSE),
            () -> "close(" + ClientInvocationId.valueOf(header) + ")");
      }
    });

    private long streamOffset = 0;

    private DataStreamOutputImpl(RaftClientRequest request) {
      this.header = request;
      this.slidingWindow = new SlidingWindow.Client<>(ClientInvocationId.valueOf(clientId, header.getCallId()));
      final ByteBuffer buffer = ClientProtoUtils.toRaftClientRequestProtoByteBuffer(header);
      this.headerFuture = send(Type.STREAM_HEADER, buffer, buffer.remaining());
    }

    private CompletableFuture<DataStreamReply> send(Type type, Object data, long length, WriteOption... options) {
      final DataStreamRequestHeader h =
          new DataStreamRequestHeader(header.getClientId(), type, header.getCallId(), streamOffset, length, options);
      return orderedStreamAsync.sendRequest(h, data, slidingWindow);
    }

    private CompletableFuture<DataStreamReply> combineHeader(CompletableFuture<DataStreamReply> future) {
      return future.thenCombine(headerFuture, (reply, headerReply) -> headerReply.isSuccess()? reply : headerReply);
    }

    private CompletableFuture<DataStreamReply> writeAsyncImpl(Object data, long length, WriteOption... options) {
      if (isClosed()) {
        return JavaUtils.completeExceptionally(new AlreadyClosedException(
            clientId + ": stream already closed, request=" + header));
      }
      final CompletableFuture<DataStreamReply> f = combineHeader(send(Type.STREAM_DATA, data, length, options));
      if (WriteOption.containsOption(options, StandardWriteOption.CLOSE)) {
        closeFuture = f;
        f.thenApply(ClientProtoUtils::getRaftClientReply).whenComplete(JavaUtils.asBiConsumer(raftClientReplyFuture));
      }
      streamOffset += length;
      return f;
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(ByteBuffer src, WriteOption... options) {
      return writeAsyncImpl(src, src.remaining(), options);
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(FilePositionCount src, WriteOption... options) {
      return writeAsyncImpl(src, src.getCount(), options);
    }

    boolean isClosed() {
      return closeFuture != null;
    }

    @Override
    public CompletableFuture<DataStreamReply> closeAsync() {
      return isClosed() ? closeFuture :
          writeAsync(DataStreamPacketByteBuffer.EMPTY_BYTE_BUFFER, StandardWriteOption.CLOSE);
    }

    public RaftClientRequest getHeader() {
      return header;
    }

    @Override
    public CompletableFuture<DataStreamReply> getHeaderFuture() {
      return headerFuture;
    }

    @Override
    public CompletableFuture<RaftClientReply> getRaftClientReplyFuture() {
      return raftClientReplyFuture;
    }

    @Override
    public WritableByteChannel getWritableByteChannel() {
      return writableByteChannelSupplier.get();
    }
  }

  @Override
  public DataStreamClientRpc getClientRpc() {
    return dataStreamClientRpc;
  }

  @Override
  public DataStreamOutputRpc stream(RaftClientRequest request) {
    return new DataStreamOutputImpl(request);
  }

  @Override
  public DataStreamOutputRpc stream(ByteBuffer headerMessage) {
    return stream(headerMessage, null);
  }

  @Override
  public DataStreamOutputRpc stream(ByteBuffer headerMessage, RoutingTable routingTable) {
    final Message message =
        Optional.ofNullable(headerMessage).map(ByteString::copyFrom).map(Message::valueOf).orElse(null);
    RaftClientRequest request = RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(dataStreamServer.getId())
        .setGroupId(groupId)
        .setCallId(CallId.getAndIncrement())
        .setMessage(message)
        .setType(RaftClientRequest.dataStreamRequestType())
        .setRoutingTable(routingTable)
        .build();
    return new DataStreamOutputImpl(request);
  }

  @Override
  public void close() throws IOException {
    dataStreamClientRpc.close();
  }
}
