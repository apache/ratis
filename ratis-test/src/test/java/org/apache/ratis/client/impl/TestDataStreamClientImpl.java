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
import org.apache.ratis.client.api.DataStreamInput;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.DataStreamObserver;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuf;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class TestDataStreamClientImpl {
  private static RaftPeer newPeer(String id) {
    return RaftPeer.newBuilder().setId(id).build();
  }

  private static class RecordingDataStreamClientRpc implements DataStreamClientRpc {
    private final AtomicReference<RaftClientRequest> request = new AtomicReference<>();
    private final AtomicReference<DataStreamObserver<DataStreamReplyByteBuf>> replyHandler = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<DataStreamReply>> replyFuture = new AtomicReference<>();

    @Override
    public CompletableFuture<DataStreamReply> streamAsync(
        DataStreamRequest dataStreamRequest, DataStreamObserver<DataStreamReplyByteBuf> replyHandler) {
      try {
        final ByteBuffer buffer = ((DataStreamRequestByteBuffer) dataStreamRequest).slice();
        request.set(ClientProtoUtils.toRaftClientRequest(RaftClientRequestProto.parseFrom(buffer)));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      this.replyHandler.set(replyHandler);
      final CompletableFuture<DataStreamReply> future = new CompletableFuture<>();
      replyFuture.set(future);
      return future;
    }

    RaftClientRequest getRequest() {
      return request.get();
    }

    void receive(DataStreamReplyByteBuf reply) {
      try (DataStreamReplyByteBuf r = reply) {
        replyHandler.get().onNext(r);
      }
    }

    void complete() {
      replyHandler.get().onCompleted();
      replyFuture.get().complete(null);
    }

    void completeExceptionally(Throwable cause) {
      replyHandler.get().onError(cause);
      replyFuture.get().completeExceptionally(cause);
    }

    @Override
    public void close() {
    }
  }

  private static DataStreamClient newDataStreamClient(
      RaftPeer dataStreamServer, RecordingDataStreamClientRpc dataStreamClientRpc) {
    final RaftProperties properties = new RaftProperties();
    return new DataStreamClientImpl(
        ClientId.randomId(), RaftGroupId.randomId(), dataStreamServer, dataStreamClientRpc, properties);
  }

  @Test
  public void testReceiveSkipsCancelledPendingRead() throws Exception {
    final RaftPeer follower = newPeer("follower");
    final RecordingDataStreamClientRpc dataStreamClientRpc = new RecordingDataStreamClientRpc();

    try (DataStreamClient dataStreamClient = newDataStreamClient(follower, dataStreamClientRpc);
         DataStreamInput input = dataStreamClient.streamReadOnly(ByteBuffer.wrap(new byte[] {1}))) {
      final CompletableFuture<DataStreamReply> cancelled = input.readAsync();
      final CompletableFuture<DataStreamReply> active = input.readAsync();
      cancelled.cancel(false);
      Assertions.assertEquals(follower.getId(), dataStreamClientRpc.getRequest().getServerId());

      final DataStreamReplyByteBuf reply = DataStreamReplyByteBuf.newBuilder()
          .setClientId(ClientId.randomId())
          .setType(Type.STREAM_DATA)
          .setStreamId(1)
          .setStreamOffset(0)
          .setBuf(Unpooled.EMPTY_BUFFER)
          .setSuccess(true)
          .build();
      dataStreamClientRpc.receive(reply);

      Assertions.assertTrue(active.isDone());
      final DataStreamReply received = active.getNow(null);
      Assertions.assertInstanceOf(DataStreamReplyByteBuffer.class, received);
      Assertions.assertEquals(Type.STREAM_DATA, received.getType());
    }
  }

  @Test
  public void testReadOnlyInputCompletesPendingReadOnCompleted() throws Exception {
    final RaftPeer follower = newPeer("follower");
    final RecordingDataStreamClientRpc dataStreamClientRpc = new RecordingDataStreamClientRpc();

    try (DataStreamClient dataStreamClient = newDataStreamClient(follower, dataStreamClientRpc);
         DataStreamInput input = dataStreamClient.streamReadOnly(ByteBuffer.wrap(new byte[] {1}))) {
      final CompletableFuture<DataStreamReply> pending = input.readAsync();

      dataStreamClientRpc.complete();

      assertFutureCause(pending, EOFException.class);
      assertFutureCause(input.readAsync(), EOFException.class);
    }
  }

  @Test
  public void testReadOnlyInputNotifiesPendingReadOnError() throws Exception {
    final RaftPeer follower = newPeer("follower");
    final RecordingDataStreamClientRpc dataStreamClientRpc = new RecordingDataStreamClientRpc();

    try (DataStreamClient dataStreamClient = newDataStreamClient(follower, dataStreamClientRpc);
         DataStreamInput input = dataStreamClient.streamReadOnly(ByteBuffer.wrap(new byte[] {1}))) {
      final CompletableFuture<DataStreamReply> pending = input.readAsync();
      final Throwable cause = new IllegalStateException("test");

      dataStreamClientRpc.completeExceptionally(cause);

      Assertions.assertSame(cause, assertFutureCause(pending, IllegalStateException.class));
      Assertions.assertSame(cause, assertFutureCause(input.readAsync(), IllegalStateException.class));
    }
  }

  private static <T extends Throwable> Throwable assertFutureCause(
      CompletableFuture<?> future, Class<T> expectedCauseClass) {
    final ExecutionException exception = Assertions.assertThrows(ExecutionException.class, future::get);
    return Assertions.assertInstanceOf(expectedCauseClass, exception.getCause());
  }
}
