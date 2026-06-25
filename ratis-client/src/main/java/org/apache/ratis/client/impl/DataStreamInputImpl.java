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

import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.api.DataStreamInput;
import org.apache.ratis.client.api.DataStreamReadChunk;
import org.apache.ratis.datastream.DataStreamObserver;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuf;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

final class DataStreamInputImpl implements DataStreamInput,
    DataStreamObserver<ReferenceCountedObject<DataStreamReply>> {
  private final RaftClientRequest header;
  private final ClientId clientId;
  private final Queue<ReferenceCountedObject<DataStreamReadChunk>> chunks = new LinkedList<>();
  private final Queue<CompletableFuture<ReferenceCountedObject<DataStreamReadChunk>>> pendingReads = new LinkedList<>();

  /*
   * null                  : the stream is open.
   * EOFException          : the stream is ended, i.e. no more data.
   * AlreadyClosedException: the stream is closed by the caller.
   * Other exception       : the stream is failed.
   */
  private Throwable readException;

  DataStreamInputImpl(DataStreamClientRpc dataStreamClientRpc, RaftClientRequest request) {
    this.header = request;
    this.clientId = request.getClientId();
    final ByteBuffer buffer = ClientProtoUtils.toRaftClientRequestProtoByteBuffer(header);
    final DataStreamRequestHeader h = new DataStreamRequestHeader(clientId, Type.STREAM_HEADER,
        header.getCallId(), 0, buffer.remaining(), StandardWriteOption.FLUSH, StandardWriteOption.CLOSE);
    dataStreamClientRpc.streamAsync(new DataStreamRequestByteBuffer(h, buffer), this)
        .whenComplete(asWhenCompleteBiConsumer());
  }

  @Override
  public synchronized void onNext(ReferenceCountedObject<DataStreamReply> reply) {
    if (readException != null) {
      return;
    }

    final ReferenceCountedObject<DataStreamReadChunk> chunkRef = buildAndTransferChunk(reply);
    final CompletableFuture<ReferenceCountedObject<DataStreamReadChunk>> pending = pendingReads.poll();
    if (pending != null) {
      final boolean completed = pending.complete(chunkRef);
      Preconditions.assertTrue(completed);
      return;
    }
    chunks.add(chunkRef);
  }

  @Override
  public synchronized void onError(Throwable throwable) {
    Objects.requireNonNull(throwable, "throwable == null");
    // An error case, release the replies
    releaseChunks();
    if (readException == null) {
      readException = throwable;
      failPendingReads();
    }
  }

  @Override
  public synchronized void onCompleted() {
    // Not an error case, do not release the replies
    if (readException == null) {
      // No more onNext(), the pending reads cannot be completed.
      readException = new EOFException(clientId + ": end of stream, request=" + header);
      failPendingReads();
    }
  }

  private void releaseChunks() {
    for (ReferenceCountedObject<DataStreamReadChunk> chunk; (chunk = chunks.poll()) != null; ) {
      chunk.release();
    }
  }

  private void failPendingReads() {
    Objects.requireNonNull(readException, "readException == null");
    for (CompletableFuture<ReferenceCountedObject<DataStreamReadChunk>> p; (p = pendingReads.poll()) != null; ) {
      p.completeExceptionally(readException);
    }
  }

  @Override
  public synchronized CompletableFuture<ReferenceCountedObject<DataStreamReadChunk>> readAsync() {
    final ReferenceCountedObject<DataStreamReadChunk> chunkRef = chunks.poll();
    if (chunkRef != null) {
      return CompletableFuture.completedFuture(chunkRef);
    }
    if (readException != null) {
      return JavaUtils.completeExceptionally(readException);
    }
    final CompletableFuture<ReferenceCountedObject<DataStreamReadChunk>> f = new CompletableFuture<>();
    pendingReads.add(f);
    return f;
  }

  @Override
  public synchronized void close() {
    // When close() is called the first time, we set
    // (1) release all replies
    // (2) set readException to AlreadyClosedException
    // (3) complete all pendingReads, and
    releaseChunks();
    if (readException == null) {
      readException = new AlreadyClosedException(clientId + ": stream already closed, request=" + header);
      failPendingReads();
    }
  }

  private static ReferenceCountedObject<DataStreamReadChunk> buildAndTransferChunk(
      ReferenceCountedObject<DataStreamReply> reply) {
    reply.retain();
    final ReferenceCountedObject<DataStreamReadChunk> chunkRef = buildChunkFromReply(reply);
    chunkRef.retain();
    reply.release();
    return chunkRef;
  }

  static ReferenceCountedObject<DataStreamReadChunk> buildChunkFromReply(
      ReferenceCountedObject<DataStreamReply> reply) {
    DataStreamReadChunk chunk;
    DataStreamReply replyData = reply.get();
    if (replyData instanceof DataStreamReplyByteBuf) {
      chunk = DataStreamReadChunkImpl.of(
          new ByteBuffer[] {((DataStreamReplyByteBuf) replyData).slice().nioBuffer()});
    } else if (replyData instanceof DataStreamReplyByteBuffer) {
      chunk = DataStreamReadChunkImpl.of(
          new ByteBuffer[] {((DataStreamReplyByteBuffer) replyData).slice()});
    } else {
      throw new IllegalArgumentException("Unexpected reply: " + reply);
    }

    return ReferenceCountedObject.<DataStreamReadChunk>newBuilder()
        .setValue(chunk)
        .setRetainMethod(reply::retain)
        .setReleaseMethod(ignored -> reply.release())
        .build();
  }
}
