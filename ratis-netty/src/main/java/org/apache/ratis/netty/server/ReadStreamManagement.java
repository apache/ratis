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
package org.apache.ratis.netty.server;

import org.apache.ratis.client.impl.OrderedAsync;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuf;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.ratis.client.impl.ClientProtoUtils.toRaftClientRequest;
import static org.apache.ratis.client.impl.ClientProtoUtils.toRaftClientReplyProto;
import static org.apache.ratis.netty.server.DataStreamManagement.newDataStreamReplyByteBuffer;
import static org.apache.ratis.netty.server.DataStreamManagement.replyDataStreamException;

public class ReadStreamManagement {
  public static final Logger LOG = LoggerFactory.getLogger(ReadStreamManagement.class);

  static class ReadStream implements WritableByteChannel {
    private final ClientId clientId;
    private final long streamId;
    private final ChannelHandlerContext ctx;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final DataStreamReplyByteBuffer terminalReply;
    private long streamOffset;

    ReadStream(RaftClientRequest request, long streamId, ChannelHandlerContext ctx, RaftClientReply terminalReply) {
      this.clientId = request.getClientId();
      this.streamId = streamId;
      this.ctx = ctx;

      this.terminalReply = newReadStreamTerminalReply(clientId, streamId, terminalReply);
    }

    private static DataStreamReplyByteBuffer newReadStreamTerminalReply(
        ClientId clientId, long streamId, RaftClientReply reply) {
      return DataStreamReplyByteBuffer.newBuilder()
          .setClientId(clientId)
          .setType(Type.STREAM_HEADER)
          .setStreamId(streamId)
          .setStreamOffset(0)
          .setBuffer(toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer())
          .setSuccess(reply.isSuccess())
          .setCommitInfos(reply.getCommitInfos())
          .build();
    }

    @Override
    public boolean isOpen() {
      return !closed.isDone();
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed.complete(null)) {
        writeAndFlush(terminalReply, 0);
      }
    }

    @Override
    public synchronized int write(ByteBuffer buffer) throws IOException {
      if (!isOpen()) {
        throw new AlreadyClosedException("Channel closed at offset " + streamOffset);
      }
      buffer = buffer.asReadOnlyBuffer();
      final int length = buffer.remaining();
      final DataStreamReplyByteBuffer reply = newReply(buffer);
      writeAndFlush(reply, length);
      streamOffset += length;
      return length;
    }

    private synchronized void writeAndFlush(DataStreamReplyByteBuffer reply, int length) throws IOException {
      final long offset = reply.getStreamOffset();
      final ChannelFuture future = ctx.writeAndFlush(reply);
      try {
        future.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("Interrupted while writing " + length + " bytes at offset " + offset);
      }
      if (!future.isSuccess()) {
        throw new IOException("Failed to write " + length + " bytes at offset " + offset, future.cause());
      }
    }

    private synchronized DataStreamReplyByteBuffer newReply(ByteBuffer buffer) {
      return DataStreamReplyByteBuffer.newBuilder()
          .setClientId(clientId)
          .setType(Type.STREAM_DATA)
          .setStreamId(streamId)
          .setStreamOffset(streamOffset)
          .setBuffer(buffer)
          .setSuccess(true)
          .setBytesWritten(buffer.remaining())
          .build();
    }
  }

  private final RaftServer server;
  private final String name;
  private final ExecutorService requestExecutor;

  ReadStreamManagement(RaftServer server) {
    this.server = server;
    this.name = server.getId() + "-" + JavaUtils.getClassSimpleName(getClass());

    final RaftProperties properties = server.getProperties();
    this.requestExecutor = ConcurrentUtils.newThreadPoolWithMax(
        RaftServerConfigKeys.DataStream.asyncRequestThreadPoolCached(properties),
        RaftServerConfigKeys.DataStream.asyncRequestThreadPoolSize(properties),
        name + "-request-");
  }

  void shutdown() {
    ConcurrentUtils.shutdownAndWait(TimeDuration.ONE_SECOND, requestExecutor,
        timeout -> LOG.warn("{}: requestExecutor shutdown timeout in {}", this, timeout));
  }

  boolean process(DataStreamRequestByteBuf requestBuf, ChannelHandlerContext ctx) {
    boolean processed = false;
    try {
      processed = processImpl(requestBuf, ctx);
    } catch (Throwable e) {
      LOG.error("Failed to process {}", requestBuf, e);
      processed = true;
    } finally {
      if (processed) {
        requestBuf.release();
      }
    }
    return processed;
  }

  private boolean processImpl(DataStreamRequestByteBuf requestBuf, ChannelHandlerContext ctx)
      throws InvalidProtocolBufferException {
    if (requestBuf.getType() != Type.STREAM_HEADER) {
      return false;
    }
    final RaftClientRequest request = toRaftClientRequest(
        RaftClientRequestProto.parseFrom(requestBuf.slice().nioBuffer()));
    if (!request.is(TypeCase.READ)) {
      return false;
    }

    final RaftServer.Division division;
    try {
      division = server.getDivision(request.getRaftGroupId());
    } catch (IOException e) {
      replyDataStreamException(server, e, request, requestBuf, ctx);
      return true;
    }

    final CompletableFuture<RaftClientReply> readCheck;
    try {
      readCheck = server.submitClientRequestAsync(newDummyReadRequest(request));
    } catch (IOException e) {
      replyDataStreamException(server, e, request, requestBuf, ctx);
      return true;
    }

    readCheck.whenCompleteAsync((readCheckReply, exception) -> {
      if (exception != null) {
        replyDataStreamException(server, exception, request, requestBuf, ctx);
        return;
      }

      if (!readCheckReply.isSuccess()) {
        ctx.writeAndFlush(newDataStreamReplyByteBuffer(requestBuf, readCheckReply));
        return;
      }

      final ReadStream stream = new ReadStream(request, requestBuf.getStreamId(), ctx, readCheckReply);
      try {
        division.getStateMachine().data().transferTo(request.getMessage(), stream);
      } catch (Throwable t) {
        LOG.error("{}: Failed read-only data stream query for {}", this, request, t);
      }
    }, requestExecutor);
    return true;
  }

  private static RaftClientRequest newDummyReadRequest(RaftClientRequest request) {
    final RaftProtos.ReadRequestTypeProto original = request.getType().getRead();
    return RaftClientRequest.newBuilder()
        .set(request)
        .setMessage(OrderedAsync.DUMMY)
        .setType(RaftClientRequest.readRequestType(
            original.getPreferNonLinearizable(), original.getReadAfterWriteConsistent(), true))
        .build();
  }

  @Override
  public String toString() {
    return name;
  }
}
