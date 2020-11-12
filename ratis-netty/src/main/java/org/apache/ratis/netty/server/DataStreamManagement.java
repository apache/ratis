/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ratis.netty.server;

import org.apache.ratis.client.DataStreamOutputRpc;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelId;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataStreamManagement {
  public static final Logger LOG = LoggerFactory.getLogger(DataStreamManagement.class);

  static class LocalStream {
    private final CompletableFuture<DataStream> streamFuture;
    private final AtomicReference<CompletableFuture<Long>> writeFuture;

    LocalStream(CompletableFuture<DataStream> streamFuture) {
      this.streamFuture = streamFuture;
      this.writeFuture = new AtomicReference<>(streamFuture.thenApply(s -> 0L));
    }

    CompletableFuture<Long> write(ByteBuf buf, Executor executor) {
      return composeAsync(writeFuture, executor,
          n -> streamFuture.thenApplyAsync(stream -> writeTo(buf, stream), executor));
    }

    CompletableFuture<Long> close(Executor executor) {
      return composeAsync(writeFuture, executor,
          n -> streamFuture.thenApplyAsync(DataStreamManagement::close, executor));
    }
  }

  static class RemoteStream {
    private final DataStreamOutputRpc out;

    RemoteStream(DataStreamOutputRpc out) {
      this.out = out;
    }

    CompletableFuture<DataStreamReply> write(DataStreamRequestByteBuf request) {
      return out.writeAsync(request.slice().nioBuffer());
    }

    CompletableFuture<DataStreamReply> startTransaction(DataStreamRequestByteBuf request,
        ChannelHandlerContext ctx, Executor executor) {
      return out.startTransactionAsync().thenApplyAsync(reply -> {
        if (reply.isSuccess()) {
          final ByteBuffer buffer = reply instanceof DataStreamReplyByteBuffer?
              ((DataStreamReplyByteBuffer)reply).slice(): null;
          sendReplySuccess(request, buffer, -1, ctx);
        }
        return reply;
      }, executor);
    }

    CompletableFuture<DataStreamReply> close() {
      return out.closeAsync();
    }
  }

  static class StreamInfo {
    private final RaftClientRequest request;
    private final boolean primary;
    private final LocalStream local;
    private final List<RemoteStream> remotes;
    private final AtomicReference<CompletableFuture<Void>> previous
        = new AtomicReference<>(CompletableFuture.completedFuture(null));

    StreamInfo(RaftClientRequest request, boolean primary,
        CompletableFuture<DataStream> stream, List<DataStreamOutputRpc> outs) {
      this.request = request;
      this.primary = primary;
      this.local = new LocalStream(stream);
      this.remotes = outs.stream().map(RemoteStream::new).collect(Collectors.toList());
    }

    AtomicReference<CompletableFuture<Void>> getPrevious() {
      return previous;
    }

    RaftClientRequest getRequest() {
      return request;
    }

    boolean isPrimary() {
      return primary;
    }

    LocalStream getLocal() {
      return local;
    }

    <T> List<T> applyToRemotes(Function<RemoteStream, T> function) {
      return remotes.isEmpty()?Collections.emptyList(): remotes.stream().map(function).collect(Collectors.toList());
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" + request;
    }
  }

  static class StreamMap {
    static class Key {
      private final ChannelId channelId;
      private final long streamId;

      Key(ChannelId channelId, long streamId) {
        this.channelId = channelId;
        this.streamId = streamId;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        } else if (obj == null || getClass() != obj.getClass()) {
          return false;
        }
        final Key that = (Key) obj;
        return this.streamId == that.streamId && Objects.equals(this.channelId, that.channelId);
      }

      @Override
      public int hashCode() {
        return Objects.hash(channelId, streamId);
      }

      @Override
      public String toString() {
        return channelId + "-" + streamId;
      }
    }

    private final ConcurrentMap<Key, StreamInfo> map = new ConcurrentHashMap<>();

    StreamInfo computeIfAbsent(Key key, Function<Key, StreamInfo> function) {
      final StreamInfo info = map.computeIfAbsent(key, function);
      LOG.debug("computeIfAbsent({}) returns {}", key, info);
      return info;
    }

    StreamInfo get(Key key) {
      final StreamInfo info = map.get(key);
      LOG.debug("get({}) returns {}", key, info);
      return info;
    }
  }

  private final RaftServer server;
  private final String name;

  private final StreamMap streams = new StreamMap();
  private final Executor executor;

  DataStreamManagement(RaftServer server) {
    this.server = server;
    this.name = server.getId() + "-" + JavaUtils.getClassSimpleName(getClass());

    final RaftProperties properties = server.getProperties();
    this.executor = Executors.newFixedThreadPool(
        RaftServerConfigKeys.DataStream.asyncThreadPoolSize(properties));
  }

  private StreamInfo newStreamInfo(ByteBuf buf,
      CheckedFunction<RaftClientRequest, List<DataStreamOutputRpc>, IOException> getDataStreamOutput) {
    try {
      final RaftClientRequest request = ClientProtoUtils.toRaftClientRequest(
          RaftClientRequestProto.parseFrom(buf.nioBuffer()));
      final StateMachine stateMachine = server.getStateMachine(request.getRaftGroupId());
      final boolean isPrimary = server.getId().equals(request.getServerId());
      return new StreamInfo(request, isPrimary, stateMachine.data().stream(request),
          isPrimary? getDataStreamOutput.apply(request): Collections.emptyList());
    } catch (Throwable e) {
      throw new CompletionException(e);
    }
  }

  static <T> CompletableFuture<T> composeAsync(AtomicReference<CompletableFuture<T>> future, Executor executor,
      Function<T, CompletableFuture<T>> function) {
    final CompletableFuture<T> composed = future.get().thenComposeAsync(function, executor);
    future.set(composed);
    return composed;
  }

  static long writeTo(ByteBuf buf, DataStream stream) {
    final WritableByteChannel channel = stream.getWritableByteChannel();
    long byteWritten = 0;
    for (ByteBuffer buffer : buf.nioBuffers()) {
      try {
        byteWritten += channel.write(buffer);
      } catch (Throwable t) {
        throw new CompletionException(t);
      }
    }
    return byteWritten;
  }

  static long close(DataStream stream) {
    try {
      stream.getWritableByteChannel().close();
      return 0L;
    } catch (IOException e) {
      throw new CompletionException("Failed to close " + stream, e);
    }
  }

  static void sendReplyNotSuccess(DataStreamRequestByteBuf request, ByteBuffer buffer, ChannelHandlerContext ctx) {
    final DataStreamReplyByteBuffer reply = new DataStreamReplyByteBuffer(
        request.getStreamId(), request.getStreamOffset(), buffer, -1, false, request.getType());
    ctx.writeAndFlush(reply);
  }

  static void sendReplySuccess(DataStreamRequestByteBuf request, ByteBuffer buffer, long bytesWritten,
      ChannelHandlerContext ctx) {
    final DataStreamReplyByteBuffer reply = new DataStreamReplyByteBuffer(
        request.getStreamId(), request.getStreamOffset(), buffer, bytesWritten, true, request.getType());
    ctx.writeAndFlush(reply);
  }

  static void sendReply(List<CompletableFuture<DataStreamReply>> remoteWrites,
      DataStreamRequestByteBuf request, long bytesWritten, ChannelHandlerContext ctx) {
    if (!checkSuccessRemoteWrite(remoteWrites, bytesWritten)) {
      sendReplyNotSuccess(request, null, ctx);
    } else {
      sendReplySuccess(request, null, bytesWritten, ctx);
    }
  }

  private CompletableFuture<Void> startTransaction(StreamInfo info, DataStreamRequestByteBuf request,
      ChannelHandlerContext ctx) {
    try {
      return server.submitClientRequestAsync(info.getRequest()).thenAcceptAsync(reply -> {
        if (reply.isSuccess()) {
          ByteBuffer buffer = ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer();
          sendReplySuccess(request, buffer, -1, ctx);
        } else if (request.getType() == Type.STREAM_CLOSE) {
          // if this server is not the leader, forward start transition to the other peers
          // there maybe other unexpected reason cause failure except not leader, forwardStartTransaction anyway
          forwardStartTransaction(info, request, reply, ctx, executor);
        } else if (request.getType() == Type.START_TRANSACTION){
          ByteBuffer buffer = ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer();
          sendReplyNotSuccess(request, buffer, ctx);
        } else {
          throw new IllegalStateException(this + ": Unexpected type " + request.getType() + ", request=" + request);
        }
      }, executor);
    } catch (IOException e) {
      sendReplyNotSuccess(request, null, ctx);
      return CompletableFuture.completedFuture(null);
    }
  }

  static void sendLeaderFailedReply(final List<CompletableFuture<DataStreamReply>> results,
      DataStreamRequestByteBuf request, RaftClientReply localReply, ChannelHandlerContext ctx) {
    // get replies from the results, ignored exceptional replies
    final Stream<RaftClientReply> remoteReplies = results.stream()
        .filter(r -> !r.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .map(DataStreamManagement::getRaftClientReply);

    // choose the leader's reply if there is any.  Otherwise, use the local reply
    final RaftClientReply chosen = Stream.concat(Stream.of(localReply), remoteReplies)
        .filter(reply -> reply.getNotLeaderException() == null)
        .findAny().orElse(localReply);

    // send reply
    final ByteBuffer buffer = ClientProtoUtils.toRaftClientReplyProto(chosen).toByteString().asReadOnlyByteBuffer();
    sendReplyNotSuccess(request, buffer, ctx);
  }

  static void forwardStartTransaction(StreamInfo info, DataStreamRequestByteBuf request, RaftClientReply localReply,
      ChannelHandlerContext ctx, Executor executor) {
    final List<CompletableFuture<DataStreamReply>> results = info.applyToRemotes(
        out -> out.startTransaction(request, ctx, executor));

    JavaUtils.allOf(results).thenAccept(v -> {
      for (CompletableFuture<DataStreamReply> result : results) {
        if (result.join().isSuccess()) {
          return;
        }
      }

      sendLeaderFailedReply(results, request, localReply, ctx);
    });
  }

  static RaftClientReply getRaftClientReply(DataStreamReply dataStreamReply) {
    if (dataStreamReply instanceof DataStreamReplyByteBuffer) {
      try {
        return ClientProtoUtils.toRaftClientReply(
            RaftClientReplyProto.parseFrom(((DataStreamReplyByteBuffer) dataStreamReply).slice()));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException("Failed to decode RaftClientReply");
      }
    } else {
      throw new IllegalStateException("Unexpected reply type");
    }
  }

  void read(DataStreamRequestByteBuf request, ChannelHandlerContext ctx,
      CheckedFunction<RaftClientRequest, List<DataStreamOutputRpc>, IOException> getDataStreamOutput) {
    LOG.debug("{}: read {}", this, request);
    final ByteBuf buf = request.slice();
    final StreamMap.Key key = new StreamMap.Key(ctx.channel().id(), request.getStreamId());

    if (request.getType() == Type.START_TRANSACTION) {
      // for peers to start transaction
      final StreamInfo info = streams.get(key);
      composeAsync(info.getPrevious(), executor, v -> startTransaction(info, request, ctx))
          .thenAccept(v -> buf.release());
      return;
    }

    final StreamInfo info;
    final CompletableFuture<Long> localWrite;
    final List<CompletableFuture<DataStreamReply>> remoteWrites;
    if (request.getType() == Type.STREAM_HEADER) {
      info = streams.computeIfAbsent(key, id -> newStreamInfo(buf, getDataStreamOutput));
      localWrite = CompletableFuture.completedFuture(0L);
      remoteWrites = Collections.emptyList();
    } else if (request.getType() == Type.STREAM_DATA) {
      info = streams.get(key);
      localWrite = info.getLocal().write(buf, executor);
      remoteWrites = info.applyToRemotes(out -> out.write(request));
    } else if (request.getType() == Type.STREAM_CLOSE) {
      info = streams.get(key);
      localWrite = info.getLocal().close(executor);
      remoteWrites = info.isPrimary()? info.applyToRemotes(RemoteStream::close): Collections.emptyList();
    } else {
      throw new IllegalStateException(this + ": Unexpected type " + request.getType() + ", request=" + request);
    }

    composeAsync(info.getPrevious(), executor, n -> JavaUtils.allOf(remoteWrites)
        .thenCombineAsync(localWrite, (v, bytesWritten) -> {
          if (request.getType() == Type.STREAM_HEADER
              || request.getType() == Type.STREAM_DATA) {
            sendReply(remoteWrites, request, bytesWritten, ctx);
          } else if (request.getType() == Type.STREAM_CLOSE) {
            if (info.isPrimary()) {
              // after all server close stream, primary server start transaction
              // TODO(runzhiwang): send start transaction to leader directly
              startTransaction(info, request, ctx);
            } else {
              sendReply(remoteWrites, request, bytesWritten, ctx);
            }
          } else {
            throw new IllegalStateException(this + ": Unexpected type " + request.getType() + ", request=" + request);
          }
          buf.release();
          return null;
        }, executor));
  }

  static boolean checkSuccessRemoteWrite(List<CompletableFuture<DataStreamReply>> replyFutures, long bytesWritten) {
    for (CompletableFuture<DataStreamReply> replyFuture : replyFutures) {
      final DataStreamReply reply = replyFuture.join();
      if (!reply.isSuccess() || reply.getBytesWritten() != bytesWritten) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return name;
  }
}
