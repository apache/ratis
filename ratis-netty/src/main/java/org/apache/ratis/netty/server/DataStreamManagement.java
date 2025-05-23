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

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuf;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.netty.metrics.NettyServerStreamRpcMetrics;
import org.apache.ratis.netty.metrics.NettyServerStreamRpcMetrics.RequestMetrics;
import org.apache.ratis.netty.metrics.NettyServerStreamRpcMetrics.RequestType;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.protocol.exceptions.AlreadyExistsException;
import org.apache.ratis.protocol.exceptions.DataStreamException;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServer.Division;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.statemachine.StateMachine.DataChannel;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelId;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataStreamManagement {
  public static final Logger LOG = LoggerFactory.getLogger(DataStreamManagement.class);

  static class LocalStream {
    private final CompletableFuture<DataStream> streamFuture;
    private final AtomicReference<CompletableFuture<Long>> writeFuture;
    private final RequestMetrics metrics;

    LocalStream(CompletableFuture<DataStream> streamFuture, RequestMetrics metrics) {
      this.streamFuture = streamFuture;
      this.writeFuture = new AtomicReference<>(streamFuture.thenApply(s -> 0L));
      this.metrics = metrics;
    }

    CompletableFuture<Long> write(ByteBuf buf, Iterable<WriteOption> options,
                                  Executor executor) {
      final Timekeeper.Context context = metrics.start();
      return composeAsync(writeFuture, executor,
          n -> streamFuture.thenCompose(stream -> writeToAsync(buf, options, stream, executor)
              .whenComplete((l, e) -> metrics.stop(context, e == null))));
    }

    void cleanUp() {
      streamFuture.thenAccept(DataStream::cleanUp);
    }
  }

  static class RemoteStream {
    private final DataStreamOutputImpl out;
    private final AtomicReference<CompletableFuture<DataStreamReply>> sendFuture
        = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final RequestMetrics metrics;

    RemoteStream(DataStreamOutputImpl out, RequestMetrics metrics) {
      this.metrics = metrics;
      this.out = out;
    }

    static Iterable<WriteOption> addFlush(List<WriteOption> original) {
      if (original.contains(StandardWriteOption.FLUSH)) {
        return original;
      }
      return Stream.concat(Stream.of(StandardWriteOption.FLUSH), original.stream())
          .collect(Collectors.toList());
    }

    CompletableFuture<DataStreamReply> write(DataStreamRequestByteBuf request, Executor executor) {
      final Timekeeper.Context context = metrics.start();
      return composeAsync(sendFuture, executor,
          n -> out.writeAsync(request.slice().retain(), addFlush(request.getWriteOptionList()))
              .whenComplete((l, e) -> metrics.stop(context, e == null)));
    }
  }

  static class StreamInfo {
    private final RaftClientRequest request;
    private final boolean primary;
    private final LocalStream local;
    private final Set<RemoteStream> remotes;
    private final Division division;
    private final AtomicReference<CompletableFuture<Void>> previous
        = new AtomicReference<>(CompletableFuture.completedFuture(null));

    StreamInfo(RaftClientRequest request, boolean primary, CompletableFuture<DataStream> stream, Division division,
        CheckedBiFunction<RaftClientRequest, Set<RaftPeer>, Set<DataStreamOutputImpl>, IOException> getStreams,
        Function<RequestType, RequestMetrics> metricsConstructor)
        throws IOException {
      this.request = request;
      this.primary = primary;
      this.local = new LocalStream(stream, metricsConstructor.apply(RequestType.LOCAL_WRITE));
      this.division = division;
      final Set<RaftPeer> successors = getSuccessors(division.getId());
      final Set<DataStreamOutputImpl> outs = getStreams.apply(request, successors);
      this.remotes = outs.stream()
          .map(o -> new RemoteStream(o, metricsConstructor.apply(RequestType.REMOTE_WRITE)))
          .collect(Collectors.toSet());
    }

    AtomicReference<CompletableFuture<Void>> getPrevious() {
      return previous;
    }

    RaftClientRequest getRequest() {
      return request;
    }

    Division getDivision() {
      return division;
    }

    Collection<CommitInfoProto> getCommitInfos() {
      return getDivision().getCommitInfos();
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

    private Set<RaftPeer> getSuccessors(RaftPeerId peerId) {
      final RaftConfiguration conf = getDivision().getRaftConf();
      final RoutingTable routingTable = request.getRoutingTable();

      if (routingTable != null) {
        return routingTable.getSuccessors(peerId).stream().map(conf::getPeer).collect(Collectors.toSet());
      }

      if (isPrimary()) {
        // Default star topology
        // get the other peers from the current configuration
        return conf.getCurrentPeers().stream()
            .filter(p -> !p.getId().equals(division.getId()))
            .collect(Collectors.toSet());
      }

      return Collections.emptySet();
    }

    void cleanUp(ClientInvocationId invocationId) {
      getDivision().getDataStreamMap().remove(invocationId);
      getLocal().cleanUp();
      applyToRemotes(remote -> remote.out.closeAsync());
    }
  }

  private final RaftServer server;
  private final String name;

  private final StreamMap<StreamInfo> streams = new StreamMap<>();
  private final ChannelMap channels;
  private final ExecutorService requestExecutor;
  private final ExecutorService writeExecutor;
  private final TimeDuration requestTimeout;

  private final NettyServerStreamRpcMetrics nettyServerStreamRpcMetrics;

  DataStreamManagement(RaftServer server, NettyServerStreamRpcMetrics metrics) {
    this.server = server;
    this.name = server.getId() + "-" + JavaUtils.getClassSimpleName(getClass());

    this.channels = new ChannelMap();
    final RaftProperties properties = server.getProperties();
    final boolean useCachedThreadPool = RaftServerConfigKeys.DataStream.asyncRequestThreadPoolCached(properties);
    this.requestExecutor = ConcurrentUtils.newThreadPoolWithMax(useCachedThreadPool,
          RaftServerConfigKeys.DataStream.asyncRequestThreadPoolSize(properties),
          name + "-request-");
    this.writeExecutor = ConcurrentUtils.newThreadPoolWithMax(useCachedThreadPool,
          RaftServerConfigKeys.DataStream.asyncWriteThreadPoolSize(properties),
          name + "-write-");
    this.requestTimeout = RaftClientConfigKeys.DataStream.requestTimeout(server.getProperties());

    this.nettyServerStreamRpcMetrics = metrics;
  }

  void shutdown() {
    ConcurrentUtils.shutdownAndWait(TimeDuration.ONE_SECOND, requestExecutor,
        timeout -> LOG.warn("{}: requestExecutor shutdown timeout in {}", this, timeout));
    ConcurrentUtils.shutdownAndWait(TimeDuration.ONE_SECOND, writeExecutor,
        timeout -> LOG.warn("{}: writeExecutor shutdown timeout in {}", this, timeout));
  }

  private CompletableFuture<DataStream> stream(RaftClientRequest request, StateMachine stateMachine) {
    final RequestMetrics metrics = getMetrics().newRequestMetrics(RequestType.STATE_MACHINE_STREAM);
    final Timekeeper.Context context = metrics.start();
    return stateMachine.data().stream(request)
        .whenComplete((r, e) -> metrics.stop(context, e == null));
  }

  private CompletableFuture<DataStream> computeDataStreamIfAbsent(RaftClientRequest request) throws IOException {
    final Division division = server.getDivision(request.getRaftGroupId());
    final ClientInvocationId invocationId = ClientInvocationId.valueOf(request);
    final CompletableFuture<DataStream> created = new CompletableFuture<>();
    final CompletableFuture<DataStream> returned = division.getDataStreamMap()
        .computeIfAbsent(invocationId, key -> created);
    if (returned != created) {
      throw new AlreadyExistsException("A DataStream already exists for " + invocationId);
    }
    stream(request, division.getStateMachine()).whenComplete(JavaUtils.asBiConsumer(created));
    return created;
  }

  private StreamInfo newStreamInfo(ByteBuf buf,
      CheckedBiFunction<RaftClientRequest, Set<RaftPeer>, Set<DataStreamOutputImpl>, IOException> getStreams) {
    try {
      final RaftClientRequest request = ClientProtoUtils.toRaftClientRequest(
          RaftClientRequestProto.parseFrom(buf.nioBuffer()));
      final boolean isPrimary = server.getId().equals(request.getServerId());
      final Division division = server.getDivision(request.getRaftGroupId());
      return new StreamInfo(request, isPrimary, computeDataStreamIfAbsent(request), division, getStreams,
          getMetrics()::newRequestMetrics);
    } catch (Throwable e) {
      throw new CompletionException(e);
    }
  }

  static <T> CompletableFuture<T> composeAsync(AtomicReference<CompletableFuture<T>> future, Executor executor,
      Function<T, CompletableFuture<T>> function) {
    return future.updateAndGet(previous -> previous.thenComposeAsync(function, executor));
  }

  static CompletableFuture<Long> writeToAsync(ByteBuf buf,
                                              Iterable<WriteOption> options,
                                              DataStream stream,
      Executor defaultExecutor) {
    final Executor e = Optional.ofNullable(stream.getExecutor()).orElse(defaultExecutor);
    return CompletableFuture.supplyAsync(() -> writeTo(buf, options, stream), e);
  }

  static long writeTo(ByteBuf buf, Iterable<WriteOption> options,
                      DataStream stream) {
    final DataChannel channel = stream.getDataChannel();
    long byteWritten = 0;
    for (ByteBuffer buffer : buf.nioBuffers()) {
      if (buffer.remaining() == 0) {
        continue;
      }
      final ReferenceCountedObject<ByteBuffer> wrapped = ReferenceCountedObject.wrap(
          buffer, buf::retain, ignored -> buf.release());
      try(UncheckedAutoCloseable ignore = wrapped.retainAndReleaseOnClose()) {
        byteWritten += channel.write(wrapped);
      } catch (Throwable t) {
        throw new CompletionException(t);
      }
    }

    if (WriteOption.containsOption(options, StandardWriteOption.SYNC)) {
      try {
        channel.force(false);
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    }

    if (WriteOption.containsOption(options, StandardWriteOption.CLOSE)) {
      close(stream);
    }
    return byteWritten;
  }

  static void close(DataStream stream) {
    try {
      stream.getDataChannel().close();
    } catch (IOException e) {
      throw new CompletionException("Failed to close " + stream, e);
    }
  }

  static DataStreamReplyByteBuffer newDataStreamReplyByteBuffer(DataStreamRequestByteBuf request,
      RaftClientReply reply) {
    final ByteBuffer buffer = ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer();
    return DataStreamReplyByteBuffer.newBuilder()
        .setDataStreamPacket(request)
        .setBuffer(buffer)
        .setSuccess(reply.isSuccess())
        .build();
  }

  private void sendReply(List<CompletableFuture<DataStreamReply>> remoteWrites,
      DataStreamRequestByteBuf request, long bytesWritten, Collection<CommitInfoProto> commitInfos,
      ChannelHandlerContext ctx) {
    final boolean success = checkSuccessRemoteWrite(remoteWrites, bytesWritten, request);
    final DataStreamReplyByteBuffer.Builder builder = DataStreamReplyByteBuffer.newBuilder()
        .setDataStreamPacket(request)
        .setSuccess(success)
        .setCommitInfos(commitInfos);
    if (success) {
      builder.setBytesWritten(bytesWritten);
    }
    ctx.writeAndFlush(builder.build());
  }

  static void replyDataStreamException(RaftServer server, Throwable cause, RaftClientRequest raftClientRequest,
      DataStreamRequestByteBuf request, ChannelHandlerContext ctx) {
    final RaftClientReply reply = RaftClientReply.newBuilder()
        .setRequest(raftClientRequest)
        .setException(new DataStreamException(server.getId(), cause))
        .build();
    sendDataStreamException(cause, request, reply, ctx);
  }

  void replyDataStreamException(Throwable cause, DataStreamRequestByteBuf request, ChannelHandlerContext ctx) {
    final RaftClientReply reply = RaftClientReply.newBuilder()
        .setClientId(ClientId.emptyClientId())
        .setServerId(server.getId())
        .setGroupId(RaftGroupId.emptyGroupId())
        .setException(new DataStreamException(server.getId(), cause))
        .build();
    sendDataStreamException(cause, request, reply, ctx);
  }

  static void sendDataStreamException(Throwable throwable, DataStreamRequestByteBuf request, RaftClientReply reply,
      ChannelHandlerContext ctx) {
    LOG.warn("Failed to process {}",  request, throwable);
    try {
      ctx.writeAndFlush(newDataStreamReplyByteBuffer(request, reply));
    } catch (Throwable t) {
      LOG.warn("Failed to sendDataStreamException {} for {}", throwable, request, t);
    } finally {
      request.release();
    }
  }

  void cleanUp(Set<ClientInvocationId> ids) {
    for (ClientInvocationId clientInvocationId : ids) {
      removeDataStream(clientInvocationId);
    }
  }

  void cleanUpOnChannelInactive(ChannelId channelId, TimeDuration channelInactiveGracePeriod) {
    // Delayed memory garbage cleanup
    Optional.ofNullable(channels.remove(channelId)).ifPresent(ids -> {
      LOG.info("Channel {} is inactive, cleanup clientInvocationIds={}", channelId, ids);
      TimeoutExecutor.getInstance().onTimeout(channelInactiveGracePeriod, () -> cleanUp(ids),
          LOG, () -> "Timeout check failed, clientInvocationIds=" + ids);
    });
  }

  void read(DataStreamRequestByteBuf request, ChannelHandlerContext ctx,
      CheckedBiFunction<RaftClientRequest, Set<RaftPeer>, Set<DataStreamOutputImpl>, IOException> getStreams) {
    LOG.debug("{}: read {}", this, request);
    try {
      readImpl(request, ctx, getStreams);
    } catch (Throwable t) {
      replyDataStreamException(t, request, ctx);
      removeDataStream(ClientInvocationId.valueOf(request.getClientId(), request.getStreamId()));
    }
  }

  private StreamInfo removeDataStream(ClientInvocationId invocationId) {
    final StreamInfo removed = streams.remove(invocationId);
    if (removed != null) {
      removed.cleanUp(invocationId);
    }
    return removed;
  }

  private void readImpl(DataStreamRequestByteBuf request, ChannelHandlerContext ctx,
      CheckedBiFunction<RaftClientRequest, Set<RaftPeer>, Set<DataStreamOutputImpl>, IOException> getStreams) {
    final boolean close = request.getWriteOptionList().contains(StandardWriteOption.CLOSE);
    ClientInvocationId key =  ClientInvocationId.valueOf(request.getClientId(), request.getStreamId());

    // add to ChannelMap
    final ChannelId channelId = ctx.channel().id();
    channels.add(channelId, key);

    final StreamInfo info;
    if (request.getType() == Type.STREAM_HEADER) {
      final MemoizedSupplier<StreamInfo> supplier = JavaUtils.memoize(
          () -> newStreamInfo(request.slice(), getStreams));
      info = streams.computeIfAbsent(key, id -> supplier.get());
      if (!supplier.isInitialized()) {
        throw new IllegalStateException("Failed to create a new stream for " + request
            + " since a stream already exists Key: " + key + " StreamInfo:" + info);
      }
      getMetrics().onRequestCreate(RequestType.HEADER);
    } else if (close) {
      info = Optional.ofNullable(streams.remove(key)).orElseThrow(
          () -> new IllegalStateException("Failed to remove StreamInfo for " + request));
    } else {
      info = Optional.ofNullable(streams.get(key)).orElseThrow(
          () -> new IllegalStateException("Failed to get StreamInfo for " + request));
    }

    final CompletableFuture<Long> localWrite;
    final List<CompletableFuture<DataStreamReply>> remoteWrites;
    if (request.getType() == Type.STREAM_HEADER) {
      localWrite = CompletableFuture.completedFuture(0L);
      remoteWrites = Collections.emptyList();
    } else if (request.getType() == Type.STREAM_DATA) {
      localWrite = info.getLocal().write(request.slice(), request.getWriteOptionList(), writeExecutor);
      remoteWrites = info.applyToRemotes(out -> out.write(request, requestExecutor));
    } else {
      throw new IllegalStateException(this + ": Unexpected type " + request.getType() + ", request=" + request);
    }

    composeAsync(info.getPrevious(), requestExecutor, n -> JavaUtils.allOf(remoteWrites)
        .thenCombineAsync(localWrite, (v, bytesWritten) -> {
          if (request.getType() == Type.STREAM_HEADER
              || request.getType() == Type.STREAM_DATA
              || close) {
            sendReply(remoteWrites, request, bytesWritten, info.getCommitInfos(), ctx);
          } else {
            throw new IllegalStateException(this + ": Unexpected type " + request.getType() + ", request=" + request);
          }
          return null;
        }, requestExecutor)).whenComplete((v, exception) -> {
      try {
        if (exception != null) {
          replyDataStreamException(server, exception, info.getRequest(), request, ctx);
          final StreamInfo removed = removeDataStream(key);
          if (removed != null) {
            Preconditions.assertSame(info, removed, "removed");
          } else {
            info.cleanUp(key);
          }
        } else if (close) {
          info.applyToRemotes(remote -> remote.out.closeAsync());
        }
      } finally {
        request.release();
        channels.remove(channelId, key);
      }
    });
  }

  static void assertReplyCorrespondingToRequest(
      final DataStreamRequestByteBuf request, final DataStreamReply reply) {
    Preconditions.assertTrue(request.getClientId().equals(reply.getClientId()));
    Preconditions.assertTrue(request.getType() == reply.getType());
    Preconditions.assertTrue(request.getStreamId() == reply.getStreamId());
    Preconditions.assertTrue(request.getStreamOffset() == reply.getStreamOffset());
  }

  private boolean checkSuccessRemoteWrite(List<CompletableFuture<DataStreamReply>> replyFutures, long bytesWritten,
      final DataStreamRequestByteBuf request) {
    for (CompletableFuture<DataStreamReply> replyFuture : replyFutures) {
      final DataStreamReply reply;
      try {
        reply = replyFuture.get(requestTimeout.getDuration(), requestTimeout.getUnit());
      } catch (Exception e) {
        throw new CompletionException("Failed to get reply for bytesWritten=" + bytesWritten + ", " + request, e);
      }
      assertReplyCorrespondingToRequest(request, reply);
      if (!reply.isSuccess()) {
        LOG.warn("reply is not success, request: {}", request);
        return false;
      }
      if (reply.getBytesWritten() != bytesWritten) {
        LOG.warn(
            "reply written bytes not match, local size: {} remote size: {} request: {}",
            bytesWritten, reply.getBytesWritten(), request);
        return false;
      }
    }
    return true;
  }

  NettyServerStreamRpcMetrics getMetrics() {
    return nettyServerStreamRpcMetrics;
  }

  @Override
  public String toString() {
    return name;
  }
}
