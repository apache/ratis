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
package org.apache.ratis.grpc.server;

import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.grpc.util.ZeroCopyMessageMarshaller;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageOrBuilder;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerMethodDefinition;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceImplBase;
import org.apache.ratis.util.BatchLogger;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class GrpcServerProtocolService extends RaftServerProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcServerProtocolService.class);
  private static final AtomicLong UNRELEASED_ON_FAILURE = new AtomicLong();

  private enum BatchLogKey implements BatchLogger.Key {
    COMPLETED_REQUEST,
    COMPLETED_REPLY
  }

  static class PendingServerRequest<REQUEST> {
    private final REQUEST request;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    PendingServerRequest(REQUEST request) {
      this.request = request;
    }

    REQUEST getRequest() {
      return request;
    }

    CompletableFuture<Void> getFuture() {
      return future;
    }
  }

  abstract class ServerRequestStreamObserver<REQUEST, REPLY extends MessageOrBuilder>
      implements StreamObserver<REQUEST> {
    private final RaftServer.Op op;
    private final Supplier<String> nameSupplier;
    private final StreamObserver<REPLY> responseObserver;
    /** For ordered {@link #onNext(Object)} requests. */
    private final AtomicReference<PendingServerRequest<REQUEST>> previousOnNext = new AtomicReference<>();
    /** For both ordered and unordered {@link #onNext(Object)} requests. */
    private final AtomicReference<CompletableFuture<REPLY>> requestFuture
        = new AtomicReference<>(CompletableFuture.completedFuture(null));
    /** Last reply sent on the gRPC stream (ordered requests discard the future value). */
    private final AtomicReference<REPLY> lastReplySent = new AtomicReference<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    ServerRequestStreamObserver(RaftServer.Op op, StreamObserver<REPLY> responseObserver) {
      this.op = op;
      this.nameSupplier = MemoizedSupplier.valueOf(() -> getId() + "_" + op);
      this.responseObserver = responseObserver;
    }

    String getName() {
      return nameSupplier.get();
    }

    private String getPreviousRequestString() {
      return Optional.ofNullable(previousOnNext.get())
          .map(PendingServerRequest::getRequest)
          .map(this::requestToString)
          .orElse(null);
    }

    abstract CompletableFuture<REPLY> process(REQUEST request) throws IOException;

    abstract long getCallId(REQUEST request);

    abstract String requestToString(REQUEST request);

    abstract String replyToString(REPLY reply);

    abstract boolean replyInOrder(REQUEST request);

    StatusRuntimeException wrapException(Throwable e, REQUEST request) {
      return GrpcUtil.wrapException(e, getCallId(request));
    }

    private void handleError(Throwable e, REQUEST request) {
      GrpcUtil.warn(LOG, () -> getId() + ": Failed " + op + " request " + requestToString(request), e);
      if (isClosed.compareAndSet(false, true)) {
        responseObserver.onError(wrapException(e, request));
      }
    }

    private synchronized REPLY handleReply(REPLY reply) {
      if (reply != null) {
        lastReplySent.set(reply);
      }
      if (!isClosed.get()) {
        if (LOG.isDebugEnabled() && reply != null) {
          LOG.debug("{}: reply {}", getId(), replyToString(reply));
        }
        responseObserver.onNext(reply);
      }
      return reply;
    }

    private String getCompletedReplyLogString(REPLY futureReply) {
      final REPLY sent = lastReplySent.get();
      if (sent != null) {
        return replyToString(sent);
      }
      if (futureReply != null) {
        return replyToString(futureReply);
      }
      return "none";
    }

    void composeRequest(CompletableFuture<REPLY> current) {
      requestFuture.updateAndGet(previous -> previous.thenCompose(reply -> current));
    }

    @Override
    public void onNext(REQUEST request) {
      if (!replyInOrder(request)) {
        try {
          composeRequest(process(request).thenApply(this::handleReply));
        } catch (Exception e) {
          handleError(e, request);
        }
        return;
      }

      final PendingServerRequest<REQUEST> current = new PendingServerRequest<>(request);
      final PendingServerRequest<REQUEST> previous = previousOnNext.getAndSet(current);
      final CompletableFuture<Void> previousFuture = Optional.ofNullable(previous)
          .map(PendingServerRequest::getFuture)
          .orElse(CompletableFuture.completedFuture(null));
      try {
        final CompletableFuture<REPLY> f = process(request).exceptionally(e -> {
          // Handle cases, such as RaftServer is paused
          handleError(e, request);
          current.getFuture().completeExceptionally(e);
          return null;
        }).thenCombine(previousFuture, (reply, v) -> {
          handleReply(reply);
          current.getFuture().complete(null);
          return null;
        });
        composeRequest(f);
      } catch (Exception e) {
        handleError(e, request);
        current.getFuture().completeExceptionally(e);
      }
    }

    @Override
    public void onCompleted() {
      if (isClosed.compareAndSet(false, true)) {
        BatchLogger.print(BatchLogKey.COMPLETED_REQUEST, getName(),
            suffix -> LOG.info("{}: Completed {}, lastRequest: {} {}",
                getId(), op, getPreviousRequestString(), suffix));
        requestFuture.get().thenAccept(reply -> {
          final String replyLog = getCompletedReplyLogString(reply);
          BatchLogger.print(BatchLogKey.COMPLETED_REPLY, getName(),
              suffix -> LOG.info("{}: Completed {}, lastReply: {} {}",
                  getId(), op, replyLog, suffix));
          responseObserver.onCompleted();
        });
      }
    }
    @Override
    public void onError(Throwable t) {
      GrpcUtil.warn(LOG, () -> getId() + ": "+ op + " onError, lastRequest: " + getPreviousRequestString(), t);
      if (isClosed.compareAndSet(false, true)) {
        Status status = Status.fromThrowable(t);
        if (status != null && status.getCode() != Status.Code.CANCELLED) {
          responseObserver.onCompleted();
        }
      }
    }
  }

  private final Supplier<RaftPeerId> idSupplier;
  private final RaftServer server;
  /** Non-null iff followers decode appendEntries without copying the state machine data. */
  private final ZeroCopyMessageMarshaller<AppendEntriesRequestProto> zeroCopyAppendEntries;

  GrpcServerProtocolService(Supplier<RaftPeerId> idSupplier, RaftServer server) {
    this(idSupplier, server, false);
  }

  GrpcServerProtocolService(Supplier<RaftPeerId> idSupplier, RaftServer server, boolean zeroCopyAppendEntries) {
    this(idSupplier, server, zeroCopyAppendEntries
        ? new ZeroCopyMessageMarshaller<>(AppendEntriesRequestProto.getDefaultInstance()) : null);
  }

  @VisibleForTesting
  GrpcServerProtocolService(Supplier<RaftPeerId> idSupplier, RaftServer server,
      ZeroCopyMessageMarshaller<AppendEntriesRequestProto> zeroCopyAppendEntries) {
    this.idSupplier = idSupplier;
    this.server = server;
    this.zeroCopyAppendEntries = zeroCopyAppendEntries;
  }

  /** @return this service, with appendEntries decoded by the zero-copy marshaller if it is enabled. */
  ServerServiceDefinition bindServiceDefinition() {
    final ServerServiceDefinition service = bindService();
    if (zeroCopyAppendEntries == null) {
      return service;
    }
    final String appendEntries = RaftServerProtocolServiceGrpc.getAppendEntriesMethod().getFullMethodName();
    final ServerServiceDefinition.Builder builder = ServerServiceDefinition.builder(
        service.getServiceDescriptor().getName());
    for (ServerMethodDefinition<?, ?> method : service.getMethods()) {
      if (method.getMethodDescriptor().getFullMethodName().equals(appendEntries)) {
        builder.addMethod(withZeroCopy(method));
      } else {
        builder.addMethod(method);
      }
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private <REPLY> ServerMethodDefinition<AppendEntriesRequestProto, REPLY> withZeroCopy(
      ServerMethodDefinition<?, ?> m) {
    final ServerMethodDefinition<AppendEntriesRequestProto, REPLY> method
        = (ServerMethodDefinition<AppendEntriesRequestProto, REPLY>) m;
    final MethodDescriptor<AppendEntriesRequestProto, REPLY> descriptor = method.getMethodDescriptor();
    return ServerMethodDefinition.create(
        descriptor.toBuilder(zeroCopyAppendEntries, descriptor.getResponseMarshaller()).build(),
        method.getServerCallHandler());
  }

  /**
   * Copy everything in the request except the state machine data, so that only the state machine data refers to
   * the zero-copy buffers.
   */
  static AppendEntriesRequestProto copyAllButStateMachineData(AppendEntriesRequestProto request) {
    final AppendEntriesRequestProto.Builder builder = copy(request.toBuilder().clearEntries().build()).toBuilder();
    for (LogEntryProto entry : request.getEntriesList()) {
      builder.addEntries(copyAllButStateMachineData(entry));
    }
    return builder.build();
  }

  private static LogEntryProto copyAllButStateMachineData(LogEntryProto entry) {
    if (!entry.hasStateMachineLogEntry() || !entry.getStateMachineLogEntry().hasStateMachineEntry()) {
      return copy(entry);
    }
    final StateMachineLogEntryProto smLogEntry = entry.getStateMachineLogEntry();
    final StateMachineEntryProto smEntry = smLogEntry.getStateMachineEntry();
    final LogEntryProto.Builder copied = copy(entry.toBuilder().setStateMachineLogEntry(smLogEntry.toBuilder()
        .setStateMachineEntry(smEntry.toBuilder().clearStateMachineData())).build()).toBuilder();
    copied.getStateMachineLogEntryBuilder().getStateMachineEntryBuilder()
        .setStateMachineData(smEntry.getStateMachineData());
    return copied.build();
  }

  /** A deep copy: parsing from a new array without aliasing. */
  @SuppressWarnings("unchecked")
  private static <M extends MessageLite> M copy(M message) {
    try {
      return (M) message.getParserForType().parseFrom(message.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("Failed to copy " + message.getClass().getSimpleName(), e);
    }
  }

  private void release(AppendEntriesRequestProto zeroCopied, Throwable error) {
    if (error == null) {
      zeroCopyAppendEntries.release(zeroCopied);
    } else {
      // the log or state machine may still be writing the data; keep the buffers rather than risk corrupting it
      final long n = UNRELEASED_ON_FAILURE.incrementAndGet();
      LOG.warn("{}: Not releasing the zero-copy buffers of a failed appendEntries ({} so far)", getId(), n, error);
    }
  }

  RaftPeerId getId() {
    return idSupplier.get();
  }

  @Override
  public void requestVote(RequestVoteRequestProto request,
      StreamObserver<RequestVoteReplyProto> responseObserver) {
    try {
      final RequestVoteReplyProto reply = server.requestVote(request);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      GrpcUtil.warn(LOG, () -> getId() + ": Failed requestVote " + ProtoUtils.toString(request.getServerRequest()), e);
      responseObserver.onError(GrpcUtil.wrapException(e));
    }
  }

  @Override
  public void startLeaderElection(StartLeaderElectionRequestProto request,
      StreamObserver<StartLeaderElectionReplyProto> responseObserver) {
    try {
      final StartLeaderElectionReplyProto reply = server.startLeaderElection(request);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Throwable e) {
      GrpcUtil.warn(LOG,
          () -> getId() + ": Failed startLeaderElection " + ProtoUtils.toString(request.getServerRequest()), e);
      responseObserver.onError(GrpcUtil.wrapException(e));
    }
  }

  @Override
  public void readIndex(ReadIndexRequestProto request, StreamObserver<ReadIndexReplyProto> responseObserver) {
    final Consumer<Throwable> warning = e -> GrpcUtil.warn(LOG,
            () -> getId() + ": Failed readIndex " + ProtoUtils.toString(request.getServerRequest()), e);
    GrpcUtil.asyncCall(responseObserver, () -> server.readIndexAsync(request), Function.identity(), warning);
  }

  @Override
  public StreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseObserver) {
    return new ServerRequestStreamObserver<AppendEntriesRequestProto, AppendEntriesReplyProto>(
        RaftServerProtocol.Op.APPEND_ENTRIES, responseObserver) {
      /** The copied requests (see {@link #copyAllButStateMachineData}) to the zero-copy ones they came from. */
      private final Map<AppendEntriesRequestProto, AppendEntriesRequestProto> zeroCopied
          = Collections.synchronizedMap(new IdentityHashMap<>());

      @Override
      public void onNext(AppendEntriesRequestProto request) {
        if (zeroCopyAppendEntries == null) {
          super.onNext(request);
          return;
        }
        final AppendEntriesRequestProto copied;
        try {
          copied = copyAllButStateMachineData(request);
        } catch (RuntimeException e) {
          zeroCopyAppendEntries.release(request);
          throw e;
        }
        zeroCopied.put(copied, request);
        super.onNext(copied);
      }

      @Override
      CompletableFuture<AppendEntriesReplyProto> process(AppendEntriesRequestProto request) throws IOException {
        final AppendEntriesRequestProto zeroCopy = zeroCopyAppendEntries == null ? null : zeroCopied.remove(request);
        if (zeroCopy == null) {
          return server.appendEntriesAsync(request);
        }
        final CompletableFuture<AppendEntriesReplyProto> future;
        try {
          future = server.appendEntriesAsync(request);
        } catch (IOException | RuntimeException e) {
          // failed before appending anything
          zeroCopyAppendEntries.release(zeroCopy);
          throw e;
        }
        // the reply is sent once the entries and their state machine data are written
        future.whenComplete((reply, e) -> release(zeroCopy, e));
        return future;
      }

      @Override
      long getCallId(AppendEntriesRequestProto request) {
        return request.getServerRequest().getCallId();
      }

      @Override
      String requestToString(AppendEntriesRequestProto request) {
        return ServerStringUtils.toAppendEntriesRequestString(request, null);
      }

      @Override
      String replyToString(AppendEntriesReplyProto reply) {
        return ServerStringUtils.toAppendEntriesReplyString(reply);
      }

      @Override
      boolean replyInOrder(AppendEntriesRequestProto request) {
        return request.getEntriesCount() != 0;
      }

      @Override
      StatusRuntimeException wrapException(Throwable e, AppendEntriesRequestProto request) {
        return GrpcUtil.wrapException(e, getCallId(request), request.getEntriesCount() == 0);
      }
    };
  }

  @Override
  public StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      StreamObserver<InstallSnapshotReplyProto> responseObserver) {
    return new ServerRequestStreamObserver<InstallSnapshotRequestProto, InstallSnapshotReplyProto>(
        RaftServerProtocol.Op.INSTALL_SNAPSHOT, responseObserver) {
      @Override
      CompletableFuture<InstallSnapshotReplyProto> process(InstallSnapshotRequestProto request) throws IOException {
        return CompletableFuture.completedFuture(server.installSnapshot(request));
      }

      @Override
      long getCallId(InstallSnapshotRequestProto request) {
        return request.getServerRequest().getCallId();
      }

      @Override
      String requestToString(InstallSnapshotRequestProto request) {
        return ServerStringUtils.toInstallSnapshotRequestString(request);
      }

      @Override
      String replyToString(InstallSnapshotReplyProto reply) {
        return ServerStringUtils.toInstallSnapshotReplyString(reply);
      }

      @Override
      boolean replyInOrder(InstallSnapshotRequestProto installSnapshotRequestProto) {
        return true;
      }
    };
  }
}
