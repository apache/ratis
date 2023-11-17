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

import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.grpc.util.ZeroCopyMessageMarshaller;
import org.apache.ratis.grpc.util.ZeroCopyReadinessChecker;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.server.util.DirectBufferCleaner;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceImplBase;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.getAppendEntriesMethod;

class GrpcServerProtocolService extends RaftServerProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcServerProtocolService.class);
  private final ZeroCopyMessageMarshaller<AppendEntriesRequestProto>
      zeroCopyAppendEntryMarshaller = new ZeroCopyMessageMarshaller<>(AppendEntriesRequestProto.getDefaultInstance());
  private final boolean zerocopyEnabled;


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

  abstract class ServerRequestStreamObserver<REQUEST, REPLY> implements StreamObserver<REQUEST> {
    private final RaftServer.Op op;
    private final StreamObserver<REPLY> responseObserver;
    /** For ordered {@link #onNext(Object)} requests. */
    private final AtomicReference<PendingServerRequest<REQUEST>> previousOnNext = new AtomicReference<>();
    /** For both ordered and unordered {@link #onNext(Object)} requests. */
    private final AtomicReference<CompletableFuture<REPLY>> requestFuture
        = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    ServerRequestStreamObserver(RaftServer.Op op, StreamObserver<REPLY> responseObserver) {
      this.op = op;
      this.responseObserver = responseObserver;
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
      if (!isClosed.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: reply {}", getId(), replyToString(reply));
        }
        responseObserver.onNext(reply);
      }
      return reply;
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
        LOG.info("{}: Completed {}, lastRequest: {}", getId(), op, getPreviousRequestString());
        requestFuture.get().thenAccept(reply -> {
          LOG.info("{}: Completed {}, lastReply: {}", getId(), op, reply);
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

  GrpcServerProtocolService(Supplier<RaftPeerId> idSupplier, RaftServer server, boolean zerocopyEnabled) {
    this.idSupplier = idSupplier;
    this.server = server;
    this.zerocopyEnabled = zerocopyEnabled;
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
    try {
      server.readIndexAsync(request).thenAccept(reply -> {
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      });
    } catch (Throwable e) {
      GrpcUtil.warn(LOG,
          () -> getId() + ": Failed readIndex " + ProtoUtils.toString(request.getServerRequest()), e);
      responseObserver.onError(GrpcUtil.wrapException(e));
    }
  }

  @Override
  public StreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseObserver) {
    return new ServerRequestStreamObserver<AppendEntriesRequestProto, AppendEntriesReplyProto>(
        RaftServerProtocol.Op.APPEND_ENTRIES, responseObserver) {

      @Override
      CompletableFuture<AppendEntriesReplyProto> process(AppendEntriesRequestProto request) throws IOException {
        InputStream handle = zeroCopyAppendEntryMarshaller.popStream(request);
        try {
          return server.appendEntriesAsync(request)
              .whenCompleteAsync((x, e) -> {
                if (x != null && x.getResult().equals(AppendEntriesReplyProto.AppendResult.SUCCESS) && !x.getIsHearbeat()) {
                  DirectBufferCleaner.INSTANCE.watch(request, handle);
                } else {
                  org.apache.ratis.thirdparty.io.grpc.internal.GrpcUtil.closeQuietly(handle);
                }
              });
        }
        catch (IOException e) {
          org.apache.ratis.thirdparty.io.grpc.internal.GrpcUtil.closeQuietly(handle);
          throw e;
        }
      }

      @Override
      long getCallId(AppendEntriesRequestProto request) {
        return request.getServerRequest().getCallId();
      }

      @Override
      String requestToString(AppendEntriesRequestProto request) {
        return ServerStringUtils.toAppendEntriesRequestString(request);
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

  /**
   * Bind this grpc service with zero-copy marshaller for
   * ordered and unordered methods if zero-copy is enabled.
   */
  public ServerServiceDefinition bindServiceWithZeroCopy() {
    ServerServiceDefinition orig = super.bindService();
    // TODO if zero-copy is not enabled or not feasible, return the original service definition.
    if (!ZeroCopyReadinessChecker.isReady()) {
      LOG.info("Zero copy is not ready.");
      return orig;
    }

    if (!zerocopyEnabled) {
      LOG.info("Zero copy is not enabled.");
      return orig;
    }

    ServerServiceDefinition.Builder builder =
        ServerServiceDefinition.builder(orig.getServiceDescriptor().getName());

    addZeroCopyMethod(orig, builder, getAppendEntriesMethod());

    // Add methods that we don't apply zero-copy.
    orig.getMethods().stream().filter(
        x -> !x.getMethodDescriptor().getFullMethodName().equals(getAppendEntriesMethod().getFullMethodName())
    ).forEach(
        builder::addMethod
    );

    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private void addZeroCopyMethod(ServerServiceDefinition orig,
      ServerServiceDefinition.Builder builder,
      MethodDescriptor<AppendEntriesRequestProto, AppendEntriesReplyProto> origMethod) {
    MethodDescriptor<AppendEntriesRequestProto, AppendEntriesReplyProto> newMethod = origMethod.toBuilder()
        .setRequestMarshaller(zeroCopyAppendEntryMarshaller)
        .build();
    ServerCallHandler<AppendEntriesRequestProto, AppendEntriesReplyProto> serverCallHandler =
        (ServerCallHandler<AppendEntriesRequestProto, AppendEntriesReplyProto>) orig.getMethod(newMethod.getFullMethodName()).getServerCallHandler();
    builder.addMethod(newMethod, serverCallHandler);
  }
}
