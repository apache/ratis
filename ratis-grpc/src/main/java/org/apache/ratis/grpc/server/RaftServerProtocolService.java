/**
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

import org.apache.ratis.grpc.RaftGrpcUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.shaded.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceImplBase;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class RaftServerProtocolService extends RaftServerProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerProtocolService.class);

  private final Supplier<RaftPeerId> idSupplier;
  private final RaftServer server;

  public RaftServerProtocolService(Supplier<RaftPeerId> idSupplier, RaftServer server) {
    this.idSupplier = idSupplier;
    this.server = server;
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
    } catch (Throwable e) {
      LOG.info("{} got exception when handling requestVote {}: {}",
          getId(), request.getServerRequest(), e);
      responseObserver.onError(RaftGrpcUtil.wrapException(e));
    }
  }

  @Override
  public StreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseObserver) {
    return new StreamObserver<AppendEntriesRequestProto>() {
      private final AtomicReference<CompletableFuture<Void>> previousOnNext =
          new AtomicReference<>(CompletableFuture.completedFuture(null));
      private final AtomicBoolean isClosed = new AtomicBoolean(false);

      @Override
      public void onNext(AppendEntriesRequestProto request) {
        final CompletableFuture<Void> current = new CompletableFuture<>();
        final CompletableFuture<Void> previous = previousOnNext.getAndSet(current);
        try {
          server.appendEntriesAsync(request).thenCombine(previous,
              (reply, v) -> {
            if (!isClosed.get()) {
              responseObserver.onNext(reply);
            }
            current.complete(null);
            return null;
          });
        } catch (Throwable e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("{} got exception when appendEntries {}: {}",
                getId(), ProtoUtils.toString(request.getServerRequest()), e);
          }
          responseObserver.onError(RaftGrpcUtil.wrapException(e, request.getServerRequest().getCallId()));
          current.completeExceptionally(e);
        }
      }

      @Override
      public void onError(Throwable t) {
        // for now we just log a msg
        LOG.info("{}: appendEntries on error. Exception: {}", getId(), t);
      }

      @Override
      public void onCompleted() {
        if (isClosed.compareAndSet(false, true)) {
          LOG.info("{}: appendEntries completed", getId());
          responseObserver.onCompleted();
        }
      }
    };
  }

  @Override
  public StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      StreamObserver<InstallSnapshotReplyProto> responseObserver) {
    return new StreamObserver<InstallSnapshotRequestProto>() {
      @Override
      public void onNext(InstallSnapshotRequestProto request) {
        try {
          final InstallSnapshotReplyProto reply = server.installSnapshot(request);
          responseObserver.onNext(reply);
        } catch (Throwable e) {
          LOG.info("{} got exception when handling installSnapshot {}: {}",
              getId(), request.getServerRequest(), e);
          responseObserver.onError(RaftGrpcUtil.wrapException(e));
        }
      }

      @Override
      public void onError(Throwable t) {
        LOG.info("{}: installSnapshot on error. Exception: {}", getId(), t);
      }

      @Override
      public void onCompleted() {
        LOG.info("{}: installSnapshot completed", getId());
        responseObserver.onCompleted();
      }
    };
  }
}
