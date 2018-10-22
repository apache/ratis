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
package org.apache.ratis.grpc.client;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.protocol.*;
import org.apache.ratis.util.TimeoutScheduler;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.netty.NegotiationType;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.grpc.AdminProtocolServiceGrpc;
import org.apache.ratis.proto.grpc.AdminProtocolServiceGrpc.AdminProtocolServiceBlockingStub;
import org.apache.ratis.proto.grpc.RaftClientProtocolServiceGrpc;
import org.apache.ratis.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceBlockingStub;
import org.apache.ratis.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceStub;
import org.apache.ratis.util.CheckedSupplier;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class GrpcClientProtocolClient implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcClientProtocolClient.class);

  private final Supplier<String> name;
  private final RaftPeer target;
  private final ManagedChannel channel;

  private final TimeDuration requestTimeoutDuration;
  private final TimeoutScheduler scheduler = TimeoutScheduler.newInstance(1);

  private final RaftClientProtocolServiceBlockingStub blockingStub;
  private final RaftClientProtocolServiceStub asyncStub;
  private final AdminProtocolServiceBlockingStub adminBlockingStub;

  private final AtomicReference<AsyncStreamObservers> appendStreamObservers = new AtomicReference<>();

  public GrpcClientProtocolClient(ClientId id, RaftPeer target, RaftProperties properties) {
    this.name = JavaUtils.memoize(() -> id + "->" + target.getId());
    this.target = target;

    final SizeInBytes flowControlWindow = GrpcConfigKeys.flowControlWindow(properties, LOG::debug);
    final SizeInBytes maxMessageSize = GrpcConfigKeys.messageSizeMax(properties, LOG::debug);
    channel = NettyChannelBuilder.forTarget(target.getAddress())
        .negotiationType(NegotiationType.PLAINTEXT)
        .flowControlWindow(flowControlWindow.getSizeInt())
        .maxInboundMessageSize(maxMessageSize.getSizeInt())
        .build();
    blockingStub = RaftClientProtocolServiceGrpc.newBlockingStub(channel);
    asyncStub = RaftClientProtocolServiceGrpc.newStub(channel);
    adminBlockingStub = AdminProtocolServiceGrpc.newBlockingStub(channel);
    this.requestTimeoutDuration = RaftClientConfigKeys.Rpc.requestTimeout(properties);
  }

  String getName() {
    return name.get();
  }

  @Override
  public void close() {
    final AsyncStreamObservers observers = appendStreamObservers.get();
    if (observers != null) {
      observers.close();
    }
    channel.shutdownNow();
  }

  RaftClientReplyProto groupAdd(GroupManagementRequestProto request) throws IOException {
    return blockingCall(() -> adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .groupManagement(request));
  }

  GroupListReplyProto groupList(GroupListRequestProto request) {
    return adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .groupList(request);
  }

  GroupInfoReplyProto groupInfo(GroupInfoRequestProto request) {
    return adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .groupInfo(request);
  }


  RaftClientReplyProto setConfiguration(
      SetConfigurationRequestProto request) throws IOException {
    return blockingCall(() -> blockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .setConfiguration(request));
  }

  private static RaftClientReplyProto blockingCall(
      CheckedSupplier<RaftClientReplyProto, StatusRuntimeException> supplier
      ) throws IOException {
    try {
      return supplier.get();
    } catch (StatusRuntimeException e) {
      throw GrpcUtil.unwrapException(e);
    }
  }

  StreamObserver<RaftClientRequestProto> append(
      StreamObserver<RaftClientReplyProto> responseHandler) {
    return asyncStub.append(responseHandler);
  }

  StreamObserver<RaftClientRequestProto> appendWithTimeout(
      StreamObserver<RaftClientReplyProto> responseHandler) {
    return asyncStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .append(responseHandler);
  }

  AsyncStreamObservers getAppendStreamObservers() {
    return appendStreamObservers.updateAndGet(a -> a != null? a : new AsyncStreamObservers());
  }

  public RaftPeer getTarget() {
    return target;
  }

  class AsyncStreamObservers implements Closeable {
    /** Request map: callId -> future */
    private final AtomicReference<Map<Long, CompletableFuture<RaftClientReply>>> replies = new AtomicReference<>(new ConcurrentHashMap<>());
    private final StreamObserver<RaftClientReplyProto> replyStreamObserver = new StreamObserver<RaftClientReplyProto>() {
      @Override
      public void onNext(RaftClientReplyProto proto) {
        final long callId = proto.getRpcReply().getCallId();
        try {
          final RaftClientReply reply = ClientProtoUtils.toRaftClientReply(proto);
          final NotLeaderException nle = reply.getNotLeaderException();
          if (nle != null) {
            completeReplyExceptionally(nle, NotLeaderException.class.getName());
            return;
          }
          handleReplyFuture(callId, f -> f.complete(reply));
        } catch (Throwable t) {
          handleReplyFuture(callId, f -> f.completeExceptionally(t));
        }
      }

      @Override
      public void onError(Throwable t) {
        final IOException ioe = GrpcUtil.unwrapIOException(t);
        completeReplyExceptionally(ioe, "onError");
      }

      @Override
      public void onCompleted() {
        completeReplyExceptionally(null, "completed");
      }
    };
    private final StreamObserver<RaftClientRequestProto> requestStreamObserver = append(replyStreamObserver);

    CompletableFuture<RaftClientReply> onNext(RaftClientRequest request) {
      final Map<Long, CompletableFuture<RaftClientReply>> map = replies.get();
      if (map == null) {
        return JavaUtils.completeExceptionally(new IOException("Already closed."));
      }
      final CompletableFuture<RaftClientReply> f = new CompletableFuture<>();
      CollectionUtils.putNew(request.getCallId(), f, map,
          () -> getName() + ":" + getClass().getSimpleName());
      try {
        requestStreamObserver.onNext(ClientProtoUtils.toRaftClientRequestProto(request));
        scheduler.onTimeout(requestTimeoutDuration, () -> timeoutCheck(request), LOG,
            () -> "Timeout check failed for client request: " + request);
      } catch(Throwable t) {
        handleReplyFuture(request.getCallId(), future -> future.completeExceptionally(t));
      }
      return f;
    }

    private void timeoutCheck(RaftClientRequest request) {
      handleReplyFuture(request.getCallId(), f -> f.completeExceptionally(
          new IOException("Request timeout " + requestTimeoutDuration + ": " + request)));
    }

    private void handleReplyFuture(long callId, Consumer<CompletableFuture<RaftClientReply>> handler) {
      Optional.ofNullable(replies.get())
          .map(replyMap -> replyMap.remove(callId))
          .ifPresent(handler);
    }

    @Override
    public void close() {
      requestStreamObserver.onCompleted();
      completeReplyExceptionally(null, "close");
    }

    private void completeReplyExceptionally(Throwable t, String event) {
      appendStreamObservers.compareAndSet(this, null);
      final Map<Long, CompletableFuture<RaftClientReply>> map = replies.getAndSet(null);
      if (map == null) {
        return;
      }
      for (Map.Entry<Long, CompletableFuture<RaftClientReply>> entry : map.entrySet()) {
        final CompletableFuture<RaftClientReply> f = entry.getValue();
        if (!f.isDone()) {
          f.completeExceptionally(t != null? t
              : new IOException(getName() + ": Stream " + event
                  + ": no reply for async request cid=" + entry.getKey()));
        }
      }
    }
  }
}
