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
package org.apache.ratis.grpc.client;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceImplBase;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class GrpcClientProtocolService extends RaftClientProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcClientProtocolService.class);

  private static class PendingAppend implements SlidingWindow.ServerSideRequest<RaftClientReply> {
    private final RaftClientRequest request;
    private volatile RaftClientReply reply;

    PendingAppend(RaftClientRequest request) {
      this.request = request;
    }

    @Override
    public boolean hasReply() {
      return reply != null || this == COMPLETED;
    }

    @Override
    public void setReply(RaftClientReply reply) {
      this.reply = reply;
    }

    RaftClientReply getReply() {
      return reply;
    }

    RaftClientRequest getRequest() {
      return request;
    }

    @Override
    public long getSeqNum() {
      return request != null? request.getSlidingWindowEntry().getSeqNum(): Long.MAX_VALUE;
    }

    @Override
    public boolean isFirstRequest() {
      return request != null && request.getSlidingWindowEntry().getIsFirst();
    }

    @Override
    public String toString() {
      return request != null? getSeqNum() + ":" + reply: "COMPLETED";
    }
  }
  private static final PendingAppend COMPLETED = new PendingAppend(null);

  private final Supplier<RaftPeerId> idSupplier;
  private final RaftClientAsynchronousProtocol protocol;

  public GrpcClientProtocolService(Supplier<RaftPeerId> idSupplier, RaftClientAsynchronousProtocol protocol) {
    this.idSupplier = idSupplier;
    this.protocol = protocol;
  }

  RaftPeerId getId() {
    return idSupplier.get();
  }

  @Override
  public void setConfiguration(SetConfigurationRequestProto proto,
      StreamObserver<RaftClientReplyProto> responseObserver) {
    final SetConfigurationRequest request = ClientProtoUtils.toSetConfigurationRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.setConfigurationAsync(request),
        ClientProtoUtils::toRaftClientReplyProto);
  }

  @Override
  public StreamObserver<RaftClientRequestProto> append(
      StreamObserver<RaftClientReplyProto> responseObserver) {
    return new AppendRequestStreamObserver(responseObserver);
  }

  @Override
  public StreamObserver<RaftClientRequestProto> unordered(StreamObserver<RaftClientReplyProto> responseObserver) {
    return new UnorderedRequestStreamObserver(responseObserver);
  }

  private final AtomicInteger streamCount = new AtomicInteger();

  private abstract class RequestStreamObserver implements StreamObserver<RaftClientRequestProto> {
    private final String name = getId() + "-" + getClass().getSimpleName() + streamCount.getAndIncrement();
    private final StreamObserver<RaftClientReplyProto> responseObserver;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    RequestStreamObserver(StreamObserver<RaftClientReplyProto> responseObserver) {
      LOG.debug("new {}", name);
      this.responseObserver = responseObserver;
    }

    String getName() {
      return name;
    }

    synchronized void responseNext(RaftClientReplyProto reply) {
      responseObserver.onNext(reply);
    }

    synchronized void responseCompleted() {
      responseObserver.onCompleted();
    }

    synchronized void responseError(Throwable t) {
      responseObserver.onError(t);
    }


    boolean setClose() {
      return isClosed.compareAndSet(false, true);
    }

    CompletableFuture<Void> processClientRequest(RaftClientRequest request, Consumer<RaftClientReply> replyHandler) {
      try {
        return protocol.submitClientRequestAsync(request
        ).thenAcceptAsync(replyHandler
        ).exceptionally(exception -> {
          // TODO: the exception may be from either raft or state machine.
          // Currently we skip all the following responses when getting an
          // exception from the state machine.
          responseError(exception, () -> "processClientRequest for " + request);
          return null;
        });
      } catch (IOException e) {
        throw new CompletionException("Failed processClientRequest for " + request + " in " + name, e);
      }
    }

    abstract void processClientRequest(RaftClientRequest request);

    @Override
    public void onNext(RaftClientRequestProto request) {
      try {
        final RaftClientRequest r = ClientProtoUtils.toRaftClientRequest(request);
        processClientRequest(r);
      } catch (Throwable e) {
        responseError(e, () -> "onNext for " + ClientProtoUtils.toString(request) + " in " + name);
      }
    }

    @Override
    public void onError(Throwable t) {
      // for now we just log a msg
      GrpcUtil.warn(LOG, () -> name + ": onError", t);
    }


    boolean responseError(Throwable t, Supplier<String> message) {
      if (setClose()) {
        t = JavaUtils.unwrapCompletionException(t);
        if (LOG.isDebugEnabled()) {
          LOG.debug(name + ": Failed " + message.get(), t);
        }
        responseError(GrpcUtil.wrapException(t));
        return true;
      }
      return false;
    }
  }

  private class UnorderedRequestStreamObserver extends RequestStreamObserver {
    /** Map: callId -> futures (seqNum is not set for unordered requests) */
    private final Map<Long, CompletableFuture<Void>> futures = new HashMap<>();

    UnorderedRequestStreamObserver(StreamObserver<RaftClientReplyProto> responseObserver) {
      super(responseObserver);
    }

    @Override
    void processClientRequest(RaftClientRequest request) {
      final CompletableFuture<Void> f = processClientRequest(request, reply -> {
        if (!reply.isSuccess()) {
          LOG.info("Failed " + request + ", reply=" + reply);
        }
        final RaftClientReplyProto proto = ClientProtoUtils.toRaftClientReplyProto(reply);
        responseNext(proto);
      });
      final long callId = request.getCallId();
      put(callId, f);
      f.thenAccept(dummy -> remove(callId));
    }

    private synchronized void put(long callId, CompletableFuture<Void> f) {
      futures.put(callId, f);
    }
    private synchronized void remove(long callId) {
      futures.remove(callId);
    }

    private synchronized CompletableFuture<Void> allOfFutures() {
      return JavaUtils.allOf(futures.values());
    }

    @Override
    public void onCompleted() {
      allOfFutures().thenAccept(dummy -> {
        if (setClose()) {
          LOG.debug("{}: close", getName());
          responseCompleted();
        }
      });
    }
  }

  private class AppendRequestStreamObserver extends RequestStreamObserver {
    private final SlidingWindow.Server<PendingAppend, RaftClientReply> slidingWindow
        = new SlidingWindow.Server<>(getName(), COMPLETED);

    AppendRequestStreamObserver(StreamObserver<RaftClientReplyProto> responseObserver) {
      super(responseObserver);
    }

    void processClientRequest(PendingAppend pending) {
      final long seq = pending.getSeqNum();
      processClientRequest(pending.getRequest(),
          reply -> slidingWindow.receiveReply(seq, reply, this::sendReply, this::processClientRequest));
    }

    @Override
    void processClientRequest(RaftClientRequest r) {
      slidingWindow.receivedRequest(new PendingAppend(r), this::processClientRequest);
    }

    private void sendReply(PendingAppend ready) {
      Preconditions.assertTrue(ready.hasReply());
      if (ready == COMPLETED) {
        close();
      } else {
        LOG.debug("{}: sendReply seq={}, {}", getName(), ready.getSeqNum(), ready.getReply());
        responseNext(ClientProtoUtils.toRaftClientReplyProto(ready.getReply()));
      }
    }

    @Override
    public void onError(Throwable t) {
      // for now we just log a msg
      GrpcUtil.warn(LOG, () -> getName() + ": onError", t);
      slidingWindow.close();
    }

    @Override
    public void onCompleted() {
      if (slidingWindow.endOfRequests()) {
        close();
      }
    }

    private void close() {
      if (setClose()) {
        LOG.debug("{}: close", getName());
        responseCompleted();
        slidingWindow.close();
      }
    }

    @Override
    boolean responseError(Throwable t, Supplier<String> message) {
      if (super.responseError(t, message)) {
        slidingWindow.close();
        return true;
      }
      return false;
    }
  }
}
