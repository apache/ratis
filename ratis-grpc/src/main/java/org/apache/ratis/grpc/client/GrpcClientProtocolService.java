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

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.protocol.*;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.shaded.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceImplBase;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class GrpcClientProtocolService extends RaftClientProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcClientProtocolService.class);

  private static class PendingAppend implements SlidingWindow.Request<RaftClientReply> {
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
      return request != null? request.getSeqNum(): Long.MAX_VALUE;
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

  private final AtomicInteger streamCount = new AtomicInteger();

  private class AppendRequestStreamObserver implements
      StreamObserver<RaftClientRequestProto> {
    private final String name = getId() + "-" +  streamCount.getAndIncrement();
    private final StreamObserver<RaftClientReplyProto> responseObserver;
    private final SlidingWindow.Server<PendingAppend, RaftClientReply> slidingWindow
        = new SlidingWindow.Server<>(name, COMPLETED);
    private final AtomicBoolean isClosed;

    AppendRequestStreamObserver(StreamObserver<RaftClientReplyProto> ro) {
      LOG.debug("new AppendRequestStreamObserver {}", name);
      this.responseObserver = ro;
      this.isClosed = new AtomicBoolean(false);
    }

    void processClientRequestAsync(PendingAppend pending) {
      try {
        protocol.submitClientRequestAsync(pending.getRequest()
        ).thenAcceptAsync(reply -> slidingWindow.receiveReply(
            pending.getSeqNum(), reply, this::sendReply, this::processClientRequestAsync)
        ).exceptionally(exception -> {
          // TODO: the exception may be from either raft or state machine.
          // Currently we skip all the following responses when getting an
          // exception from the state machine.
          responseError(exception, () -> "processClientRequestAsync for " + pending.getRequest());
          return null;
        });
      } catch (IOException e) {
        throw new CompletionException("Failed processClientRequestAsync for " + pending.getRequest(), e);
      }
    }

    @Override
    public void onNext(RaftClientRequestProto request) {
      try {
        final RaftClientRequest r = ClientProtoUtils.toRaftClientRequest(request);
        final PendingAppend p = new PendingAppend(r);
        slidingWindow.receivedRequest(p, this::processClientRequestAsync);
      } catch (Throwable e) {
        responseError(e, () -> "onNext for " + ClientProtoUtils.toString(request));
      }
    }

    private void sendReply(PendingAppend ready) {
        Preconditions.assertTrue(ready.hasReply());
        if (ready == COMPLETED) {
          close();
        } else {
          LOG.debug("{}: sendReply seq={}, {}", name, ready.getSeqNum(), ready.getReply());
          responseObserver.onNext(
              ClientProtoUtils.toRaftClientReplyProto(ready.getReply()));
        }
    }

    @Override
    public void onError(Throwable t) {
      // for now we just log a msg
      GrpcUtil.warn(LOG, () -> name + ": onError", t);
      slidingWindow.close();
    }

    @Override
    public void onCompleted() {
      if (slidingWindow.endOfRequests()) {
        close();
      }
    }

    private void close() {
      if (isClosed.compareAndSet(false, true)) {
        LOG.debug("{}: close", name);
        responseObserver.onCompleted();
        slidingWindow.close();
      }
    }

    void responseError(Throwable t, Supplier<String> message) {
      if (isClosed.compareAndSet(false, true)) {
        t = JavaUtils.unwrapCompletionException(t);
        if (LOG.isDebugEnabled()) {
          LOG.debug(name + ": Failed " + message.get(), t);
        }
        responseObserver.onError(GrpcUtil.wrapException(t));
        slidingWindow.close();
      }
    }
  }
}