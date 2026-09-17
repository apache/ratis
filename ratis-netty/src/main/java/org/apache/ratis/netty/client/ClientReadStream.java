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
package org.apache.ratis.netty.client;

import org.apache.ratis.datastream.DataStreamObserver;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuf;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.thirdparty.io.netty.util.concurrent.ScheduledFuture;
import org.apache.ratis.util.NettyUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

class ClientReadStream {
  private final NettyClientReplies.RequestEntry terminalEntry;
  private final CompletableFuture<DataStreamReply> replyFuture;
  private final DataStreamObserver<ReferenceCountedObject<DataStreamReply>> replyHandler;
  private Supplier<ScheduledFuture<?>> timeoutScheduler;
  private ScheduledFuture<?> timeoutFuture;

  ClientReadStream(DataStreamRequest request, CompletableFuture<DataStreamReply> replyFuture,
      DataStreamObserver<ReferenceCountedObject<DataStreamReply>> replyHandler) {
    this.terminalEntry = new NettyClientReplies.RequestEntry(request);
    this.replyFuture = replyFuture;
    this.replyHandler = replyHandler;
  }

  synchronized boolean receiveReply(ReferenceCountedObject<DataStreamReply> ref) {
    NettyUtils.cancel(timeoutFuture);
    try {
      replyHandler.onNext(ref);
    } catch (Throwable t) {
      completeExceptionally(t);
      return true;
    }

    final DataStreamReplyByteBuf reply = Preconditions.assertInstanceOf(ref.get(), DataStreamReplyByteBuf.class);
    final boolean terminal = !reply.isSuccess() || terminalEntry.equals(new NettyClientReplies.RequestEntry(reply));
    if (terminal) {
      replyFuture.complete(reply.copy());
      return true;
    }
    scheduleTimeout();
    return false;
  }

  synchronized void completeExceptionally(Throwable t) {
    NettyUtils.cancel(timeoutFuture);
    replyFuture.completeExceptionally(t);
  }

  synchronized void scheduleTimeout(Supplier<ScheduledFuture<?>> scheduleMethod) {
    timeoutScheduler = scheduleMethod;
    scheduleTimeout();
  }

  private void scheduleTimeout() {
    if (!replyFuture.isDone() && timeoutScheduler != null) {
      timeoutFuture = timeoutScheduler.get();
    }
  }
}
