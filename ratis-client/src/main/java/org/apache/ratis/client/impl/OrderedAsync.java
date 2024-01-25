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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.retry.ClientRetryEvent;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.impl.RaftClientImpl.PendingClientRequest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.proto.RaftProtos.SlidingWindowEntry;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.util.BatchLogger;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.SlidingWindow;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;

/** Send ordered asynchronous requests to a raft service. */
public final class OrderedAsync {
  public static final Logger LOG = LoggerFactory.getLogger(OrderedAsync.class);

  private enum BatchLogKey implements BatchLogger.Key {
    SEND_REQUEST_EXCEPTION
  }

  static class PendingOrderedRequest extends PendingClientRequest
      implements SlidingWindow.ClientSideRequest<RaftClientReply> {
    private final long callId;
    private final long seqNum;
    private final AtomicReference<Function<SlidingWindowEntry, RaftClientRequest>> requestConstructor;
    private volatile boolean isFirst = false;

    PendingOrderedRequest(long callId, long seqNum,
        Function<SlidingWindowEntry, RaftClientRequest> requestConstructor) {
      this.callId = callId;
      this.seqNum = seqNum;
      this.requestConstructor = new AtomicReference<>(requestConstructor);
    }

    @Override
    public RaftClientRequest newRequestImpl() {
      return Optional.ofNullable(requestConstructor.get())
          .map(f -> f.apply(ProtoUtils.toSlidingWindowEntry(seqNum, isFirst)))
          .orElse(null);
    }

    @Override
    public void setFirstRequest() {
      isFirst = true;
    }

    @Override
    public long getSeqNum() {
      return seqNum;
    }

    @Override
    public boolean hasReply() {
      return getReplyFuture().isDone();
    }

    @Override
    public void setReply(RaftClientReply reply) {
      requestConstructor.set(null);
      getReplyFuture().complete(reply);
    }

    @Override
    public void fail(Throwable e) {
      requestConstructor.set(null);
      getReplyFuture().completeExceptionally(e);
    }

    @Override
    public String toString() {
      return "[cid=" + callId + ", seq=" + getSeqNum() + "]";
    }
  }

  static OrderedAsync newInstance(RaftClientImpl client, RaftProperties properties) {
    final OrderedAsync ordered = new OrderedAsync(client, properties);
    // send a dummy watch request to establish the connection
    // TODO: this is a work around, it is better to fix the underlying RPC implementation
    if (RaftClientConfigKeys.Async.Experimental.sendDummyRequest(properties)) {
      ordered.send(RaftClientRequest.watchRequestType(), null, null);
    }
    return ordered;
  }

  private final RaftClientImpl client;
  /** Map: id -> {@link SlidingWindow}, in order to support async calls to the Raft service or individual servers. */
  private final ConcurrentMap<String, SlidingWindow.Client<PendingOrderedRequest, RaftClientReply>> slidingWindows
      = new ConcurrentHashMap<>();
  private final Semaphore requestSemaphore;

  private OrderedAsync(RaftClientImpl client, RaftProperties properties) {
    this.client = Objects.requireNonNull(client, "client == null");
    this.requestSemaphore = new Semaphore(RaftClientConfigKeys.Async.outstandingRequestsMax(properties));
  }

  private void resetSlidingWindow(RaftClientRequest request) {
    getSlidingWindow(request).resetFirstSeqNum();
  }

  private SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> getSlidingWindow(RaftClientRequest request) {
    return getSlidingWindow(request.isToLeader()? null: request.getServerId());
  }

  private SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> getSlidingWindow(RaftPeerId target) {
    final String id = target != null ? target.toString() : "RAFT";
    return slidingWindows.computeIfAbsent(id, key -> new SlidingWindow.Client<>(client.getId() + "->" + key));
  }

  private void failAllAsyncRequests(RaftClientRequest request, Throwable t) {
    getSlidingWindow(request).fail(request.getSlidingWindowEntry().getSeqNum(), t);
  }

  CompletableFuture<RaftClientReply> send(RaftClientRequest.Type type, Message message, RaftPeerId server) {
    if (!type.is(TypeCase.WATCH) && !type.is(TypeCase.MESSAGESTREAM)) {
      Objects.requireNonNull(message, "message == null");
    }
    try {
      requestSemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return JavaUtils.completeExceptionally(IOUtils.toInterruptedIOException(
          "Interrupted when sending " + type + ", message=" + message, e));
    }

    final long callId = CallId.getAndIncrement();
    final LongFunction<PendingOrderedRequest> constructor = seqNum -> new PendingOrderedRequest(callId, seqNum,
        slidingWindowEntry -> client.newRaftClientRequest(server, callId, message, type, slidingWindowEntry));
    return getSlidingWindow(server).submitNewRequest(constructor, this::sendRequestWithRetry
    ).getReplyFuture(
    ).thenApply(reply -> RaftClientImpl.handleRaftException(reply, CompletionException::new)
    ).whenComplete((r, e) -> {
      if (e != null) {
        if (e.getCause() instanceof AlreadyClosedException) {
          LOG.error("Failed to send request, message=" + message + " due to " + e);
        } else {
          LOG.error("Failed to send request, message=" + message, e);
        }
      }
      requestSemaphore.release();
    });
  }

  private void sendRequestWithRetry(PendingOrderedRequest pending) {
    if (pending == null) {
      return;
    }
    if (pending.getReplyFuture().isDone()) {
      return;
    }

    final RaftClientRequest request = pending.newRequest();
    if (request == null) { // already done
      LOG.debug("{} newRequest returns null", pending);
      return;
    }

    if (getSlidingWindow((RaftPeerId) null).isFirst(pending.getSeqNum())) {
      pending.setFirstRequest();
    }
    LOG.debug("{}: send* {}", client.getId(), request);
    client.getClientRpc().sendRequestAsync(request).thenAccept(reply -> {
      LOG.debug("{}: receive* {}", client.getId(), reply);
      Objects.requireNonNull(reply, "reply == null");
      client.handleReply(request, reply);
      getSlidingWindow(request).receiveReply(
          request.getSlidingWindowEntry().getSeqNum(), reply, this::sendRequestWithRetry);
    }).exceptionally(e -> {
      final Throwable exception = e;
      final String key = client.getId() + "-" + request.getCallId() + "-" + exception;
      final Consumer<String> op = suffix -> LOG.error("{} {}: Failed* {}", suffix, client.getId(), request, exception);
      BatchLogger.warn(BatchLogKey.SEND_REQUEST_EXCEPTION, key, op);
      handleException(pending, request, e);
      return null;
    });
  }

  private void handleException(PendingOrderedRequest pending, RaftClientRequest request, Throwable e) {
    final RetryPolicy retryPolicy = client.getRetryPolicy();
    if (client.isClosed()) {
      failAllAsyncRequests(request, new AlreadyClosedException(client + " is closed."));
      return;
    }

    e = JavaUtils.unwrapCompletionException(e);
    if (!(e instanceof IOException) || e instanceof GroupMismatchException) {
      // non-retryable exceptions
      failAllAsyncRequests(request, e);
      return;
    }

    final ClientRetryEvent event = pending.newClientRetryEvent(request, e);
    final RetryPolicy.Action action = retryPolicy.handleAttemptFailure(event);
    if (!action.shouldRetry()) {
      failAllAsyncRequests(request, client.noMoreRetries(event));
      return;
    }

    if (e instanceof NotLeaderException) {
      client.handleNotLeaderException(request, (NotLeaderException) e, this::resetSlidingWindow);
    } else {
      client.handleIOException(request, (IOException) e, null, this::resetSlidingWindow);
    }
    final TimeDuration sleepTime = client.getEffectiveSleepTime(e, action.getSleepTime());
    LOG.debug("schedule* retry with sleep {} for attempt #{} of {}, {}",
        sleepTime, event.getAttemptCount(), request, retryPolicy);
    final SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> slidingWindow = getSlidingWindow(request);
    client.getScheduler().onTimeout(sleepTime,
        () -> slidingWindow.retry(pending, this::sendRequestWithRetry),
        LOG, () -> "Failed* to retry " + pending);
  }

  void assertRequestSemaphore(int expectedAvailablePermits, int expectedQueueLength) {
    Preconditions.assertSame(expectedAvailablePermits, requestSemaphore.availablePermits(), "availablePermits");
    Preconditions.assertSame(expectedQueueLength, requestSemaphore.getQueueLength(), "queueLength");
  }
}
