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
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.CallId;
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
import java.util.function.Function;
import java.util.function.LongFunction;

/** Send ordered asynchronous requests to a raft service. */
public final class OrderedAsync {
  public static final Logger LOG = LoggerFactory.getLogger(OrderedAsync.class);

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
    return getSlidingWindow(request.is(TypeCase.STALEREAD) ? request.getServerId() : null);
  }

  private SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> getSlidingWindow(RaftPeerId target) {
    final String id = target != null ? target.toString() : "RAFT";
    return slidingWindows.computeIfAbsent(id, key -> new SlidingWindow.Client<>(client.getId() + "->" + key));
  }

  private void failAllAsyncRequests(RaftClientRequest request, Throwable t) {
    getSlidingWindow(request).fail(request.getSlidingWindowEntry().getSeqNum(), t);
  }

  private void handleAsyncRetryFailure(ClientRetryEvent event) {
    failAllAsyncRequests(event.getRequest(), client.noMoreRetries(event));
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
        LOG.error("Failed to send request, message=" + message, e);
      }
      requestSemaphore.release();
    });
  }

  private void sendRequestWithRetry(PendingOrderedRequest pending) {
    final CompletableFuture<RaftClientReply> f = pending.getReplyFuture();
    if (f.isDone()) {
      return;
    }

    final RaftClientRequest request = pending.newRequestImpl();
    if (request == null) { // already done
      LOG.debug("{} newRequestImpl returns null", pending);
      return;
    }

    final RetryPolicy retryPolicy = client.getRetryPolicy();
    sendRequest(pending).thenAccept(reply -> {
      if (f.isDone()) {
        return;
      }
      if (reply == null) {
        scheduleWithTimeout(pending, request, retryPolicy, null);
      } else {
        client.handleReply(request, reply);
        f.complete(reply);
      }
    }).exceptionally(e -> {
      if (e instanceof CompletionException) {
        e = JavaUtils.unwrapCompletionException(e);
        scheduleWithTimeout(pending, request, retryPolicy, e);
        return null;
      }
      f.completeExceptionally(e);
      return null;
    });
  }

  private void scheduleWithTimeout(PendingOrderedRequest pending,
      RaftClientRequest request, RetryPolicy retryPolicy, Throwable e) {
    final int attempt = pending.getAttemptCount();
    final ClientRetryEvent event = new ClientRetryEvent(request, e, pending);
    final TimeDuration sleepTime = client.getEffectiveSleepTime(e,
        retryPolicy.handleAttemptFailure(event).getSleepTime());
    LOG.debug("schedule* attempt #{} with sleep {} and policy {} for {}", attempt, sleepTime, retryPolicy, request);
    scheduleWithTimeout(pending, sleepTime, getSlidingWindow(request));
  }

  private void scheduleWithTimeout(PendingOrderedRequest pending, TimeDuration sleepTime,
      SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> slidingWindow) {
    client.getScheduler().onTimeout(sleepTime,
        () -> slidingWindow.retry(pending, this::sendRequestWithRetry),
        LOG, () -> "Failed* to retry " + pending);
  }

  private CompletableFuture<RaftClientReply> sendRequest(PendingOrderedRequest pending) {
    final RetryPolicy retryPolicy = client.getRetryPolicy();
    final CompletableFuture<RaftClientReply> f;
    final RaftClientRequest request;
    if (getSlidingWindow((RaftPeerId) null).isFirst(pending.getSeqNum())) {
      pending.setFirstRequest();
    }
    request = pending.newRequest();
    LOG.debug("{}: send* {}", client.getId(), request);
    f = client.getClientRpc().sendRequestAsync(request);
    return f.thenApply(reply -> {
      LOG.debug("{}: receive* {}", client.getId(), reply);
      getSlidingWindow(request).receiveReply(
          request.getSlidingWindowEntry().getSeqNum(), reply, this::sendRequestWithRetry);
      return reply;
    }).exceptionally(e -> {
      if (LOG.isTraceEnabled()) {
        LOG.trace(client.getId() + ": Failed* " + request, e);
      } else {
        LOG.debug("{}: Failed* {} with {}", client.getId(), request, e);
      }
      e = JavaUtils.unwrapCompletionException(e);
      if (e instanceof IOException && !(e instanceof GroupMismatchException)) {
        pending.incrementExceptionCount(e);
        final ClientRetryEvent event = new ClientRetryEvent(request, e, pending);
        if (!retryPolicy.handleAttemptFailure(event).shouldRetry()) {
          handleAsyncRetryFailure(event);
        } else {
          if (e instanceof NotLeaderException) {
            NotLeaderException nle = (NotLeaderException)e;
            client.handleNotLeaderException(request, nle, this::resetSlidingWindow);
          } else {
            client.handleIOException(request, (IOException) e, null, this::resetSlidingWindow);
          }
        }
        throw new CompletionException(e);
      }
      failAllAsyncRequests(request, e);
      return null;
    });
  }

  void assertRequestSemaphore(int expectedAvailablePermits, int expectedQueueLength) {
    Preconditions.assertTrue(requestSemaphore.availablePermits() == expectedAvailablePermits);
    Preconditions.assertTrue(requestSemaphore.getQueueLength() == expectedQueueLength);
  }
}
