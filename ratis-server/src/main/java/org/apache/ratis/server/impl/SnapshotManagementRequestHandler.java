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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class SnapshotManagementRequestHandler {
  public static final Logger LOG = LoggerFactory.getLogger(TransferLeadership.class);

  class PendingRequest {
    private final SnapshotManagementRequest request;
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
    private final AtomicBoolean triggerTakingSnapshot = new AtomicBoolean(true);

    PendingRequest(SnapshotManagementRequest request) {
      LOG.info("new PendingRequest " + request);
      this.request = request;
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    boolean shouldTriggerTakingSnapshot() {
      return triggerTakingSnapshot.getAndSet(false);
    }

    void complete(long index) {
      LOG.info("{}: Successfully take snapshot at index {} for request {}", server.getMemberId(), index, request);
      replyFuture.complete(server.newSuccessReply(request, index));
    }

    void timeout() {
      replyFuture.completeExceptionally(new TimeoutIOException(
          server.getMemberId() + ": Failed to take a snapshot within timeout " + request.getTimeoutMs() + "ms"));
    }


    @Override
    public String toString() {
      return request.toString();
    }
  }

  static class PendingRequestReference {
    private final AtomicReference<PendingRequest> ref = new AtomicReference<>();

    Optional<PendingRequest> get() {
      return Optional.ofNullable(ref.get());
    }

    Optional<PendingRequest> getAndSetNull() {
      return Optional.ofNullable(ref.getAndSet(null));
    }

    PendingRequest getAndUpdate(Supplier<PendingRequest> supplier) {
      return ref.getAndUpdate(p -> p != null? p: supplier.get());
    }
  }

  private final RaftServerImpl server;
  private final TimeoutScheduler scheduler = TimeoutScheduler.getInstance();
  private final PendingRequestReference pending = new PendingRequestReference();

  SnapshotManagementRequestHandler(RaftServerImpl server) {
    this.server = server;
  }

  CompletableFuture<RaftClientReply> takingSnapshotAsync(SnapshotManagementRequest request) {
    final MemoizedSupplier<PendingRequest> supplier = JavaUtils.memoize(() -> new PendingRequest(request));
    final PendingRequest previous = pending.getAndUpdate(supplier);
    if (previous != null) {
      return previous.getReplyFuture();
    }

    server.getState().notifyStateMachineUpdater();
    scheduler.onTimeout(TimeDuration.valueOf(request.getTimeoutMs(), TimeUnit.MILLISECONDS),
        this::timeout, LOG, () -> "Timeout check failed for snapshot request: " + request);
    return supplier.get().getReplyFuture();
  }

  boolean shouldTriggerTakingSnapshot() {
    return pending.get().map(PendingRequest::shouldTriggerTakingSnapshot).orElse(false);
  }

  void completeTakingSnapshot(long index) {
    pending.getAndSetNull().ifPresent(p -> p.complete(index));
  }

  void timeout() {
    pending.getAndSetNull().ifPresent(PendingRequest::timeout);
  }
}
