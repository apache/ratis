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
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public class PendingStepDown {
  public static final Logger LOG = LoggerFactory.getLogger(PendingStepDown.class);

  class PendingRequest {
    private final TransferLeadershipRequest request;
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();

    PendingRequest(TransferLeadershipRequest request) {
      this.request = request;
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    void complete(Function<TransferLeadershipRequest, RaftClientReply> newSuccessReply) {
      LOG.info("Successfully step down leader at {} for request {}", leader, request);
      replyFuture.complete(newSuccessReply.apply(request));
    }

    void timeout() {
      replyFuture.completeExceptionally(new TimeoutIOException(
          ": Failed to step down leader on " +  leader + "request " + request.getTimeoutMs() + "ms"));
    }

    @Override
    public String toString() {
      return request.toString();
    }
  }


  static class PendingRequestReference {
    private final AtomicReference<PendingRequest> ref = new AtomicReference<>();

    Optional<PendingRequest> getAndSetNull() {
      return Optional.ofNullable(ref.getAndSet(null));
    }

    PendingRequest getAndUpdate(Supplier<PendingRequest> supplier) {
      return ref.getAndUpdate(p -> p != null? p: supplier.get());
    }
  }

  private final LeaderStateImpl leader;
  private final TimeoutScheduler scheduler = TimeoutScheduler.getInstance();
  private final PendingRequestReference pending = new PendingRequestReference();

  PendingStepDown(LeaderStateImpl leaderState) {
    this.leader = leaderState;
  }

  CompletableFuture<RaftClientReply> submitAsync(TransferLeadershipRequest request) {
    Preconditions.assertNull(request.getNewLeader(), "request.getNewLeader()");
    final MemoizedSupplier<PendingRequest> supplier = JavaUtils.memoize(() -> new PendingRequest(request));
    final PendingRequest previous = pending.getAndUpdate(supplier);
    if (previous != null) {
      return previous.getReplyFuture();
    }
    leader.submitStepDownEvent(LeaderState.StepDownReason.FORCE);
    scheduler.onTimeout(TimeDuration.valueOf(request.getTimeoutMs(), TimeUnit.MILLISECONDS),
        this::timeout, LOG, () -> "Timeout check failed for step down leader request: " + request);
    return supplier.get().getReplyFuture();
  }

  void complete(Function<TransferLeadershipRequest, RaftClientReply> newSuccessReply) {
    pending.getAndSetNull().ifPresent(p -> p.complete(newSuccessReply));
  }

  void timeout() {
    pending.getAndSetNull().ifPresent(PendingRequest::timeout);
  }
}
