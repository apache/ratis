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
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class StepDownLeader {
  public static final Logger LOG = LoggerFactory.getLogger(StepDownLeader.class);

  class PendingRequest {
    private final TransferLeadershipRequest request;
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();

    PendingRequest(TransferLeadershipRequest request) {
      this.request = request;
    }

    TransferLeadershipRequest getRequest() {
      return request;
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    void complete() {
      LOG.info("Successfully step down leader at {} for request {}", server.getMemberId(), request);
      replyFuture.complete(server.newSuccessReply(request));
    }

    void timeout() {
      replyFuture.completeExceptionally(new TimeoutIOException(
          ": Failed to step down leader on " +  server.getMemberId() + "request " + request.getTimeoutMs() + "ms"));
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

  StepDownLeader(RaftServerImpl server) {
    this.server = server;
  }

  CompletableFuture<RaftClientReply> stepDownLeaderAsync(TransferLeadershipRequest request) {
    final MemoizedSupplier<PendingRequest> supplier = JavaUtils.memoize(() -> new PendingRequest(request));
    final PendingRequest previous = pending.getAndUpdate(supplier);
    if (previous != null) {
      return previous.getReplyFuture();
    }
    server.getRole().getLeaderState()
        .ifPresent(leader -> leader.submitStepDownEvent(LeaderState.StepDownReason.NO_LEADER_MODE));
    scheduler.onTimeout(TimeDuration.valueOf(request.getTimeoutMs(), TimeUnit.MILLISECONDS),
        this::timeout, LOG, () -> "Timeout check failed for step down leader request: " + request);
    return supplier.get().getReplyFuture();
  }

  void completeStepDownLeader() {
    pending.getAndSetNull().ifPresent(PendingRequest::complete);
  }

  void timeout() {
    pending.getAndSetNull().ifPresent(PendingRequest::timeout);
  }
}
