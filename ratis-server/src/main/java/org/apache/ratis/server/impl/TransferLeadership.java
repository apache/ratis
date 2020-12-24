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
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
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

public class TransferLeadership {
  public static final Logger LOG = LoggerFactory.getLogger(TransferLeadership.class);

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

    void complete(RaftPeerId currentLeader, boolean timeout) {
      if (replyFuture.isDone()) {
        return;
      }

      if (currentLeader != null && currentLeader.equals(request.getNewLeader())) {
        replyFuture.complete(server.newSuccessReply(request));
      } else if (timeout) {
        final TransferLeadershipException tle = new TransferLeadershipException(server.getMemberId()
            + ": Failed to transfer leadership to " + request.getNewLeader()
            + " (timed out " + request.getTimeoutMs() + "ms): current leader is " + currentLeader);
        replyFuture.complete(server.newExceptionReply(request, tle));
      }
    }

    @Override
    public String toString() {
      return request.toString();
    }
  }

  private final RaftServerImpl server;
  private final TimeoutScheduler scheduler = TimeoutScheduler.getInstance();
  private final AtomicReference<PendingRequest> pending = new AtomicReference<>();

  TransferLeadership(RaftServerImpl server) {
    this.server = server;
  }

  boolean isSteppingDown() {
    return pending.get() != null;
  }

  CompletableFuture<RaftClientReply> start(TransferLeadershipRequest request) {
    final MemoizedSupplier<PendingRequest> supplier = JavaUtils.memoize(() -> new PendingRequest(request));
    final PendingRequest previous = pending.getAndUpdate(f -> f != null? f: supplier.get());
    if (previous != null) {
      if (request.getNewLeader().equals(previous.getRequest().getNewLeader())) {
        final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
        previous.getReplyFuture().whenComplete((r, e) -> {
          if (e != null) {
            replyFuture.completeExceptionally(e);
          } else {
            replyFuture.complete(r.isSuccess()? server.newSuccessReply(request)
                : server.newExceptionReply(request, r.getException()));
          }
        });
        return replyFuture;
      } else {
        final TransferLeadershipException tle = new TransferLeadershipException(server.getMemberId() +
            "Failed to transfer leadership to " + request.getNewLeader() + ": a previous " + previous + " exists");
        return CompletableFuture.completedFuture(server.newExceptionReply(request, tle));
      }
    }

    scheduler.onTimeout(TimeDuration.valueOf(request.getTimeoutMs(), TimeUnit.MILLISECONDS),
        () -> finish(server.getState().getLeaderId(), true),
        LOG, () -> "Timeout check failed for append entry request: " + request);
    return supplier.get().getReplyFuture();
  }

  void finish(RaftPeerId currentLeader, boolean timeout) {
    Optional.ofNullable(pending.getAndSet(null))
        .ifPresent(r -> r.complete(currentLeader, timeout));
  }
}
