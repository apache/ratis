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

import org.apache.ratis.protocol.PauseLeaderElectionRequest;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
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
import java.util.concurrent.atomic.AtomicReference;

public class PauseResumeLeaderElection {
  public static final Logger LOG = LoggerFactory.getLogger(TransferLeadership.class);

  static class PendingRequest {
    private final PauseLeaderElectionRequest request;
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();

    PendingRequest(PauseLeaderElectionRequest request) {
      LOG.info("New pause leader election on server {}", request.getPausedServer());
      this.request = request;
    }
    PauseLeaderElectionRequest getRequest() {
      return request;
    }
    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    void complete(RaftServerImpl impl, boolean timeout) {
      if (replyFuture.isDone()) {
        return;
      }
      boolean bool = request.getPause();
      if (bool == impl.getRole().getLeaderElectionPauseState()) {
        replyFuture.complete(impl.newSuccessReply(request, request.getPausedServer()));
      } else if (timeout) {
        replyFuture.completeExceptionally(new TimeoutIOException(
            "Failed to pause leader election on server" + request.getPausedServer()
            + " within timeout " + request.getTimeoutMs() + "ms"));
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

  PauseResumeLeaderElection(RaftServerImpl server) {
    this.server = server;
  }

  boolean isSteppingDown() {
    return pending.get() != null;
  }

  CompletableFuture<RaftClientReply> pauseLeaderElectionAsync(PauseLeaderElectionRequest request) {
    final MemoizedSupplier<PendingRequest> supplier = JavaUtils.memoize(() -> new PendingRequest(
        request));
    final PendingRequest previous = pending.getAndUpdate(f -> f != null? f: supplier.get());
    if (previous != null) {
       if (request.getPausedServer().equals(previous.getRequest().getPausedServer())) {
         final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
         previous.getReplyFuture().whenComplete((r, e) -> {
           if (e != null) {
             replyFuture.completeExceptionally(e);
           } else {
             replyFuture.complete(r.isSuccess()? server.newSuccessReply(request, request.getPausedServer())
                 : server.newExceptionReply(request, r.getException()));
           }
         });
         return replyFuture;
       } else {
         String msg = String.format("Failed to pause leader election on server %s, "
             + "a previous reuqest exists", request.getPausedServer());
         return CompletableFuture.completedFuture(
             server.newExceptionReply(request, new RaftException(msg)));
       }
    }
    try {
      server.setLeaderElectionPause(request.getPause());
    } catch (ServerNotReadyException e) {
      LOG.info("Failed to pause leader election on {} , the server is not ready", server.getId());
      e.printStackTrace();
    }
    scheduler.onTimeout(TimeDuration.valueOf(request.getTimeoutMs(), TimeUnit.MILLISECONDS),
        () -> finish(server, true),
        LOG, () -> "Timeout check failed for pause leader election: " + request);
    return supplier.get().getReplyFuture();
  }
  void finish(RaftServerImpl impl, boolean timeout) {
    Optional.ofNullable(pending.getAndSet(null))
        .ifPresent(r -> r.complete(impl, timeout));
  }
}
