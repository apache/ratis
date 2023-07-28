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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class TransferLeadership {
  public static final Logger LOG = LoggerFactory.getLogger(TransferLeadership.class);

  private static class Context {
    private final TransferLeadershipRequest request;
    private final Supplier<LogAppender> transferee;

    Context(TransferLeadershipRequest request, Supplier<LogAppender> transferee) {
      this.request = request;
      this.transferee = transferee;
    }

    TransferLeadershipRequest getRequest() {
      return request;
    }

    RaftPeerId getTransfereeId() {
      return request.getNewLeader();
    }

    LogAppender getTransfereeLogAppender() {
      return transferee.get();
    }
  }

  static class Result {
    enum Type {
      SUCCESS,
      DIFFERENT_LEADER,
      NULL_FOLLOWER,
      NULL_LOG_APPENDER,
      NOT_UP_TO_DATE,
      TIMED_OUT,
      FAILED_TO_START,
      COMPLETED_EXCEPTIONALLY,
    }

    static final Result SUCCESS = new Result(Type.SUCCESS);
    static final Result DIFFERENT_LEADER = new Result(Type.DIFFERENT_LEADER);
    static final Result NULL_FOLLOWER = new Result(Type.NULL_FOLLOWER);
    static final Result NULL_LOG_APPENDER = new Result(Type.NULL_LOG_APPENDER);

    private final Type type;
    private final String errorMessage;
    private final Throwable exception;

    private Result(Type type) {
      this(type, null);
    }

    private Result(Type type, String errorMessage, Throwable exception) {
      this.type = type;
      this.errorMessage = errorMessage;
      this.exception = exception;
    }

    Result(Type type, String errorMessage) {
      this(type, errorMessage, null);
    }

    Result(Throwable t) {
      this(Type.COMPLETED_EXCEPTIONALLY, null, t);
    }

    Type getType() {
      return type;
    }

    @Override
    public String toString() {
      if (exception == null) {
        return type + (errorMessage == null ? "" : "(" + errorMessage + ")");
      }
      return type + ": " + StringUtils.stringifyException(exception);
    }
  }

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

    void complete(Result result) {
      if (replyFuture.isDone()) {
        return;
      }
      final RaftPeerId currentLeader = server.getState().getLeaderId();
      if (currentLeader != null && currentLeader.equals(request.getNewLeader())) {
        replyFuture.complete(server.newSuccessReply(request));
      } else {
        if (result.getType() == Result.Type.SUCCESS) {
          result = Result.DIFFERENT_LEADER;
        }
        final TransferLeadershipException tle = new TransferLeadershipException(server.getMemberId()
            + ": Failed to transfer leadership to " + request.getNewLeader()
            + " (the current leader is " + currentLeader + "): " + result);
        replyFuture.complete(server.newExceptionReply(request, tle));
      }
    }

    @Override
    public String toString() {
      return request.toString();
    }
  }

  private final RaftServerImpl server;
  private final TimeDuration requestTimeout;
  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();

  private final AtomicReference<PendingRequest> pending = new AtomicReference<>();

  TransferLeadership(RaftServerImpl server, RaftProperties properties) {
    this.server = server;
    this.requestTimeout = RaftServerConfigKeys.Rpc.requestTimeout(properties);
  }

  private Optional<RaftPeerId> getTransferee() {
    return Optional.ofNullable(pending.get())
        .map(r -> r.getRequest().getNewLeader());
  }

  boolean isSteppingDown() {
    return pending.get() != null;
  }

  static Result isFollowerUpToDate(FollowerInfo follower, TermIndex leaderLastEntry) {
    if (follower == null) {
      return Result.NULL_FOLLOWER;
    }
    if (leaderLastEntry == null) {
      // The transferee is expecting leaderLastEntry to be non-null,
      // return NOT_UP_TO_DATE to indicate TransferLeadership should wait.
      return new Result(Result.Type.NOT_UP_TO_DATE, "leaderLastEntry is null");
    }
    final long followerMatchIndex = follower.getMatchIndex();
    if (followerMatchIndex < leaderLastEntry.getIndex()) {
      return new Result(Result.Type.NOT_UP_TO_DATE, "followerMatchIndex = " + followerMatchIndex
          + " < leaderLastEntry.getIndex() = " + leaderLastEntry.getIndex());
    }
    return Result.SUCCESS;
  }

  private Result sendStartLeaderElection(FollowerInfo follower) {
    final TermIndex lastEntry = server.getState().getLastEntry();

    final Result result = isFollowerUpToDate(follower, lastEntry);
    if (result != Result.SUCCESS) {
      return result;
    }

    final RaftPeerId transferee = follower.getId();
    LOG.info("{}: sendStartLeaderElection to follower {}, lastEntry={}",
        server.getMemberId(), transferee, lastEntry);

    final RaftProtos.StartLeaderElectionRequestProto r = ServerProtoUtils.toStartLeaderElectionRequestProto(
        server.getMemberId(), transferee, lastEntry);
    final CompletableFuture<RaftProtos.StartLeaderElectionReplyProto> f = CompletableFuture.supplyAsync(() -> {
      server.getLeaderElectionMetrics().onTransferLeadership();
      try {
        return server.getServerRpc().startLeaderElection(r);
      } catch (IOException e) {
        throw new CompletionException("Failed to sendStartLeaderElection to follower " + transferee, e);
      }
    }, server.getServerExecutor()).whenComplete((reply, exception) -> {
      if (reply != null) {
        LOG.info("{}: Received startLeaderElection reply from {}: success? {}",
            server.getMemberId(), transferee, reply.getServerReply().getSuccess());
      } else if (exception != null) {
        LOG.warn(server.getMemberId() + ": Failed to startLeaderElection for " + transferee, exception);
      }
    });

    if (f.isCompletedExceptionally()) { // already failed
      try {
        f.join();
      } catch (Throwable t) {
        return new Result(t);
      }
    }
    return Result.SUCCESS;
  }

  /**
   * If the transferee has just append some entries and becomes up-to-date,
   * send StartLeaderElection to it
   */
  void onFollowerAppendEntriesReply(FollowerInfo follower) {
    if (!getTransferee().filter(t -> t.equals(follower.getId())).isPresent()) {
      return;
    }
    final Result result = sendStartLeaderElection(follower);
    if (result == Result.SUCCESS) {
      LOG.info("{}: sent StartLeaderElection to transferee {} after received AppendEntriesResponse",
          server.getMemberId(), follower.getId());
    }
  }

  private Result tryTransferLeadership(Context context) {
    final RaftPeerId transferee = context.getTransfereeId();
    LOG.info("{}: start transferring leadership to {}", server.getMemberId(), transferee);
    final LogAppender appender = context.getTransfereeLogAppender();
    if (appender == null) {
      return Result.NULL_LOG_APPENDER;
    }
    final FollowerInfo follower = appender.getFollower();
    final Result result = sendStartLeaderElection(follower);
    if (result.getType() == Result.Type.SUCCESS) {
      LOG.info("{}: {} sent StartLeaderElection to transferee {} immediately as it already has up-to-date log",
          server.getMemberId(), result, transferee);
    } else if (result.getType() == Result.Type.NOT_UP_TO_DATE) {
      LOG.info("{}: {} notifying LogAppender to send AppendEntries to transferee {}",
          server.getMemberId(), result, transferee);
      appender.notifyLogAppender();
    }
    return result;
  }

  void start(LogAppender transferee) {
    // TransferLeadership will block client request, so we don't want wait too long.
    // If everything goes well, transferee should be elected within the min rpc timeout.
    final long timeout = server.properties().minRpcTimeoutMs();
    final TransferLeadershipRequest request = new TransferLeadershipRequest(ClientId.emptyClientId(),
        server.getId(), server.getMemberId().getGroupId(), 0, transferee.getFollowerId(), timeout);
    start(new Context(request, () -> transferee));
  }

  CompletableFuture<RaftClientReply> start(LeaderStateImpl leaderState, TransferLeadershipRequest request) {
    final Context context = new Context(request,
        JavaUtils.memoize(() -> leaderState.getLogAppender(request.getNewLeader()).orElse(null)));
    return start(context);
  }

  private CompletableFuture<RaftClientReply> start(Context context) {
    final TransferLeadershipRequest request = context.getRequest();
    final MemoizedSupplier<PendingRequest> supplier = JavaUtils.memoize(() -> new PendingRequest(request));
    final PendingRequest previous = pending.getAndUpdate(f -> f != null? f: supplier.get());
    if (previous != null) {
      return createReplyFutureFromPreviousRequest(request, previous);
    }
    final PendingRequest pendingRequest = supplier.get();
    final Result result = tryTransferLeadership(context);
    final Result.Type type = result.getType();
    if (type != Result.Type.SUCCESS && type != Result.Type.NOT_UP_TO_DATE) {
      pendingRequest.complete(result);
    } else {
      // if timeout is not specified in request, use default request timeout
      final TimeDuration timeout = request.getTimeoutMs() == 0 ? requestTimeout
          : TimeDuration.valueOf(request.getTimeoutMs(), TimeUnit.MILLISECONDS);
      scheduler.onTimeout(timeout, () -> complete(new Result(Result.Type.TIMED_OUT,
              timeout.toString(TimeUnit.SECONDS, 3))),
          LOG, () -> "Failed to handle timeout");
    }
    return pendingRequest.getReplyFuture();
  }

  private CompletableFuture<RaftClientReply> createReplyFutureFromPreviousRequest(
      TransferLeadershipRequest request, PendingRequest previous) {
    if (request.getNewLeader().equals(previous.getRequest().getNewLeader())) {
      final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
      previous.getReplyFuture().whenComplete((r, e) -> {
        if (e != null) {
          replyFuture.completeExceptionally(e);
        } else {
          replyFuture.complete(r.isSuccess() ? server.newSuccessReply(request)
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

  void complete(Result result) {
    Optional.ofNullable(pending.getAndSet(null))
        .ifPresent(r -> r.complete(result));
  }
}
