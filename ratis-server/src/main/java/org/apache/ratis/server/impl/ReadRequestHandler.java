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

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.OrderedAsync;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.ReadIndexReplyProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadRequestTypeProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.protocol.exceptions.StaleReadException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.ratis.server.impl.ServerProtoUtils.toReadIndexReplyProto;
import static org.apache.ratis.server.impl.ServerProtoUtils.toReadIndexRequestProto;

/** Handle read requests, including stale reads, linearizable reads and readIndex, for a {@link RaftServerImpl}. */
class ReadRequestHandler {
  static final Logger LOG = LoggerFactory.getLogger(ReadRequestHandler.class);

  private final RaftServerImpl server;
  private final ServerState state;
  private final StateMachine stateMachine;
  private final RaftServerConfigKeys.Read.Option readOption;

  private final CompletableFuture<RaftClientReply> dummySuccessReply;

  ReadRequestHandler(RaftServerImpl server, RaftProperties properties) {
    this.server = server;
    this.state = server.getState();
    this.stateMachine = server.getStateMachine();
    this.readOption = RaftServerConfigKeys.Read.option(properties);

    this.dummySuccessReply = CompletableFuture.completedFuture(RaftClientReply.newBuilder()
        .setClientId(ClientId.emptyClientId())
        .setServerId(server.getId())
        .setGroupId(getMemberId().getGroupId())
        .setSuccess()
        .build());
  }

  RaftGroupMemberId getMemberId() {
    return state.getMemberId();
  }

  CompletableFuture<RaftClientReply> staleReadAsync(RaftClientRequest request) {
    final long minIndex = request.getType().getStaleRead().getMinIndex();
    final long commitIndex = state.getLog().getLastCommittedIndex();
    LOG.debug("{}: minIndex={}, commitIndex={} from {}", getMemberId(), minIndex, commitIndex, request.getClientId());
    if (commitIndex < minIndex) {
      final StaleReadException e = new StaleReadException(
          "Unable to serve stale-read due to server commit index = " + commitIndex + " < min = " + minIndex);
      return CompletableFuture.completedFuture(
          server.newExceptionReply(request, new StateMachineException(getMemberId(), e)));
    }
    return processQueryFuture(stateMachine.queryStale(request.getMessage(), minIndex), request);
  }

  ReadException getReadException(String op, long installSnapshot, boolean started) {
    return installSnapshot == RaftLog.INVALID_LOG_INDEX ? null : new ReadException(getMemberId() + ": Failed to " + op
        + " readIndex as snapshot (" + installSnapshot + ") installation is " + (started ? "started" : "in progress"));
  }

  private ReadException getReadExceptionIfInstallingSnapshot() {
    return getReadException("add", server.getSnapshotInstallationHandler().getInProgressInstallSnapshotIndex(), false);
  }

  private CompletableFuture<ReadIndexReplyProto> sendReadIndexAsync(
      ClientId clientId, ReadRequestTypeProto readRequestType) {
    final long installSnapshot = server.getSnapshotInstallationHandler().getInProgressInstallSnapshotIndex();
    if (installSnapshot != RaftLog.INVALID_LOG_INDEX) {
      return JavaUtils.completeExceptionally(getReadException("get", installSnapshot, false));
    }
    final RaftPeerId leaderId = server.getInfo().getLeaderId();
    if (leaderId == null) {
      return JavaUtils.completeExceptionally(new ReadIndexException(getMemberId() + ": Leader is unknown."));
    }
    final ReadIndexRequestProto request = toReadIndexRequestProto(clientId, readRequestType, getMemberId(), leaderId);
    try {
      return server.getServerRpc().async().readIndexAsync(request);
    } catch (IOException e) {
      return JavaUtils.completeExceptionally(e);
    }
  }

  private CompletableFuture<Long> getReadIndex(
      ClientId clientId, ReadRequestTypeProto readRequestType, LeaderStateImpl leader) {
    if (leader != null) {
      if (readRequestType.getStrongReadAfterWrite()) {
        final long latestWriteIndex = server.getWriteIndexCache().getLatestWriteIndex();
        return leader.getReadIndex(latestWriteIndex);
      }
      return server.getWriteIndexCache().getWriteIndexFuture(clientId, readRequestType)
          .thenCompose(leader::getReadIndex);
    }
    return sendReadIndexAsync(clientId, readRequestType).thenApply(reply -> {
      if (reply.getServerReply().getSuccess()) {
        return reply.getReadIndex();
      } else {
        throw new CompletionException(new ReadIndexException(server.getId()
                + ": Failed to get read index from the leader: " + reply));
      }
    });
  }

  CompletableFuture<Message> readOnlyAsync(ClientId clientId, ReadRequestTypeProto readRequestType,
      Message queryMessage) throws IOException {
    Objects.requireNonNull(clientId, "clientId == null");
    Objects.requireNonNull(readRequestType, "readRequestType == null");
    Objects.requireNonNull(queryMessage, "queryMessage == null");
    server.assertLifeCycleState(LifeCycle.States.RUNNING);
    if (readRequestType.getPreferNonLinearizable() || readOption == RaftServerConfigKeys.Read.Option.DEFAULT) {
      if (!server.getInfo().isLeader()) {
        return JavaUtils.completeExceptionally(server.newNotLeaderException());
      }
      if (!server.getInfo().isLeaderReady()) {
        return JavaUtils.completeExceptionally(new LeaderNotReadyException(getMemberId()));
      }
      return stateMachine.query(queryMessage);
    } else if (readOption == RaftServerConfigKeys.Read.Option.LINEARIZABLE) {
      final LeaderStateImpl leader = server.getRole().getLeaderState().orElse(null);
      return getReadIndex(clientId, readRequestType, leader)
          .thenCompose(readIndex -> state.getReadRequests().waitToAdvance(readIndex,
              this::getReadExceptionIfInstallingSnapshot))
          .thenCompose(readIndex -> stateMachine.query(queryMessage));
    } else {
      throw new IllegalStateException("Unexpected read option: " + readOption);
    }
  }

  CompletableFuture<RaftClientReply> readAsync(RaftClientRequest request) {
    if (request.getType().getRead().getPreferNonLinearizable()
        || readOption == RaftServerConfigKeys.Read.Option.DEFAULT) {
      final CompletableFuture<RaftClientReply> reply = server.checkLeaderState(request);
      if (reply != null) {
        return reply;
      }
      return isDummyRead(request) ? CompletableFuture.completedFuture(server.newSuccessReply(request))
          : queryStateMachine(request);
    } else if (readOption == RaftServerConfigKeys.Read.Option.LINEARIZABLE) {
      final LeaderStateImpl leader = server.getRole().getLeaderState().orElse(null);
      final CompletableFuture<Long> replyFuture;
      replyFuture = getReadIndex(request.getClientId(), request.getType().getRead(), leader);

      return replyFuture
          .thenCompose(readIndex -> state.getReadRequests().waitToAdvance(readIndex,
              this::getReadExceptionIfInstallingSnapshot))
          .thenCompose(readIndex -> isDummyRead(request)
              ? CompletableFuture.completedFuture(server.newSuccessReply(request)) : queryStateMachine(request))
          .exceptionally(e -> readException2Reply(request, e));
    } else {
      throw new IllegalStateException("Unexpected read option: " + readOption);
    }
  }

  static boolean isDummyRead(RaftClientRequest request) {
    return request.getMessage() != null && OrderedAsync.DUMMY.getContent().equals(request.getMessage().getContent());
  }

  private RaftClientReply readException2Reply(RaftClientRequest request, Throwable e) {
    e = JavaUtils.unwrapCompletionException(e);
    if (e instanceof StateMachineException ) {
      return server.newExceptionReply(request, (StateMachineException) e);
    } else if (e instanceof ReadException) {
      return server.newExceptionReply(request, (ReadException) e);
    } else if (e instanceof ReadIndexException) {
      return server.newExceptionReply(request, (ReadIndexException) e);
    } else {
      throw new CompletionException(e);
    }
  }

  private CompletableFuture<RaftClientReply> queryStateMachine(RaftClientRequest request) {
    if (request.getType().getRead().getDummy()) {
      return dummySuccessReply;
    }
    return processQueryFuture(stateMachine.query(request.getMessage()), request);
  }

  private CompletableFuture<RaftClientReply> processQueryFuture(
      CompletableFuture<Message> queryFuture, RaftClientRequest request) {
    return queryFuture.thenApply(r -> server.newReplyBuilder(request).setSuccess().setMessage(r).build())
        .exceptionally(e -> {
          e = JavaUtils.unwrapCompletionException(e);
          if (e instanceof StateMachineException) {
            return server.newExceptionReply(request, (StateMachineException)e);
          }
          throw new CompletionException(e);
        });
  }

  CompletableFuture<ReadIndexReplyProto> readIndexAsync(ReadIndexRequestProto request) throws IOException {
    server.assertLifeCycleState(LifeCycle.States.RUNNING);

    final RaftPeerId peerId = RaftPeerId.valueOf(request.getServerRequest().getRequestorId());

    final LeaderStateImpl leader = server.getRole().getLeaderState().orElse(null);
    if (leader == null) {
      return CompletableFuture.completedFuture(toReadIndexReplyProto(peerId, getMemberId()));
    }

    final RaftClientRequest clientRequest = ClientProtoUtils.toRaftClientRequest(request.getClientRequest());
    return getReadIndex(clientRequest.getClientId(), clientRequest.getType().getRead(), leader)
        .thenApply(index -> toReadIndexReplyProto(peerId, getMemberId(), true, index))
        .whenComplete((reply, exception) -> {
          if (exception == null) {
            // Leader should try to trigger heartbeat immediately after leader replies the ReadIndex to the follower
            // so that the follower's commitIndex can be updated to the leader's commitIndex and the follower
            // can start applying the logs up until the leader's commitIndex (instead of waiting for the next
            // AppendEntries to happen through heartbeat or new transactions (which might increase the latency
            // considerably)).
            // Note that if the follower commitIndex is already equal to the leader's commitIndex, no heartbeat
            // will be triggered, see GrpcLogAppender#isFollowerCommitBehindLastCommitIndex.
            RaftPeerId requestorId = RaftPeerId.valueOf(reply.getServerReply().getRequestorId());
            leader.getLogAppender(requestorId).ifPresent(LogAppender::triggerHeartbeat);
          }
        })
        .exceptionally(throwable -> toReadIndexReplyProto(peerId, getMemberId()));
  }
}
