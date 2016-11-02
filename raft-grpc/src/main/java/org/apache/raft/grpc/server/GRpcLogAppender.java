/**
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
package org.apache.raft.grpc.server;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.util.Time;
import org.apache.raft.grpc.RaftGRpcService;
import org.apache.raft.grpc.RaftGrpcConfigKeys;
import org.apache.raft.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.raft.server.FollowerInfo;
import org.apache.raft.server.LeaderState;
import org.apache.raft.server.LogAppender;
import org.apache.raft.server.RaftServer;
import org.apache.raft.statemachine.SnapshotInfo;
import org.apache.raft.util.CodeInjectionForTesting;

import java.io.InterruptedIOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.raft.grpc.RaftGRpcService.GRPC_SEND_SERVER_REQUEST;

/**
 * A new log appender implementation using grpc bi-directional stream API.
 */
public class GRpcLogAppender extends LogAppender {
  private final RaftServerProtocolClient client;
  private final Queue<AppendEntriesRequestProto> pendingRequests;
  private final int maxPendingRequestsNum;
  private volatile boolean firstResponseReceived = false;

  public GRpcLogAppender(RaftServer server, LeaderState leaderState,
      FollowerInfo f) {
    super(server, leaderState, f);

    RaftGRpcService rpcService = (RaftGRpcService) server.getServerRpc();
    client = rpcService.getRpcClient(f.getPeer());
    maxPendingRequestsNum = server.getProperties().getInt(
        RaftGrpcConfigKeys.RAFT_GRPC_LEADER_MAX_OUTSTANDING_APPENDS_KEY,
        RaftGrpcConfigKeys.RAFT_GRPC_LEADER_MAX_OUTSTANDING_APPENDS_DEFAULT);
    pendingRequests = new LinkedList<>();
  }

  @Override
  public void run() {
    final StreamObserver<AppendEntriesRequestProto> appendLogRequestObserver =
        client.appendEntries(new AppendLogResponseHandler());
    final InstallSnapshotResponseHandler snapshotResponseHandler =
        new InstallSnapshotResponseHandler();
    final StreamObserver<InstallSnapshotRequestProto> snapshotRequestObserver =
        client.installSnapshot(snapshotResponseHandler);
    while (isAppenderRunning()) {
      // TODO restart a stream RPC if necessary
      if (shouldSendRequest()) {
        SnapshotInfo snapshot = shouldInstallSnapshot();
        if (snapshot != null) {
          installSnapshot(snapshot, snapshotResponseHandler,
              snapshotRequestObserver);
        } else {
          // keep appending log entries or sending heartbeats
          appendLog(appendLogRequestObserver);
        }
      }

      if (isAppenderRunning() && !shouldSendRequest()) {
        // use lastSend time instead of lastResponse time
        final long waitTime = getHeartbeatRemainingTime(
            follower.getLastRpcTime());
        if (waitTime > 0) {
          synchronized (this) {
            try {
              LOG.debug("{} decides to wait {}ms before appending to {}",
                  server.getId(), waitTime, follower.getPeer().getId());
              wait(waitTime);
            } catch (InterruptedException ignored) {
            }
          }
        }
      }
    }
    appendLogRequestObserver.onCompleted();
  }

  private boolean shouldWait() {
    return pendingRequests.size() >= maxPendingRequestsNum ||
        shouldWaitForFirstResponse();
  }

  private void appendLog(
      StreamObserver<AppendEntriesRequestProto> requestObserver) {
    try {
      AppendEntriesRequestProto pending = null;
      // if the queue's size >= maxSize, wait
      synchronized (this) {
        while (isAppenderRunning() && shouldWait()) {
          try {
            LOG.debug("{} wait to send the next AppendEntries to {}",
                server.getId(), follower.getPeer().getId());
            this.wait();
          } catch (InterruptedException ignored) {
          }
        }

        if (isAppenderRunning()) {
          // prepare and enqueue the append request. note changes on follower's
          // nextIndex and ops on pendingRequests should always be associated
          // together and protected by the lock
          pending = createRequest();
          if (pending != null) {
            Preconditions.checkState(pendingRequests.offer(pending));
            updateNextIndex(pending);
          }
        }
      }

      if (pending != null && isAppenderRunning()) {
        sendRequest(pending, requestObserver);
      }
    } catch (RuntimeException e) {
      // TODO we can cancel the original RPC and restart it. In this way we can
      // cancel all the pending requests in the channel
      LOG.info(this + "got exception when appending log to " + follower, e);
    }
  }

  private void sendRequest(AppendEntriesRequestProto request,
      StreamObserver<AppendEntriesRequestProto> requestObserver) {
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, server.getId(),
        null, request);

    requestObserver.onNext(request);
    follower.updateLastRpcSendTime(Time.monotonicNow());
  }

  private void updateNextIndex(AppendEntriesRequestProto request) {
    final int count = request.getEntriesCount();
    if (count > 0) {
      follower.updateNextIndex(request.getEntries(count - 1).getIndex() + 1);
    }
  }

  /**
   * if this is the first append, wait for the response of the first append so
   * that we can get the correct next index.
   */
  private boolean shouldWaitForFirstResponse() {
    return pendingRequests.size() > 0 && !firstResponseReceived;
  }

  /**
   * StreamObserver for handling responses from the follower
   */
  private class AppendLogResponseHandler
      implements StreamObserver<AppendEntriesReplyProto> {
    /**
     * After receiving a appendEntries reply, do the following:
     * 1. If the reply is success, update the follower's match index and submit
     *    an event to leaderState
     * 2. If the reply is NOT_LEADER, step down
     * 3. If the reply is INCONSISTENCY, decrease the follower's next index
     *    based on the response
     */
    @Override
    public void onNext(AppendEntriesReplyProto reply) {
      LOG.debug("{} received {} response from {}", server.getId(),
          (!firstResponseReceived ? "the first" : "a"),
          follower.getPeer().getId());

      // update the last rpc time
      follower.updateLastRpcResponseTime(Time.monotonicNow());

      if (!firstResponseReceived) {
        firstResponseReceived = true;
      }
      switch (reply.getResult()) {
        case SUCCESS:
          onSuccess(reply);
          break;
        case NOT_LEADER:
          onNotLeader(reply);
          break;
        case INCONSISTENCY:
          onInconsistency(reply);
          break;
        default:
          break;
      }
      notifyAppend();
    }

    /**
     * for now we simply retry the first pending request
     */
    @Override
    public void onError(Throwable t) {
      LOG.info("{} got error when appending entries to {}, exception: {}",
          server.getId(), follower.getPeer().getId(), t);
      // clear the pending requests queue and reset the next index of follower
      // TODO reuse the requests
      AppendEntriesRequestProto request = pendingRequests.peek();
      if (request != null) {
        final long nextIndex = request.hasPreviousLog() ?
            request.getPreviousLog().getIndex() + 1 : raftLog.getStartIndex();
        clearPendingRequests(nextIndex);
      }
    }

    @Override
    public void onCompleted() {
      LOG.info("{} stops appending log entries to follower {}", server.getId(),
          follower);
    }
  }

  private synchronized void clearPendingRequests(long newNextIndex) {
    pendingRequests.clear();
    follower.decreaseNextIndex(newNextIndex);
  }

  private void onSuccess(AppendEntriesReplyProto reply) {
    AppendEntriesRequestProto request = pendingRequests.poll();
    final long replyNextIndex = reply.getNextIndex();
    Preconditions.checkNotNull(request,
        "Got reply with next index %s but the pending queue is empty",
        replyNextIndex);

    if (request.getEntriesCount() == 0) {
      Preconditions.checkState(!request.hasPreviousLog() ||
              replyNextIndex - 1 == request.getPreviousLog().getIndex(),
          "reply's next index is %s, request's previous is %s",
          replyNextIndex, request.getPreviousLog());
    } else {
      // check if the reply and the pending request is consistent
      final long lastEntryIndex = request
          .getEntries(request.getEntriesCount() - 1).getIndex();
      Preconditions.checkState(replyNextIndex == lastEntryIndex + 1,
          "reply's next index is %s, request's last entry index is %s",
          replyNextIndex, lastEntryIndex);
      follower.updateMatchIndex(lastEntryIndex);
      submitEventOnSuccessAppend();
    }
  }

  private void onNotLeader(AppendEntriesReplyProto reply) {
    checkResponseTerm(reply.getTerm());
    // TODO cancel the RPC
  }

  private void onInconsistency(AppendEntriesReplyProto reply) {
    AppendEntriesRequestProto request = pendingRequests.peek();
    Preconditions.checkState(request.hasPreviousLog());
    if (request.getPreviousLog().getIndex() >= reply.getNextIndex()) {
      clearPendingRequests(reply.getNextIndex());
    }
    // TODO cancel the rpc call and restart it, so as not to send in-q requests
  }

  private class InstallSnapshotResponseHandler
      implements StreamObserver<InstallSnapshotReplyProto> {
    private final Queue<Integer> pending;
    private final AtomicBoolean done = new AtomicBoolean(false);

    InstallSnapshotResponseHandler() {
      pending = new LinkedList<>();
    }

    synchronized void addPending(InstallSnapshotRequestProto request) {
      pending.offer(request.getRequestIndex());
    }

    synchronized void removePending(InstallSnapshotReplyProto reply) {
      int index = pending.poll();
      Preconditions.checkState(index == reply.getRequestIndex());
    }

    boolean isDone() {
      return done.get();
    }

    void close() {
      done.set(true);
      GRpcLogAppender.this.notifyAppend();
    }

    synchronized boolean hasAllResponse() {
      return pending.isEmpty();
    }

    @Override
    public void onNext(InstallSnapshotReplyProto reply) {
      LOG.debug("{} received {} response from {}", server.getId(),
          (!firstResponseReceived ? "the first" : "a"),
          follower.getPeer().getId());

      // update the last rpc time
      follower.updateLastRpcResponseTime(Time.monotonicNow());

      if (!firstResponseReceived) {
        firstResponseReceived = true;
      }

      switch (reply.getResult()) {
        case SUCCESS:
          removePending(reply);
          break;
        case NOT_LEADER:
          checkResponseTerm(reply.getTerm());
          break;
        case UNRECOGNIZED:
          break;
      }
    }

    @Override
    public void onError(Throwable t) {
      LOG.info("{} got error when installing snapshot to {}, exception: {}",
          server.getId(), follower.getPeer().getId(), t);
      close();
    }

    @Override
    public void onCompleted() {
      LOG.info("{} stops sending snapshots to follower {}", server.getId(),
          follower);
      close();
    }
  }

  private void installSnapshot(SnapshotInfo snapshot,
      InstallSnapshotResponseHandler responseHandler,
      StreamObserver<InstallSnapshotRequestProto> requestHandler) {
    LOG.info("{}: follower {}'s next index is {}," +
            " log's start index is {}, need to install snapshot",
        server.getId(), follower.getPeer(), follower.getNextIndex(),
        raftLog.getStartIndex());

    final String requestId = UUID.randomUUID().toString();
    try {
      for (InstallSnapshotRequestProto request :
          new SnapshotRequestIter(snapshot, requestId)) {
        if (isAppenderRunning()) {
          requestHandler.onNext(request);
          follower.updateLastRpcSendTime(Time.monotonicNow());
          responseHandler.addPending(request);
        }
      }
      requestHandler.onCompleted();
    } catch (InterruptedIOException iioe) {
      LOG.info(this + " was interrupted: " + iioe);
      return;
    } catch (Exception ioe) {
      LOG.warn(this + ": failed to install SnapshotInfo " + snapshot.getFiles(),
          ioe);
      return;
    }

    synchronized (this) {
      while (isAppenderRunning() && !responseHandler.isDone()) {
        try {
          wait();
        } catch (InterruptedException ignored) {
        }
      }
    }

    if (responseHandler.hasAllResponse()) {
      follower.updateMatchIndex(snapshot.getTermIndex().getIndex());
      follower.updateNextIndex(snapshot.getTermIndex().getIndex() + 1);
      LOG.info("{}: install snapshot-{} successfully on follower {}",
          server.getId(), snapshot.getTermIndex().getIndex(), follower.getPeer());
    }
  }
}
