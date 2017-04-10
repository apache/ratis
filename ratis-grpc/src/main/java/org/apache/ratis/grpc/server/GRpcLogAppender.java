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
package org.apache.ratis.grpc.server;

import org.apache.ratis.shaded.io.grpc.Status;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.grpc.RaftGRpcService;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.server.impl.FollowerInfo;
import org.apache.ratis.server.impl.LeaderState;
import org.apache.ratis.server.impl.LogAppender;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.Preconditions;

import static org.apache.ratis.grpc.RaftGRpcService.GRPC_SEND_SERVER_REQUEST;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A new log appender implementation using grpc bi-directional stream API.
 */
public class GRpcLogAppender extends LogAppender {
  private final RaftServerProtocolClient client;
  private final Queue<AppendEntriesRequestProto> pendingRequests;
  private final int maxPendingRequestsNum;
  private volatile boolean firstResponseReceived = false;

  private final AppendLogResponseHandler appendResponseHandler;
  private final InstallSnapshotResponseHandler snapshotResponseHandler;

  private volatile StreamObserver<AppendEntriesRequestProto> appendLogRequestObserver;
  private StreamObserver<InstallSnapshotRequestProto> snapshotRequestObserver;

  public GRpcLogAppender(RaftServerImpl server, LeaderState leaderState,
                         FollowerInfo f) {
    super(server, leaderState, f);

    RaftGRpcService rpcService = (RaftGRpcService) server.getServerRpc();
    client = rpcService.getRpcClient(f.getPeer());
    maxPendingRequestsNum = GrpcConfigKeys.Server.leaderOutstandingAppendsMax(server.getProperties());
    pendingRequests = new ConcurrentLinkedQueue<>();

    appendResponseHandler = new AppendLogResponseHandler();
    snapshotResponseHandler = new InstallSnapshotResponseHandler();
  }

  @Override
  public void run() {
    while (isAppenderRunning()) {
      if (shouldSendRequest()) {
        SnapshotInfo snapshot = shouldInstallSnapshot();
        if (snapshot != null) {
          installSnapshot(snapshot, snapshotResponseHandler);
        } else {
          // keep appending log entries or sending heartbeats
          appendLog();
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
                  server.getId(), waitTime, follower.getPeer());
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

  private void appendLog() {
    if (appendLogRequestObserver == null) {
      appendLogRequestObserver = client.appendEntries(appendResponseHandler);
    }
    AppendEntriesRequestProto pending = null;
    final StreamObserver<AppendEntriesRequestProto> s;
    synchronized (this) {
      // if the queue's size >= maxSize, wait
      while (isAppenderRunning() && shouldWait()) {
        try {
          LOG.debug("{} wait to send the next AppendEntries to {}",
              server.getId(), follower.getPeer());
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
          Preconditions.assertTrue(pendingRequests.offer(pending));
          updateNextIndex(pending);
        }
      }
      s = appendLogRequestObserver;
    }

    if (pending != null && isAppenderRunning()) {
      sendRequest(pending, s);
    }
  }

  private void sendRequest(AppendEntriesRequestProto request,
      StreamObserver<AppendEntriesRequestProto> s) {
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, server.getId(),
        null, request);

    s.onNext(request);
    follower.updateLastRpcSendTime();
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
          follower.getPeer());

      // update the last rpc time
      follower.updateLastRpcResponseTime();

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
      if (!isAppenderRunning()) {
        LOG.info("{} is stopped", GRpcLogAppender.this);
        return;
      }
      LOG.warn("{} got error when appending entries to {}, exception: {}.",
          server.getId(), follower.getPeer().getId(), t);

      synchronized (this) {
        final Status cause = Status.fromThrowable(t);
        if (cause != null && cause.getCode() == Status.Code.INTERNAL) {
          // TODO check other Status. Add sleep to avoid tight loop
          LOG.debug("{} restarts Append call to {} due to error {}",
              server.getId(), follower.getPeer(), t);
          // recreate the StreamObserver
          appendLogRequestObserver = client.appendEntries(appendResponseHandler);
          // reset firstResponseReceived to false
          firstResponseReceived = false;
        }

        // clear the pending requests queue and reset the next index of follower
        AppendEntriesRequestProto request = pendingRequests.peek();
        if (request != null) {
          final long nextIndex = request.hasPreviousLog() ?
              request.getPreviousLog().getIndex() + 1 : raftLog.getStartIndex();
          clearPendingRequests(nextIndex);
        }
      }
    }

    @Override
    public void onCompleted() {
      LOG.info("{} stops appending log entries to follower {}", server.getId(),
          follower);
    }
  }

  private void clearPendingRequests(long newNextIndex) {
    pendingRequests.clear();
    follower.decreaseNextIndex(newNextIndex);
  }

  private void onSuccess(AppendEntriesReplyProto reply) {
    AppendEntriesRequestProto request = pendingRequests.poll();
    final long replyNextIndex = reply.getNextIndex();
    Objects.requireNonNull(request,
        () -> "Got reply with next index " + replyNextIndex
            + " but the pending queue is empty");

    if (request.getEntriesCount() == 0) {
      Preconditions.assertTrue(!request.hasPreviousLog() ||
              replyNextIndex - 1 == request.getPreviousLog().getIndex(),
          "reply's next index is %s, request's previous is %s",
          replyNextIndex, request.getPreviousLog());
    } else {
      // check if the reply and the pending request is consistent
      final long lastEntryIndex = request
          .getEntries(request.getEntriesCount() - 1).getIndex();
      Preconditions.assertTrue(replyNextIndex == lastEntryIndex + 1,
          "reply's next index is %s, request's last entry index is %s",
          replyNextIndex, lastEntryIndex);
      follower.updateMatchIndex(lastEntryIndex);
      submitEventOnSuccessAppend();
    }
  }

  private void onNotLeader(AppendEntriesReplyProto reply) {
    checkResponseTerm(reply.getTerm());
    // the running loop will end and the connection will onComplete
  }

  private synchronized void onInconsistency(AppendEntriesReplyProto reply) {
    AppendEntriesRequestProto request = pendingRequests.peek();
    Preconditions.assertTrue(request.hasPreviousLog());
    if (request.getPreviousLog().getIndex() >= reply.getNextIndex()) {
      clearPendingRequests(reply.getNextIndex());
    }
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
      Preconditions.assertTrue(index == reply.getRequestIndex());
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
          follower.getPeer());

      // update the last rpc time
      follower.updateLastRpcResponseTime();

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
      if (!isAppenderRunning()) {
        LOG.info("{} is stopped", GRpcLogAppender.this);
        return;
      }
      LOG.info("{} got error when installing snapshot to {}, exception: {}",
          server.getId(), follower.getPeer(), t);
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
      InstallSnapshotResponseHandler responseHandler) {
    LOG.info("{}: follower {}'s next index is {}," +
            " log's start index is {}, need to install snapshot",
        server.getId(), follower.getPeer(), follower.getNextIndex(),
        raftLog.getStartIndex());

    snapshotRequestObserver = client.installSnapshot(snapshotResponseHandler);
    final String requestId = UUID.randomUUID().toString();
    try {
      for (InstallSnapshotRequestProto request :
          new SnapshotRequestIter(snapshot, requestId)) {
        if (isAppenderRunning()) {
          snapshotRequestObserver.onNext(request);
          follower.updateLastRpcSendTime();
          responseHandler.addPending(request);
        } else {
          break;
        }
      }
      snapshotRequestObserver.onCompleted();
    } catch (Exception e) {
      LOG.warn("{} failed to install snapshot {}. Exception: {}", this,
          snapshot.getFiles(), e);
      snapshotRequestObserver.onError(e);
      return;
    } finally {
      snapshotRequestObserver = null;
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
