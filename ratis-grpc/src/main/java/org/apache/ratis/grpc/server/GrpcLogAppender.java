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

import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.FollowerInfo;
import org.apache.ratis.server.impl.LeaderState;
import org.apache.ratis.server.impl.LogAppender;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A new log appender implementation using grpc bi-directional stream API.
 */
public class GrpcLogAppender extends LogAppender {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcLogAppender.class);

  private final GrpcService rpcService;
  private final Map<Long, AppendEntriesRequestProto> pendingRequests;
  private final int maxPendingRequestsNum;
  private long callId = 0;
  private volatile boolean firstResponseReceived = false;

  private final TimeDuration requestTimeoutDuration;
  private final TimeoutScheduler scheduler = TimeoutScheduler.newInstance(1);

  private volatile StreamObserver<AppendEntriesRequestProto> appendLogRequestObserver;

  public GrpcLogAppender(RaftServerImpl server, LeaderState leaderState,
                         FollowerInfo f) {
    super(server, leaderState, f);

    this.rpcService = (GrpcService) server.getServerRpc();

    maxPendingRequestsNum = GrpcConfigKeys.Server.leaderOutstandingAppendsMax(
        server.getProxy().getProperties());
    requestTimeoutDuration = RaftServerConfigKeys.Rpc.requestTimeout(server.getProxy().getProperties());
    pendingRequests = new ConcurrentHashMap<>();
  }

  private GrpcServerProtocolClient getClient() throws IOException {
    return rpcService.getProxies().getProxy(follower.getPeer().getId());
  }

  private synchronized void resetClient(AppendEntriesRequestProto request) {
    rpcService.getProxies().resetProxy(follower.getPeer().getId());
    appendLogRequestObserver = null;
    firstResponseReceived = false;

    // clear the pending requests queue and reset the next index of follower
    final long nextIndex = request != null && request.hasPreviousLog()?
        request.getPreviousLog().getIndex() + 1: raftLog.getStartIndex();
    clearPendingRequests(nextIndex);
  }

  @Override
  protected void runAppenderImpl() throws IOException {
    for(; isAppenderRunning(); mayWait()) {
      if (shouldSendRequest()) {
        SnapshotInfo snapshot = shouldInstallSnapshot();
        if (snapshot != null) {
          installSnapshot(snapshot);
        } else if (!shouldWait()) {
          // keep appending log entries or sending heartbeats
          appendLog();
        }
      }
      checkSlowness();
    }

    Optional.ofNullable(appendLogRequestObserver).ifPresent(StreamObserver::onCompleted);
  }

  private long getWaitTimeMs() {
    if (!shouldSendRequest()) {
      return getHeartbeatRemainingTime(); // No requests, wait until heartbeat
    } else if (shouldWait()) {
      return halfMinTimeoutMs; // Should wait for a short time
    }
    return 0L;
  }

  private void mayWait() {
    // use lastSend time instead of lastResponse time
    final long waitTimeMs = getWaitTimeMs();
    if (waitTimeMs <= 0L) {
      return;
    }

    synchronized(this) {
      try {
        LOG.trace("{}: wait {}ms", this, waitTimeMs);
        wait(waitTimeMs);
      } catch(InterruptedException ie) {
        LOG.warn(this + ": Wait interrupted by " + ie);
      }
    }
  }

  @Override
  protected boolean shouldSendRequest() {
    return appendLogRequestObserver == null || super.shouldSendRequest();
  }

  /** @return true iff not received first response or queue is full. */
  private boolean shouldWait() {
    final int size = pendingRequests.size();
    if (size == 0) {
      return false;
    }
    return !firstResponseReceived || size >= maxPendingRequestsNum;
  }

  private void appendLog() throws IOException {
    final AppendEntriesRequestProto pending;
    final StreamObserver<AppendEntriesRequestProto> s;
    synchronized (this) {
      // prepare and enqueue the append request. note changes on follower's
      // nextIndex and ops on pendingRequests should always be associated
      // together and protected by the lock
      pending = createRequest(callId++);
      if (pending == null) {
        return;
      }
      pendingRequests.put(pending.getServerRequest().getCallId(), pending);
      updateNextIndex(pending);
      if (appendLogRequestObserver == null) {
        appendLogRequestObserver = getClient().appendEntries(new AppendLogResponseHandler());
      }
      s = appendLogRequestObserver;
    }

    if (isAppenderRunning()) {
      sendRequest(pending, s);
    }
  }

  private void sendRequest(AppendEntriesRequestProto request,
      StreamObserver<AppendEntriesRequestProto> s) {
    CodeInjectionForTesting.execute(GrpcService.GRPC_SEND_SERVER_REQUEST,
        server.getId(), null, request);

    s.onNext(request);
    scheduler.onTimeout(requestTimeoutDuration, () -> timeoutAppendRequest(request), LOG,
        () -> "Timeout check failed for append entry request: " + request);
    follower.updateLastRpcSendTime();
  }

  private void timeoutAppendRequest(AppendEntriesRequestProto request) {
    AppendEntriesRequestProto pendingRequest = pendingRequests.remove(request.getServerRequest().getCallId());
    if (pendingRequest != null) {
      LOG.warn( "{}: appendEntries Timeout, request={}", this, ProtoUtils.toString(pendingRequest.getServerRequest()));
    }
  }

  private void updateNextIndex(AppendEntriesRequestProto request) {
    final int count = request.getEntriesCount();
    if (count > 0) {
      follower.updateNextIndex(request.getEntries(count - 1).getIndex() + 1);
    }
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
        LOG.info("{} is stopped", GrpcLogAppender.this);
        return;
      }
      GrpcUtil.warn(LOG, () -> server.getId() + ": Failed appendEntries to " + follower.getPeer(), t);

      long callId = GrpcUtil.getCallId(t);
      resetClient(pendingRequests.get(callId));
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

  protected synchronized void onSuccess(AppendEntriesReplyProto reply) {
    AppendEntriesRequestProto request = pendingRequests.remove(reply.getServerReply().getCallId());
    if (request == null) {
      // If reply comes after timeout, the reply is ignored.
      LOG.warn("{}: Request not found, ignoring reply: {}", this, ServerProtoUtils.toString(reply));
      return;
    }
    updateCommitIndex(reply.getFollowerCommit());

    final long replyNextIndex = reply.getNextIndex();
    final long lastIndex = replyNextIndex - 1;
    final boolean updateMatchIndex;

    if (request.getEntriesCount() == 0) {
      Preconditions.assertTrue(!request.hasPreviousLog() ||
              lastIndex == request.getPreviousLog().getIndex(),
          "reply's next index is %s, request's previous is %s",
          replyNextIndex, request.getPreviousLog());
      updateMatchIndex = request.hasPreviousLog() && follower.getMatchIndex() < lastIndex;
    } else {
      // check if the reply and the pending request is consistent
      final long lastEntryIndex = request
          .getEntries(request.getEntriesCount() - 1).getIndex();
      Preconditions.assertTrue(lastIndex == lastEntryIndex,
          "reply's next index is %s, request's last entry index is %s",
          replyNextIndex, lastEntryIndex);
      updateMatchIndex = true;
    }
    if (updateMatchIndex) {
      follower.updateMatchIndex(lastIndex);
      submitEventOnSuccessAppend();
    }
  }

  private void onNotLeader(AppendEntriesReplyProto reply) {
    checkResponseTerm(reply.getTerm());
    // the running loop will end and the connection will onComplete
  }

  private synchronized void onInconsistency(AppendEntriesReplyProto reply) {
    AppendEntriesRequestProto request = pendingRequests.remove(reply.getServerReply().getCallId());
    if (request == null) {
      // If reply comes after timeout, the reply is ignored.
      LOG.warn("{}: Ignoring {}", server.getId(), reply);
      return;
    }
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
      GrpcLogAppender.this.notifyAppend();
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
        LOG.info("{} is stopped", GrpcLogAppender.this);
        return;
      }
      LOG.info("{} got error when installing snapshot to {}, exception: {}",
          server.getId(), follower.getPeer(), t);
      resetClient(null);
      close();
    }

    @Override
    public void onCompleted() {
      LOG.info("{} stops sending snapshots to follower {}", server.getId(),
          follower);
      close();
    }
  }

  private void installSnapshot(SnapshotInfo snapshot) {
    LOG.info("{}: follower {}'s next index is {}," +
            " log's start index is {}, need to install snapshot",
        server.getId(), follower.getPeer(), follower.getNextIndex(),
        raftLog.getStartIndex());

    final InstallSnapshotResponseHandler responseHandler = new InstallSnapshotResponseHandler();
    StreamObserver<InstallSnapshotRequestProto> snapshotRequestObserver = null;
    final String requestId = UUID.randomUUID().toString();
    try {
      snapshotRequestObserver = getClient().installSnapshot(responseHandler);
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
      if (snapshotRequestObserver != null) {
        snapshotRequestObserver.onError(e);
      }
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
