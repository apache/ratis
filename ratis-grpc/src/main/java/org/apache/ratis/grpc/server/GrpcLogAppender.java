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
package org.apache.ratis.grpc.server;

import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.grpc.metrics.GrpcServerMetrics;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.FollowerInfo;
import org.apache.ratis.server.impl.LeaderState;
import org.apache.ratis.server.impl.LogAppender;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
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

import com.codahale.metrics.Timer;

/**
 * A new log appender implementation using grpc bi-directional stream API.
 */
public class GrpcLogAppender extends LogAppender {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcLogAppender.class);

  private final GrpcService rpcService;
  private final Map<Long, AppendEntriesRequest> pendingRequests;
  private final int maxPendingRequestsNum;
  private long callId = 0;
  private volatile boolean firstResponseReceived = false;
  private final boolean installSnapshotEnabled;

  private final TimeDuration requestTimeoutDuration;
  private final TimeoutScheduler scheduler = TimeoutScheduler.newInstance(1);

  private volatile StreamObserver<AppendEntriesRequestProto> appendLogRequestObserver;

  private final GrpcServerMetrics grpcServerMetrics;

  public GrpcLogAppender(RaftServerImpl server, LeaderState leaderState,
                         FollowerInfo f) {
    super(server, leaderState, f);

    this.rpcService = (GrpcService) server.getServerRpc();

    maxPendingRequestsNum = GrpcConfigKeys.Server.leaderOutstandingAppendsMax(
        server.getProxy().getProperties());
    requestTimeoutDuration = RaftServerConfigKeys.Rpc.requestTimeout(server.getProxy().getProperties());
    pendingRequests = new ConcurrentHashMap<>();
    installSnapshotEnabled = RaftServerConfigKeys.Log.Appender.installSnapshotEnabled(
        server.getProxy().getProperties());
    grpcServerMetrics = new GrpcServerMetrics(server.getMemberId().toString());
  }

  private GrpcServerProtocolClient getClient() throws IOException {
    return rpcService.getProxies().getProxy(getFollowerId());
  }

  private synchronized void resetClient(AppendEntriesRequestProto request) {
    rpcService.getProxies().resetProxy(getFollowerId());
    appendLogRequestObserver = null;
    firstResponseReceived = false;

    // clear the pending requests queue and reset the next index of follower
    final long nextIndex = request != null && request.hasPreviousLog()?
        request.getPreviousLog().getIndex() + 1: follower.getMatchIndex() + 1;
    pendingRequests.clear();
    follower.decreaseNextIndex(nextIndex);
  }

  @Override
  protected void runAppenderImpl() throws IOException {
    boolean shouldAppendLog;
    for(; isAppenderRunning(); mayWait()) {
      shouldAppendLog = true;
      if (shouldSendRequest()) {
        if (installSnapshotEnabled) {
          SnapshotInfo snapshot = shouldInstallSnapshot();
          if (snapshot != null) {
            installSnapshot(snapshot);
            shouldAppendLog = false;
          }
        } else {
          TermIndex installSnapshotNotificationTermIndex = shouldNotifyToInstallSnapshot();
          if (installSnapshotNotificationTermIndex != null) {
            installSnapshot(installSnapshotNotificationTermIndex);
            shouldAppendLog = false;
          }
        }
        if (shouldAppendLog && !shouldWait()) {
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
    final AppendEntriesRequest request;
    final StreamObserver<AppendEntriesRequestProto> s;
    synchronized (this) {
      // prepare and enqueue the append request. note changes on follower's
      // nextIndex and ops on pendingRequests should always be associated
      // together and protected by the lock
      AppendEntriesRequestProto pending = createRequest(callId++);
      if (pending == null) {
        return;
      }
      grpcServerMetrics.onRequestCreate();
      request = new AppendEntriesRequest(pending,
          grpcServerMetrics.getGrpcLogAppenderLatencyTimer(getFollowerId().toString()));
      pendingRequests.put(pending.getServerRequest().getCallId(), request);
      increaseNextIndex(pending);
      if (appendLogRequestObserver == null) {
        appendLogRequestObserver = getClient().appendEntries(new AppendLogResponseHandler());
      }
      s = appendLogRequestObserver;
    }

    if (isAppenderRunning()) {
      sendRequest(request, s);
    }
  }

  private void sendRequest(AppendEntriesRequest request, StreamObserver<AppendEntriesRequestProto> s) {
    CodeInjectionForTesting.execute(GrpcService.GRPC_SEND_SERVER_REQUEST,
        server.getId(), null, request);
    AppendEntriesRequestProto requestProto = request.getRequestProto();
    request.startRequestTimer();
    s.onNext(requestProto);
    scheduler.onTimeout(requestTimeoutDuration, () -> timeoutAppendRequest(requestProto), LOG,
        () -> "Timeout check failed for append entry request: " + request);
    follower.updateLastRpcSendTime();
  }

  private void timeoutAppendRequest(AppendEntriesRequestProto request) {
    AppendEntriesRequest pendingRequest = pendingRequests.remove(request.getServerRequest().getCallId());
    if (pendingRequest != null) {
      LOG.warn( "{}: appendEntries Timeout, request={}", this,
          ServerProtoUtils.toString(pendingRequest.getRequestProto()));
    }
  }

  private void increaseNextIndex(AppendEntriesRequestProto request) {
    final int count = request.getEntriesCount();
    if (count > 0) {
      follower.increaseNextIndex(request.getEntries(count - 1).getIndex() + 1);
    }
  }

  /**
   * StreamObserver for handling responses from the follower
   */
  private class AppendLogResponseHandler implements StreamObserver<AppendEntriesReplyProto> {
    private final String name = follower.getName() + "-" + getClass().getSimpleName();

    /**
     * After receiving a appendEntries reply, do the following:
     * 1. If the reply is success, update the follower's match index and submit
     *    an event to leaderState
     * 2. If the reply is NOT_LEADER, step down
     * 3. If the reply is INCONSISTENCY, increase/ decrease the follower's next
     *    index based on the response
     */
    @Override
    public void onNext(AppendEntriesReplyProto reply) {
      final AppendEntriesRequest request = pendingRequests.remove(reply.getServerReply().getCallId());
      AppendEntriesRequestProto requestProto = request.getRequestProto();
      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: received {} reply {}, request={}",
            this, firstResponseReceived? "a": "the first",
            ServerProtoUtils.toString(reply), ServerProtoUtils.toString(requestProto));
      }
      request.stopRequestTimer(); // Update completion time

      try {
        onNextImpl(requestProto, reply);
      } catch(Throwable t) {
        LOG.error("Failed onNext request=" + ServerProtoUtils.toString(requestProto)
            + ", reply=" + ServerProtoUtils.toString(reply), t);
      }
    }

    private void onNextImpl(AppendEntriesRequestProto request, AppendEntriesReplyProto reply) {
      // update the last rpc time
      follower.updateLastRpcResponseTime();

      if (!firstResponseReceived) {
        firstResponseReceived = true;
      }
      if (request == null) {
        // The request is already handled (probably timeout), ignore the reply.
        LOG.warn("{}: Request not found, ignoring reply: {}", this, ServerProtoUtils.toString(reply));
        return;
      }

      switch (reply.getResult()) {
        case SUCCESS:
          grpcServerMetrics.onRequestSuccess(getFollowerId().toString());
          updateCommitIndex(reply.getFollowerCommit());
          if (checkAndUpdateMatchIndex(request)) {
            submitEventOnSuccessAppend();
          }
          break;
        case NOT_LEADER:
          grpcServerMetrics.onRequestNotLeader(getFollowerId().toString());
          if (checkResponseTerm(reply.getTerm())) {
            return;
          }
          break;
        case INCONSISTENCY:
          grpcServerMetrics.onRequestInconsistency(getFollowerId().toString());
          updateNextIndex(reply.getNextIndex());
          break;
        default:
          throw new IllegalStateException("Unexpected reply result: " + reply.getResult());
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
      GrpcUtil.warn(LOG, () -> this + ": Failed appendEntries", t);
      grpcServerMetrics.onRequestRetry(); // Update try counter
      long callId = GrpcUtil.getCallId(t);
      resetClient(pendingRequests.remove(callId).getRequestProto());
    }

    @Override
    public void onCompleted() {
      LOG.info("{}: follower responses appendEntries COMPLETED", this);
      resetClient(null);
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private boolean checkAndUpdateMatchIndex(AppendEntriesRequestProto request) {
    final int n = request.getEntriesCount();
    final long newMatchIndex = n == 0? request.getPreviousLog().getIndex(): request.getEntries(n - 1).getIndex();
    return follower.updateMatchIndex(newMatchIndex);
  }

  private synchronized void updateNextIndex(long replyNextIndex) {
    pendingRequests.clear();
    follower.updateNextIndex(replyNextIndex);
  }

  private class InstallSnapshotResponseHandler implements StreamObserver<InstallSnapshotReplyProto> {
    private final String name = follower.getName() + "-" + getClass().getSimpleName();
    private final Queue<Integer> pending;
    private final AtomicBoolean done = new AtomicBoolean(false);

    InstallSnapshotResponseHandler() {
      pending = new LinkedList<>();
    }

    synchronized void addPending(InstallSnapshotRequestProto request) {
      pending.offer(request.getSnapshotChunk().getRequestIndex());
    }

    synchronized void removePending(InstallSnapshotReplyProto reply) {
      final Integer index = pending.poll();
      Objects.requireNonNull(index, "index == null");
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
      if (LOG.isInfoEnabled()) {
        LOG.info("{}: received {} reply {}", this, firstResponseReceived ? "a" : "the first",
            ServerProtoUtils.toString(reply));
      }

      // update the last rpc time
      follower.updateLastRpcResponseTime();

      if (!firstResponseReceived) {
        firstResponseReceived = true;
      }

      switch (reply.getResult()) {
        case SUCCESS:
        case IN_PROGRESS:
          removePending(reply);
          break;
        case ALREADY_INSTALLED:
          final long followerSnapshotIndex = reply.getSnapshotIndex();
          LOG.info("{}: set follower snapshotIndex to {}.", this, followerSnapshotIndex);
          follower.setSnapshotIndex(followerSnapshotIndex);
          removePending(reply);
          break;
        case NOT_LEADER:
          checkResponseTerm(reply.getTerm());
          break;
        case CONF_MISMATCH:
          LOG.error("{}: Configuration Mismatch ({}): Leader {} has it set to {} but follower {} has it set to {}",
              this, RaftServerConfigKeys.Log.Appender.INSTALL_SNAPSHOT_ENABLED_KEY,
              server.getId(), installSnapshotEnabled, getFollowerId(), !installSnapshotEnabled);
        case UNRECOGNIZED:
          break;
      }
    }

    @Override
    public void onError(Throwable t) {
      if (!isAppenderRunning()) {
        LOG.info("{} is stopped", this);
        return;
      }
      LOG.error("{}: Failed installSnapshot: {}", this, t);
      resetClient(null);
      close();
    }

    @Override
    public void onCompleted() {
      LOG.info("{}: follower responses installSnapshot COMPLETED", this);
      close();
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Send installSnapshot request to Follower with a snapshot.
   * @param snapshot the snapshot to be sent to Follower
   */
  private void installSnapshot(SnapshotInfo snapshot) {
    LOG.info("{}: followerNextIndex = {} but logStartIndex = {}, send snapshot {} to follower",
        this, follower.getNextIndex(), raftLog.getStartIndex(), snapshot);

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
      LOG.warn("{}: failed to install snapshot {}: {}", this, snapshot.getFiles(), e);
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
      follower.setSnapshotIndex(snapshot.getTermIndex().getIndex());
      LOG.info("{}: installed snapshot {} successfully", this, snapshot);
    }
  }

  /**
   * Send installSnapshot request to Follower with only a notification that a snapshot needs to be installed.
   * @param firstAvailableLogTermIndex the first available log's index on the Leader
   */
  private void installSnapshot(TermIndex firstAvailableLogTermIndex) {
    LOG.info("{}: followerNextIndex = {} but logStartIndex = {}, notify follower to install snapshot-{}",
        this, follower.getNextIndex(), raftLog.getStartIndex(), firstAvailableLogTermIndex);

    final InstallSnapshotResponseHandler responseHandler = new InstallSnapshotResponseHandler();
    StreamObserver<InstallSnapshotRequestProto> snapshotRequestObserver = null;
    // prepare and enqueue the notify install snapshot request.
    final InstallSnapshotRequestProto request = createInstallSnapshotNotificationRequest(firstAvailableLogTermIndex);
    if (LOG.isInfoEnabled()) {
      LOG.info("{}: send {}", this, ServerProtoUtils.toString(request));
    }
    try {
      snapshotRequestObserver = getClient().installSnapshot(responseHandler);
      snapshotRequestObserver.onNext(request);
      follower.updateLastRpcSendTime();
      responseHandler.addPending(request);
      snapshotRequestObserver.onCompleted();
    } catch (Exception e) {
      GrpcUtil.warn(LOG, () -> this + ": Failed to notify follower to install snapshot.", e);
      if (snapshotRequestObserver != null) {
        snapshotRequestObserver.onError(e);
      }
      return;
    }

    synchronized (this) {
      if (isAppenderRunning() && !responseHandler.isDone()) {
        try {
          wait();
        } catch (InterruptedException ignored) {
        }
      }
    }
  }

  /**
   * Should the Leader notify the Follower to install the snapshot through
   * its own State Machine.
   * @return the first available log's start term index
   */
  private TermIndex shouldNotifyToInstallSnapshot() {
    if (follower.getNextIndex() < raftLog.getStartIndex()) {
      // The Leader does not have the logs from the Follower's last log
      // index onwards. And install snapshot is disabled. So the Follower
      // should be notified to install the latest snapshot through its
      // State Machine.
      return raftLog.getTermIndex(raftLog.getStartIndex());
    }
    return null;
  }

  static class AppendEntriesRequest {
    private final AppendEntriesRequestProto requestProto;
    private final Timer timer;
    private Timer.Context timerContext;

    AppendEntriesRequest(AppendEntriesRequestProto requestProto, Timer timer) {
      this.requestProto = requestProto;
      this.timer = timer;
    }

    AppendEntriesRequestProto getRequestProto() {
      return requestProto;
    }

    void startRequestTimer() {
      timerContext = timer.time();
    }

    void stopRequestTimer() {
      timerContext.stop();
    }
  }
}
