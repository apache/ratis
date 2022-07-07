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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.grpc.metrics.GrpcServerMetrics;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppenderBase;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.util.ServerStringUtils;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ratis.thirdparty.com.codahale.metrics.Timer;

/**
 * A new log appender implementation using grpc bi-directional stream API.
 */
public class GrpcLogAppender extends LogAppenderBase {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcLogAppender.class);

  private final RequestMap pendingRequests = new RequestMap();
  private final int maxPendingRequestsNum;
  private long callId = 0;
  private volatile boolean firstResponseReceived = false;
  private final boolean installSnapshotEnabled;

  private final TimeDuration requestTimeoutDuration;
  private final TimeoutScheduler scheduler = TimeoutScheduler.getInstance();

  private volatile StreamObserver<AppendEntriesRequestProto> appendLogRequestObserver;

  private final GrpcServerMetrics grpcServerMetrics;

  private final AutoCloseableReadWriteLock lock;
  private final StackTraceElement caller;

  public GrpcLogAppender(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
    super(server, leaderState, f);

    Preconditions.assertNotNull(getServerRpc(), "getServerRpc()");

    final RaftProperties properties = server.getRaftServer().getProperties();
    this.maxPendingRequestsNum = GrpcConfigKeys.Server.leaderOutstandingAppendsMax(properties);
    this.requestTimeoutDuration = RaftServerConfigKeys.Rpc.requestTimeout(properties);
    this.installSnapshotEnabled = RaftServerConfigKeys.Log.Appender.installSnapshotEnabled(properties);

    grpcServerMetrics = new GrpcServerMetrics(server.getMemberId().toString());
    grpcServerMetrics.addPendingRequestsCount(getFollowerId().toString(), pendingRequests::logRequestsSize);

    lock = new AutoCloseableReadWriteLock(this);
    caller = LOG.isTraceEnabled()? JavaUtils.getCallerStackTraceElement(): null;
  }

  @Override
  public GrpcService getServerRpc() {
    return (GrpcService)super.getServerRpc();
  }

  private GrpcServerProtocolClient getClient() throws IOException {
    return getServerRpc().getProxies().getProxy(getFollowerId());
  }

  private void resetClient(AppendEntriesRequest request, boolean onError) {
    try (AutoCloseableLock writeLock = lock.writeLock(caller, LOG::trace)) {
      getClient().resetConnectBackoff();
      appendLogRequestObserver = null;
      firstResponseReceived = false;
      // clear the pending requests queue and reset the next index of follower
      pendingRequests.clear();
      final long nextIndex = 1 + Optional.ofNullable(request)
          .map(AppendEntriesRequest::getPreviousLog)
          .map(TermIndex::getIndex)
          .orElseGet(getFollower()::getMatchIndex);
      if (onError && getFollower().getMatchIndex() == 0 && request == null) {
        LOG.warn("{}: Leader has not got in touch with Follower {} yet, " +
          "just keep nextIndex unchanged and retry.", this, getFollower());
        return;
      }
      getFollower().decreaseNextIndex(nextIndex);
    } catch (IOException ie) {
      LOG.warn(this + ": Failed to getClient for " + getFollowerId(), ie);
    }
  }

  private boolean isFollowerCommitBehindLastCommitIndex() {
    return getRaftLog().getLastCommittedIndex() > getFollower().getCommitIndex();
  }

  @Override
  public void run() throws IOException {
    boolean installSnapshotRequired;
    for(; isRunning(); mayWait()) {
      installSnapshotRequired = false;

      //HB period is expired OR we have messages OR follower is behind with commit index
      if (shouldSendAppendEntries() || isFollowerCommitBehindLastCommitIndex()) {

        if (installSnapshotEnabled) {
          SnapshotInfo snapshot = shouldInstallSnapshot();
          if (snapshot != null) {
            installSnapshot(snapshot);
            installSnapshotRequired = true;
          }
        } else {
          TermIndex installSnapshotNotificationTermIndex = shouldNotifyToInstallSnapshot();
          if (installSnapshotNotificationTermIndex != null) {
            installSnapshot(installSnapshotNotificationTermIndex);
            installSnapshotRequired = true;
          }
        }

        appendLog(installSnapshotRequired || haveTooManyPendingRequests());

      }
      getLeaderState().checkHealth(getFollower());
    }

    Optional.ofNullable(appendLogRequestObserver).ifPresent(StreamObserver::onCompleted);
  }

  public long getWaitTimeMs() {
    if (haveTooManyPendingRequests()) {
      return getHeartbeatWaitTimeMs(); // Should wait for a short time
    } else if (shouldSendAppendEntries()) {
      return 0L;
    }
    return Math.min(10L, getHeartbeatWaitTimeMs());
  }

  private void mayWait() {
    // use lastSend time instead of lastResponse time
    try {
      getEventAwaitForSignal().await(getWaitTimeMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      LOG.warn(this + ": Wait interrupted by " + ie);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void stop() {
    grpcServerMetrics.unregister();
    super.stop();
  }

  @Override
  public boolean shouldSendAppendEntries() {
    return appendLogRequestObserver == null || super.shouldSendAppendEntries();
  }

  /**
   * @return true iff not received first response or queue is full.
   */
  private boolean haveTooManyPendingRequests() {
    final int size = pendingRequests.logRequestsSize();
    if (size == 0) {
      return false;
    }
    return !firstResponseReceived || size >= maxPendingRequestsNum;
  }

  private void appendLog(boolean excludeLogEntries) throws IOException {
    final AppendEntriesRequestProto pending;
    final AppendEntriesRequest request;
    final StreamObserver<AppendEntriesRequestProto> s;
    try (AutoCloseableLock writeLock = lock.writeLock(caller, LOG::trace)) {
      // prepare and enqueue the append request. note changes on follower's
      // nextIndex and ops on pendingRequests should always be associated
      // together and protected by the lock
      pending = newAppendEntriesRequest(callId++, excludeLogEntries);
      if (pending == null) {
        return;
      }
      request = new AppendEntriesRequest(pending, getFollowerId(), grpcServerMetrics);
      pendingRequests.put(request);
      increaseNextIndex(pending);
      if (appendLogRequestObserver == null) {
        appendLogRequestObserver = getClient().appendEntries(new AppendLogResponseHandler());
      }
      s = appendLogRequestObserver;
    }

    if (isRunning()) {
      sendRequest(request, pending, s);
    }
  }

  private void sendRequest(AppendEntriesRequest request, AppendEntriesRequestProto proto,
        StreamObserver<AppendEntriesRequestProto> s) {
    CodeInjectionForTesting.execute(GrpcService.GRPC_SEND_SERVER_REQUEST,
        getServer().getId(), null, proto);
    request.startRequestTimer();
    s.onNext(proto);
    scheduler.onTimeout(requestTimeoutDuration,
        () -> timeoutAppendRequest(request.getCallId(), request.isHeartbeat()),
        LOG, () -> "Timeout check failed for append entry request: " + request);
    getFollower().updateLastRpcSendTime(request.isHeartbeat());
  }

  private void timeoutAppendRequest(long cid, boolean heartbeat) {
    final AppendEntriesRequest pending = pendingRequests.handleTimeout(cid, heartbeat);
    if (pending != null) {
      LOG.warn("{}: {} appendEntries Timeout, request={}", this, heartbeat ? "HEARTBEAT" : "", pending);
      grpcServerMetrics.onRequestTimeout(getFollowerId().toString(), heartbeat);
    }
  }

  private void increaseNextIndex(AppendEntriesRequestProto request) {
    final int count = request.getEntriesCount();
    if (count > 0) {
      getFollower().increaseNextIndex(request.getEntries(count - 1).getIndex() + 1);
    }
  }

  private void increaseNextIndex(final long installedSnapshotIndex) {
    getFollower().updateNextIndex(installedSnapshotIndex + 1);
  }

  /**
   * StreamObserver for handling responses from the follower
   */
  private class AppendLogResponseHandler implements StreamObserver<AppendEntriesReplyProto> {
    private final String name = getFollower().getName() + "-" + JavaUtils.getClassSimpleName(getClass());

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
      AppendEntriesRequest request = pendingRequests.remove(reply);
      if (request != null) {
        request.stopRequestTimer(); // Update completion time
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: received {} reply {}, request={}",
            this, firstResponseReceived? "a": "the first",
            ServerStringUtils.toAppendEntriesReplyString(reply), request);
      }

      try {
        onNextImpl(reply);
      } catch(Exception t) {
        LOG.error("Failed onNext request=" + request
            + ", reply=" + ServerStringUtils.toAppendEntriesReplyString(reply), t);
      }
    }

    private void onNextImpl(AppendEntriesReplyProto reply) {
      // update the last rpc time
      getFollower().updateLastRpcResponseTime();

      if (!firstResponseReceived) {
        firstResponseReceived = true;
      }

      switch (reply.getResult()) {
        case SUCCESS:
          grpcServerMetrics.onRequestSuccess(getFollowerId().toString(), reply.getIsHearbeat());
          getLeaderState().onFollowerCommitIndex(getFollower(), reply.getFollowerCommit());
          if (getFollower().updateMatchIndex(reply.getMatchIndex())) {
            getLeaderState().onFollowerSuccessAppendEntries(getFollower());
          }
          break;
        case NOT_LEADER:
          grpcServerMetrics.onRequestNotLeader(getFollowerId().toString());
          if (onFollowerTerm(reply.getTerm())) {
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
      notifyLogAppender();
    }

    /**
     * for now we simply retry the first pending request
     */
    @Override
    public void onError(Throwable t) {
      if (!isRunning()) {
        LOG.info("{} is stopped", GrpcLogAppender.this);
        return;
      }
      GrpcUtil.warn(LOG, () -> this + ": Failed appendEntries", t);
      grpcServerMetrics.onRequestRetry(); // Update try counter
      AppendEntriesRequest request = pendingRequests.remove(GrpcUtil.getCallId(t), GrpcUtil.isHeartbeat(t));
      resetClient(request, true);
    }

    @Override
    public void onCompleted() {
      LOG.info("{}: follower responses appendEntries COMPLETED", this);
      resetClient(null, false);
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private void updateNextIndex(long replyNextIndex) {
    try (AutoCloseableLock writeLock = lock.writeLock(caller, LOG::trace)) {
      pendingRequests.clear();
      getFollower().setNextIndex(replyNextIndex);
    }
  }

  private class InstallSnapshotResponseHandler implements StreamObserver<InstallSnapshotReplyProto> {
    private final String name = getFollower().getName() + "-" + JavaUtils.getClassSimpleName(getClass());
    private final Queue<Integer> pending;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final boolean isNotificationOnly;

    InstallSnapshotResponseHandler() {
      this(false);
    }

    InstallSnapshotResponseHandler(boolean notifyOnly) {
      pending = new LinkedList<>();
      this.isNotificationOnly = notifyOnly;
    }

    void addPending(InstallSnapshotRequestProto request) {
      try (AutoCloseableLock writeLock = lock.writeLock(caller, LOG::trace)) {
        pending.offer(request.getSnapshotChunk().getRequestIndex());
      }
    }

    void removePending(InstallSnapshotReplyProto reply) {
      try (AutoCloseableLock writeLock = lock.writeLock(caller, LOG::trace)) {
        final Integer index = pending.poll();
        Objects.requireNonNull(index, "index == null");
        Preconditions.assertTrue(index == reply.getRequestIndex());
      }
    }

    //compare follower's latest installed snapshot index with leader's start index
    void onFollowerCatchup(long followerSnapshotIndex) {
      final long leaderStartIndex = getRaftLog().getStartIndex();
      final long followerNextIndex = followerSnapshotIndex + 1;
      if (followerNextIndex >= leaderStartIndex) {
        LOG.info("{}: Follower can catch up leader after install the snapshot, as leader's start index is {}",
            this, followerNextIndex);
        notifyInstallSnapshotFinished(InstallSnapshotResult.SUCCESS, followerSnapshotIndex);
      }
    }

    void notifyInstallSnapshotFinished(InstallSnapshotResult result, long snapshotIndex) {
      getServer().getStateMachine().event().notifySnapshotInstalled(result, snapshotIndex);
    }

    boolean isDone() {
      return done.get();
    }

    void close() {
      done.set(true);
      notifyLogAppender();
    }

    boolean hasAllResponse() {
      try (AutoCloseableLock readLock = lock.readLock(caller, LOG::trace)) {
        return pending.isEmpty();
      }
    }

    @Override
    public void onNext(InstallSnapshotReplyProto reply) {
      if (LOG.isInfoEnabled()) {
        LOG.info("{}: received {} reply {}", this, firstResponseReceived ? "a" : "the first",
            ServerStringUtils.toInstallSnapshotReplyString(reply));
      }

      // update the last rpc time
      getFollower().updateLastRpcResponseTime();

      if (!firstResponseReceived) {
        firstResponseReceived = true;
      }
      final long followerSnapshotIndex;
      switch (reply.getResult()) {
        case SUCCESS:
          LOG.info("{}: Completed InstallSnapshot. Reply: {}", this, reply);
          getFollower().setAttemptedToInstallSnapshot();
          removePending(reply);
          break;
        case IN_PROGRESS:
          LOG.info("{}: InstallSnapshot in progress.", this);
          removePending(reply);
          break;
        case ALREADY_INSTALLED:
          followerSnapshotIndex = reply.getSnapshotIndex();
          LOG.info("{}: Follower snapshot is already at index {}.", this, followerSnapshotIndex);
          getFollower().setSnapshotIndex(followerSnapshotIndex);
          getFollower().setAttemptedToInstallSnapshot();
          getLeaderState().onFollowerCommitIndex(getFollower(), followerSnapshotIndex);
          increaseNextIndex(followerSnapshotIndex);
          removePending(reply);
          break;
        case NOT_LEADER:
          onFollowerTerm(reply.getTerm());
          break;
        case CONF_MISMATCH:
          LOG.error("{}: Configuration Mismatch ({}): Leader {} has it set to {} but follower {} has it set to {}",
              this, RaftServerConfigKeys.Log.Appender.INSTALL_SNAPSHOT_ENABLED_KEY,
              getServer().getId(), installSnapshotEnabled, getFollowerId(), !installSnapshotEnabled);
          break;
        case SNAPSHOT_INSTALLED:
          followerSnapshotIndex = reply.getSnapshotIndex();
          LOG.info("{}: Follower installed snapshot at index {}", this, followerSnapshotIndex);
          getFollower().setSnapshotIndex(followerSnapshotIndex);
          getFollower().setAttemptedToInstallSnapshot();
          getLeaderState().onFollowerCommitIndex(getFollower(), followerSnapshotIndex);
          increaseNextIndex(followerSnapshotIndex);
          onFollowerCatchup(followerSnapshotIndex);
          removePending(reply);
          break;
        case SNAPSHOT_UNAVAILABLE:
          LOG.info("{}: Follower could not install snapshot as it is not available.", this);
          getFollower().setAttemptedToInstallSnapshot();
          notifyInstallSnapshotFinished(InstallSnapshotResult.SNAPSHOT_UNAVAILABLE, RaftLog.INVALID_LOG_INDEX);
          removePending(reply);
          break;
        case UNRECOGNIZED:
          LOG.error("Unrecongnized the reply result {}: Leader is {}, follower is {}",
              reply.getResult(), getServer().getId(), getFollowerId());
          break;
        default:
          break;
      }
    }

    @Override
    public void onError(Throwable t) {
      if (!isRunning()) {
        LOG.info("{} is stopped", GrpcLogAppender.this);
        return;
      }
      GrpcUtil.warn(LOG, () -> this + ": Failed InstallSnapshot", t);
      grpcServerMetrics.onRequestRetry(); // Update try counter
      resetClient(null, true);
      close();
    }

    @Override
    public void onCompleted() {
      if (!isNotificationOnly || LOG.isDebugEnabled()) {
        LOG.info("{}: follower responded installSnapshot COMPLETED", this);
      }
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
        this, getFollower().getNextIndex(), getRaftLog().getStartIndex(), snapshot);

    final InstallSnapshotResponseHandler responseHandler = new InstallSnapshotResponseHandler();
    StreamObserver<InstallSnapshotRequestProto> snapshotRequestObserver = null;
    final String requestId = UUID.randomUUID().toString();
    try {
      snapshotRequestObserver = getClient().installSnapshot(responseHandler);
      for (InstallSnapshotRequestProto request : newInstallSnapshotRequests(requestId, snapshot)) {
        if (isRunning()) {
          snapshotRequestObserver.onNext(request);
          getFollower().updateLastRpcSendTime(false);
          responseHandler.addPending(request);
        } else {
          break;
        }
      }
      snapshotRequestObserver.onCompleted();
      grpcServerMetrics.onInstallSnapshot();
    } catch (Exception e) {
      LOG.warn("{}: failed to install snapshot {}: {}", this, snapshot.getFiles(), e);
      if (snapshotRequestObserver != null) {
        snapshotRequestObserver.onError(e);
      }
      return;
    }

    while (isRunning() && !responseHandler.isDone()) {
      try {
        getEventAwaitForSignal().await();
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }

    if (responseHandler.hasAllResponse()) {
      getFollower().setSnapshotIndex(snapshot.getTermIndex().getIndex());
      LOG.info("{}: installed snapshot {} successfully", this, snapshot);
    }
  }

  /**
   * Send installSnapshot request to Follower with only a notification that a snapshot needs to be installed.
   * @param firstAvailableLogTermIndex the first available log's index on the Leader
   */
  private void installSnapshot(TermIndex firstAvailableLogTermIndex) {
    LOG.info("{}: followerNextIndex = {} but logStartIndex = {}, notify follower to install snapshot-{}",
        this, getFollower().getNextIndex(), getRaftLog().getStartIndex(), firstAvailableLogTermIndex);

    final InstallSnapshotResponseHandler responseHandler = new InstallSnapshotResponseHandler(true);
    StreamObserver<InstallSnapshotRequestProto> snapshotRequestObserver = null;
    // prepare and enqueue the notify install snapshot request.
    final InstallSnapshotRequestProto request = newInstallSnapshotNotificationRequest(firstAvailableLogTermIndex);
    if (LOG.isInfoEnabled()) {
      LOG.info("{}: send {}", this, ServerStringUtils.toInstallSnapshotRequestString(request));
    }
    try {
      snapshotRequestObserver = getClient().installSnapshot(responseHandler);
      snapshotRequestObserver.onNext(request);
      getFollower().updateLastRpcSendTime(false);
      responseHandler.addPending(request);
      snapshotRequestObserver.onCompleted();
    } catch (Exception e) {
      GrpcUtil.warn(LOG, () -> this + ": Failed to notify follower to install snapshot.", e);
      if (snapshotRequestObserver != null) {
        snapshotRequestObserver.onError(e);
      }
      return;
    }

    while (isRunning() && !responseHandler.isDone()) {
      try {
        getEventAwaitForSignal().await();
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Should the Leader notify the Follower to install the snapshot through
   * its own State Machine.
   * @return the first available log's start term index
   */
  private TermIndex shouldNotifyToInstallSnapshot() {
    final FollowerInfo follower = getFollower();
    final long leaderNextIndex = getRaftLog().getNextIndex();
    final boolean isFollowerBootstrapping = getLeaderState().isFollowerBootstrapping(follower);
    final long leaderStartIndex = getRaftLog().getStartIndex();
    final TermIndex firstAvailable = Optional.ofNullable(getRaftLog().getTermIndex(leaderStartIndex))
        .orElseGet(() -> TermIndex.valueOf(getServer().getInfo().getCurrentTerm(), leaderNextIndex));
    if (isFollowerBootstrapping && !follower.hasAttemptedToInstallSnapshot()) {
      // If the follower is bootstrapping and has not yet installed any snapshot from leader, then the follower should
      // be notified to install a snapshot. Every follower should try to install at least one snapshot during
      // bootstrapping, if available.
      LOG.debug("{}: follower is bootstrapping, notify to install snapshot to {}.", this, firstAvailable);
      return firstAvailable;
    }

    final long followerNextIndex = follower.getNextIndex();
    if (followerNextIndex >= leaderNextIndex) {
      return null;
    }

    if (followerNextIndex < leaderStartIndex) {
      // The Leader does not have the logs from the Follower's last log
      // index onwards. And install snapshot is disabled. So the Follower
      // should be notified to install the latest snapshot through its
      // State Machine.
      return firstAvailable;
    } else if (leaderStartIndex == RaftLog.INVALID_LOG_INDEX) {
      // Leader has no logs to check from, hence return next index.
      return firstAvailable;
    }

    return null;
  }

  static class AppendEntriesRequest {
    private final Timer timer;
    private volatile Timer.Context timerContext;

    private final long callId;
    private final TermIndex previousLog;
    private final int entriesCount;

    private final TermIndex lastEntry;

    AppendEntriesRequest(AppendEntriesRequestProto proto, RaftPeerId followerId, GrpcServerMetrics grpcServerMetrics) {
      this.callId = proto.getServerRequest().getCallId();
      this.previousLog = proto.hasPreviousLog()? TermIndex.valueOf(proto.getPreviousLog()): null;
      this.entriesCount = proto.getEntriesCount();
      this.lastEntry = entriesCount > 0? TermIndex.valueOf(proto.getEntries(entriesCount - 1)): null;

      this.timer = grpcServerMetrics.getGrpcLogAppenderLatencyTimer(followerId.toString(), isHeartbeat());
      grpcServerMetrics.onRequestCreate(isHeartbeat());
    }

    long getCallId() {
      return callId;
    }

    TermIndex getPreviousLog() {
      return previousLog;
    }

    void startRequestTimer() {
      timerContext = timer.time();
    }

    void stopRequestTimer() {
      timerContext.stop();
    }

    boolean isHeartbeat() {
      return entriesCount == 0;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass())
          + ":cid=" + callId
          + ",entriesCount=" + entriesCount
          + ",lastEntry=" + lastEntry;
    }
  }

  static class RequestMap {
    private final Map<Long, AppendEntriesRequest> logRequests = new ConcurrentHashMap<>();
    private final Map<Long, AppendEntriesRequest> heartbeats = new ConcurrentHashMap<>();

    int logRequestsSize() {
      return logRequests.size();
    }

    void clear() {
      logRequests.clear();
      heartbeats.clear();
    }

    void put(AppendEntriesRequest request) {
      if (request.isHeartbeat()) {
        heartbeats.put(request.getCallId(), request);
      } else {
        logRequests.put(request.getCallId(), request);
      }
    }

    AppendEntriesRequest remove(AppendEntriesReplyProto reply) {
      return remove(reply.getServerReply().getCallId(), reply.getIsHearbeat());
    }

    AppendEntriesRequest remove(long cid, boolean isHeartbeat) {
      return isHeartbeat ? heartbeats.remove(cid): logRequests.remove(cid);
    }

    public AppendEntriesRequest handleTimeout(long callId, boolean heartbeat) {
      return heartbeat ? heartbeats.remove(callId) : logRequests.get(callId);
    }
  }
}
