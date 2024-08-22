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
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.MultipleLinearRandomRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppenderBase;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto.InstallSnapshotReplyBodyCase;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto.InstallSnapshotRequestBodyCase;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A new log appender implementation using grpc bi-directional stream API.
 */
public class GrpcLogAppender extends LogAppenderBase {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcLogAppender.class);

  private enum BatchLogKey implements BatchLogger.Key {
    RESET_CLIENT,
    APPEND_LOG_RESPONSE_HANDLER_ON_ERROR
  }

  public static final int INSTALL_SNAPSHOT_NOTIFICATION_INDEX = 0;

  private static final Comparator<Long> CALL_ID_COMPARATOR = (left, right) -> {
    // calculate diff in order to take care the possibility of numerical overflow
    final long diff = left - right;
    return diff == 0? 0: diff > 0? 1: -1;
  };

  enum Event {
    APPEND_ENTRIES_REPLY,
    APPEND_ENTRIES_INCONSISTENCY_REPLY,
    SNAPSHOT_REPLY,
    COMPLETE,
    TIMEOUT,
    ERROR;

    boolean updateFirstReplyReceived(boolean firstReplyReceived) {
      switch (this) {
        case APPEND_ENTRIES_REPLY:
        case APPEND_ENTRIES_INCONSISTENCY_REPLY:
        case SNAPSHOT_REPLY:
        case COMPLETE:
          return true;
        case ERROR:
          return false;
        case TIMEOUT:
          return firstReplyReceived;
        default:
          throw new IllegalStateException("Unexpected event: " + this);
      }
    }

    boolean isError() {
      switch (this) {
        case APPEND_ENTRIES_INCONSISTENCY_REPLY:
        case TIMEOUT:
        case ERROR:
          return true;
        case APPEND_ENTRIES_REPLY:
        case SNAPSHOT_REPLY:
        case COMPLETE:
          return false;
        default:
          throw new IllegalStateException("Unexpected event: " + this);
      }
    }
  }

  static class ReplyState {
    private boolean firstReplyReceived = false;
    private int errorCount = 0;

    synchronized boolean isFirstReplyReceived() {
      return firstReplyReceived;
    }

    synchronized int getErrorCount() {
      return errorCount;
    }

    int process(AppendResult result) {
      return process(result == AppendResult.INCONSISTENCY? Event.APPEND_ENTRIES_INCONSISTENCY_REPLY
          : Event.APPEND_ENTRIES_REPLY);
    }

    synchronized int process(Event event) {
      firstReplyReceived = event.updateFirstReplyReceived(firstReplyReceived);
      if (event.isError()) {
        errorCount++;
      } else {
        errorCount = 0;
      }
      return errorCount;
    }
  }

  private final AtomicLong callId = new AtomicLong();

  private final RequestMap pendingRequests = new RequestMap();
  private final int maxPendingRequestsNum;
  private final boolean installSnapshotEnabled;

  private final TimeDuration requestTimeoutDuration;
  private final TimeDuration installSnapshotStreamTimeout;
  private final TimeDuration logMessageBatchDuration;
  private final int maxOutstandingInstallSnapshots;
  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile StreamObservers appendLogRequestObserver;
  private final boolean useSeparateHBChannel;

  private final GrpcServerMetrics grpcServerMetrics;

  private final AutoCloseableReadWriteLock lock;
  private final StackTraceElement caller;
  private final RetryPolicy errorRetryWaitPolicy;
  private final ReplyState replyState = new ReplyState();

  public GrpcLogAppender(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
    super(server, leaderState, f);

    Preconditions.assertNotNull(getServerRpc(), "getServerRpc()");

    final RaftProperties properties = server.getRaftServer().getProperties();
    this.maxPendingRequestsNum = GrpcConfigKeys.Server.leaderOutstandingAppendsMax(properties);
    this.requestTimeoutDuration = RaftServerConfigKeys.Rpc.requestTimeout(properties);
    this.maxOutstandingInstallSnapshots = GrpcConfigKeys.Server.installSnapshotRequestElementLimit(properties);
    this.installSnapshotStreamTimeout = GrpcConfigKeys.Server.installSnapshotRequestTimeout(properties)
        .multiply(maxOutstandingInstallSnapshots);
    this.logMessageBatchDuration = GrpcConfigKeys.Server.logMessageBatchDuration(properties);
    this.installSnapshotEnabled = RaftServerConfigKeys.Log.Appender.installSnapshotEnabled(properties);
    this.useSeparateHBChannel = GrpcConfigKeys.Server.heartbeatChannel(properties);

    grpcServerMetrics = new GrpcServerMetrics(server.getMemberId().toString());
    grpcServerMetrics.addPendingRequestsCount(getFollowerId().toString(), pendingRequests::logRequestsSize);

    lock = new AutoCloseableReadWriteLock(this);
    caller = LOG.isTraceEnabled()? JavaUtils.getCallerStackTraceElement(): null;
    errorRetryWaitPolicy = MultipleLinearRandomRetry.parseCommaSeparated(
        RaftServerConfigKeys.Log.Appender.retryPolicy(properties));
  }

  @Override
  public GrpcService getServerRpc() {
    return (GrpcService)super.getServerRpc();
  }

  private GrpcServerProtocolClient getClient() throws IOException {
    return getServerRpc().getProxies().getProxy(getFollowerId());
  }

  private void resetClient(AppendEntriesRequest request, Event event) {
    try (AutoCloseableLock writeLock = lock.writeLock(caller, LOG::trace)) {
      getClient().resetConnectBackoff();
      if (appendLogRequestObserver != null) {
        appendLogRequestObserver.stop();
        appendLogRequestObserver = null;
      }
      final int errorCount = replyState.process(event);
      // clear the pending requests queue and reset the next index of follower
      pendingRequests.clear();
      final FollowerInfo f = getFollower();
      final long nextIndex = 1 + Optional.ofNullable(request)
          .map(AppendEntriesRequest::getPreviousLog)
          .map(TermIndex::getIndex)
          .orElseGet(f::getMatchIndex);
      if (event.isError() && request == null) {
        final long followerNextIndex = f.getNextIndex();
        BatchLogger.warn(BatchLogKey.RESET_CLIENT, f.getId() + "-" + followerNextIndex, suffix ->
            LOG.warn("{}: Follower failed (request=null, errorCount={}); keep nextIndex ({}) unchanged and retry.{}",
                this, errorCount, followerNextIndex, suffix), logMessageBatchDuration);
        return;
      }
      if (request != null && request.isHeartbeat()) {
        return;
      }
      getFollower().computeNextIndex(getNextIndexForError(nextIndex));
    } catch (IOException ie) {
      LOG.warn(this + ": Failed to getClient for " + getFollowerId(), ie);
    }
  }

  private boolean isFollowerCommitBehindLastCommitIndex() {
    return getRaftLog().getLastCommittedIndex() > getFollower().getCommitIndex();
  }

  private boolean installSnapshot() {
    if (installSnapshotEnabled) {
      final SnapshotInfo snapshot = shouldInstallSnapshot();
      if (snapshot != null) {
        installSnapshot(snapshot);
        return true;
      }
    } else {
      // check installSnapshotNotification
      final TermIndex firstAvailable = shouldNotifyToInstallSnapshot();
      if (firstAvailable != null) {
        notifyInstallSnapshot(firstAvailable);
        return true;
      }
    }
    return false;
  }

  @Override
  public void run() throws IOException {
    for(; isRunning(); mayWait()) {
      //HB period is expired OR we have messages OR follower is behind with commit index
      if (shouldSendAppendEntries() || isFollowerCommitBehindLastCommitIndex()) {
        final boolean installingSnapshot = installSnapshot();
        appendLog(installingSnapshot || haveTooManyPendingRequests());
      }
      getLeaderState().checkHealth(getFollower());
    }

    Optional.ofNullable(appendLogRequestObserver).ifPresent(StreamObservers::onCompleted);
  }

  public long getWaitTimeMs() {
    if (haveTooManyPendingRequests()) {
      return getHeartbeatWaitTimeMs(); // Should wait for a short time
    } else if (shouldSendAppendEntries() && !isSlowFollower()) {
      // For normal nodes, new entries should be sent ASAP
      // however for slow followers (especially when the follower is down),
      // keep sending without any wait time only ends up in high CPU load
      return TimeDuration.max(getRemainingWaitTime(), TimeDuration.ZERO).toLong(TimeUnit.MILLISECONDS);
    }
    return getHeartbeatWaitTimeMs();
  }

  private boolean isSlowFollower() {
    final TimeDuration elapsedTime = getFollower().getLastRpcResponseTime().elapsedTime();
    return elapsedTime.compareTo(getServer().properties().rpcSlownessTimeout()) > 0;
  }

  private void mayWait() {
    // use lastSend time instead of lastResponse time
    try {
      getEventAwaitForSignal().await(getWaitTimeMs() + errorWaitTimeMs(),
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      LOG.warn(this + ": Wait interrupted by " + ie);
      Thread.currentThread().interrupt();
    }
  }

  private long errorWaitTimeMs() {
    return errorRetryWaitPolicy.handleAttemptFailure(replyState::getErrorCount)
        .getSleepTime().toLong(TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<LifeCycle.State> stopAsync() {
    grpcServerMetrics.unregister();
    return super.stopAsync();
  }

  @Override
  public boolean shouldSendAppendEntries() {
    return appendLogRequestObserver == null || super.shouldSendAppendEntries();
  }

  @Override
  public boolean hasPendingDataRequests() {
    return pendingRequests.logRequestsSize() > 0;
  }

  /** @return true iff either (1) queue is full, or (2) queue is non-empty and not received first response. */
  private boolean haveTooManyPendingRequests() {
    final int size = pendingRequests.logRequestsSize();
    if (size == 0) {
      return false;
    } else if (size >= maxPendingRequestsNum) {
      return true;
    } else {
      // queue is non-empty and non-full
      return !replyState.isFirstReplyReceived();
    }
  }

  static class StreamObservers {
    private final CallStreamObserver<AppendEntriesRequestProto> appendLog;
    private final CallStreamObserver<AppendEntriesRequestProto> heartbeat;
    private final TimeDuration waitForReady;
    private volatile boolean running = true;

    StreamObservers(GrpcServerProtocolClient client, AppendLogResponseHandler handler, boolean separateHeartbeat,
        TimeDuration waitTimeMin) {
      this.appendLog = client.appendEntries(handler, false);
      this.heartbeat = separateHeartbeat? client.appendEntries(handler, true): null;
      this.waitForReady = waitTimeMin.isPositive()? waitTimeMin: TimeDuration.ONE_MILLISECOND;
    }

    void onNext(AppendEntriesRequestProto proto)
        throws InterruptedIOException {
      CallStreamObserver<AppendEntriesRequestProto> stream;
      boolean isHeartBeat = heartbeat != null && proto.getEntriesCount() == 0;
      if (isHeartBeat) {
        stream = heartbeat;
      } else {
        stream = appendLog;
      }
      // stall for stream to be ready.
      while (!stream.isReady() && running) {
        sleep(waitForReady, isHeartBeat);
      }
      stream.onNext(proto);
    }

    void stop() {
      running = false;
    }

    void onCompleted() {
      appendLog.onCompleted();
      Optional.ofNullable(heartbeat).ifPresent(StreamObserver::onCompleted);
    }
  }

  @Override
  public long getCallId() {
    return callId.get();
  }

  @Override
  public Comparator<Long> getCallIdComparator() {
    return CALL_ID_COMPARATOR;
  }

  private void appendLog(boolean heartbeat) throws IOException {
    ReferenceCountedObject<AppendEntriesRequestProto> pending = null;
    final AppendEntriesRequest request;
    try (AutoCloseableLock writeLock = lock.writeLock(caller, LOG::trace)) {
      // Prepare and send the append request.
      // Note changes on follower's nextIndex and ops on pendingRequests should always be done under the write-lock
      pending = nextAppendEntriesRequest(callId.getAndIncrement(), heartbeat);
      if (pending == null) {
        return;
      }
      request = new AppendEntriesRequest(pending.get(), getFollowerId(), grpcServerMetrics);
      pendingRequests.put(request);
      increaseNextIndex(pending.get());
      if (appendLogRequestObserver == null) {
        appendLogRequestObserver = new StreamObservers(
            getClient(), new AppendLogResponseHandler(), useSeparateHBChannel, getWaitTimeMin());
      }
    } catch(Exception e) {
      if (pending != null) {
        pending.release();
      }
      throw e;
    }

    try {
      final TimeDuration remaining = getRemainingWaitTime();
      if (remaining.isPositive()) {
        sleep(remaining, heartbeat);
      }
      if (isRunning()) {
        sendRequest(request, pending.get());
      }
    } finally {
      pending.release();
    }
  }

  private static void sleep(TimeDuration waitTime, boolean heartbeat)
      throws InterruptedIOException {
    try {
      waitTime.sleep();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(
          "Interrupted appendLog, heartbeat? " + heartbeat, e);
    }
  }

  private void sendRequest(AppendEntriesRequest request,
      AppendEntriesRequestProto proto) throws InterruptedIOException {
    CodeInjectionForTesting.execute(GrpcService.GRPC_SEND_SERVER_REQUEST,
        getServer().getId(), null, proto);
    resetHeartbeatTrigger();

    StreamObservers observers = appendLogRequestObserver;
    if (observers != null) {
      request.startRequestTimer();
      observers.onNext(proto);
      getFollower().updateLastRpcSendTime(request.isHeartbeat());
      scheduler.onTimeout(requestTimeoutDuration,
          () -> timeoutAppendRequest(request.getCallId(), request.isHeartbeat()),
          LOG, () -> "Timeout check failed for append entry request: " + request);
    }
  }

  private void timeoutAppendRequest(long cid, boolean heartbeat) {
    final AppendEntriesRequest pending = pendingRequests.handleTimeout(cid, heartbeat);
    if (pending != null) {
      final int errorCount = replyState.process(Event.TIMEOUT);
      LOG.warn("{}: Timed out {}appendEntries, errorCount={}, request={}",
          this, heartbeat ? "HEARTBEAT " : "", errorCount, pending);
      grpcServerMetrics.onRequestTimeout(getFollowerId().toString(), heartbeat);
      pending.stopRequestTimer();
    }
  }

  private void increaseNextIndex(AppendEntriesRequestProto request) {
    final int count = request.getEntriesCount();
    if (count > 0) {
      getFollower().increaseNextIndex(request.getEntries(count - 1).getIndex() + 1);
    }
  }

  private void increaseNextIndex(final long installedSnapshotIndex, Object reason) {
    final long newNextIndex = installedSnapshotIndex + 1;
    LOG.info("{}: updateNextIndex {} for {}", this, newNextIndex, reason);
    getFollower().updateNextIndex(newNextIndex);
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
        getFollower().updateLastRespondedAppendEntriesSendTime(request.getSendTime());
      }
      getFollower().updateLastRpcResponseTime();

      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: received {} reply {}, request={}",
            this, replyState.isFirstReplyReceived()? "a": "the first",
            ServerStringUtils.toAppendEntriesReplyString(reply), request);
      }

      try {
        onNextImpl(request, reply);
      } catch(Exception t) {
        LOG.error("Failed onNext request=" + request
            + ", reply=" + ServerStringUtils.toAppendEntriesReplyString(reply), t);
      }
    }

    private void onNextImpl(AppendEntriesRequest request, AppendEntriesReplyProto reply) {
      final int errorCount = replyState.process(reply.getResult());

      switch (reply.getResult()) {
        case SUCCESS:
          grpcServerMetrics.onRequestSuccess(getFollowerId().toString(), reply.getIsHearbeat());
          getLeaderState().onFollowerCommitIndex(getFollower(), reply.getFollowerCommit());
          if (getFollower().updateMatchIndex(reply.getMatchIndex())) {
            getFollower().updateNextIndex(reply.getMatchIndex() + 1);
            getLeaderState().onFollowerSuccessAppendEntries(getFollower());
          }
          break;
        case NOT_LEADER:
          grpcServerMetrics.onRequestNotLeader(getFollowerId().toString());
          LOG.warn("{}: received {} reply with term {}", this, reply.getResult(), reply.getTerm());
          if (onFollowerTerm(reply.getTerm())) {
            return;
          }
          break;
        case INCONSISTENCY:
          grpcServerMetrics.onRequestInconsistency(getFollowerId().toString());
          LOG.warn("{}: received {} reply with nextIndex {}, errorCount={}, request={}",
              this, reply.getResult(), reply.getNextIndex(), errorCount, request);
          final long requestFirstIndex = request != null? request.getFirstIndex(): RaftLog.INVALID_LOG_INDEX;
          updateNextIndex(getNextIndexForInconsistency(requestFirstIndex, reply.getNextIndex()));
          break;
        default:
          throw new IllegalStateException("Unexpected reply result: " + reply.getResult());
      }
      getLeaderState().onAppendEntriesReply(GrpcLogAppender.this, reply);
      notifyLogAppender();
    }

    /**
     * for now we simply retry the first pending request
     */
    @Override
    public void onError(Throwable t) {
      if (!isRunning()) {
        LOG.info("{} is already stopped", GrpcLogAppender.this);
        return;
      }
      BatchLogger.warn(BatchLogKey.APPEND_LOG_RESPONSE_HANDLER_ON_ERROR, AppendLogResponseHandler.this.name,
          suffix -> GrpcUtil.warn(LOG, () -> this + ": Failed appendEntries" + suffix, t),
          logMessageBatchDuration, t instanceof StatusRuntimeException);
      grpcServerMetrics.onRequestRetry(); // Update try counter
      AppendEntriesRequest request = pendingRequests.remove(GrpcUtil.getCallId(t), GrpcUtil.isHeartbeat(t));
      resetClient(request, Event.ERROR);
    }

    @Override
    public void onCompleted() {
      LOG.info("{}: follower responses appendEntries COMPLETED", this);
      resetClient(null, Event.COMPLETE);
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
        final int index;
        if (isNotificationOnly) {
          Preconditions.assertSame(InstallSnapshotRequestBodyCase.NOTIFICATION,
                  request.getInstallSnapshotRequestBodyCase(), "request case");
          index = INSTALL_SNAPSHOT_NOTIFICATION_INDEX;
        } else {
          Preconditions.assertSame(InstallSnapshotRequestBodyCase.SNAPSHOTCHUNK,
                  request.getInstallSnapshotRequestBodyCase(), "request case");
          index = request.getSnapshotChunk().getRequestIndex();
        }
        if (index == 0) {
          Preconditions.assertTrue(pending.isEmpty(), "pending queue is non-empty before offer for index 0");
        }
        pending.offer(index);
      }
    }

    void removePending(InstallSnapshotReplyProto reply) {
      try (AutoCloseableLock writeLock = lock.writeLock(caller, LOG::trace)) {
        final int index = Objects.requireNonNull(pending.poll(), "index == null");
        if (isNotificationOnly) {
          Preconditions.assertSame(InstallSnapshotReplyBodyCase.SNAPSHOTINDEX,
                  reply.getInstallSnapshotReplyBodyCase(), "reply case");
          Preconditions.assertSame(INSTALL_SNAPSHOT_NOTIFICATION_INDEX, (int) index, "poll index");
        } else {
          Preconditions.assertSame(InstallSnapshotReplyBodyCase.REQUESTINDEX,
                  reply.getInstallSnapshotReplyBodyCase(), "reply case");
          Preconditions.assertSame(reply.getRequestIndex(), (int) index, "poll index");
        }
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
      getServer().getStateMachine().event().notifySnapshotInstalled(result, snapshotIndex, getFollower().getPeer());
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
        LOG.info("{}: received {} reply {}", this, replyState.isFirstReplyReceived()? "a" : "the first",
            ServerStringUtils.toInstallSnapshotReplyString(reply));
      }

      // update the last rpc time
      getFollower().updateLastRpcResponseTime();
      replyState.process(Event.SNAPSHOT_REPLY);

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
          increaseNextIndex(followerSnapshotIndex, reply.getResult());
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
          increaseNextIndex(followerSnapshotIndex, reply.getResult());
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
          LOG.error("Unrecognized the reply result {}: Leader is {}, follower is {}",
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
      resetClient(null, Event.ERROR);
      close();
    }

    @Override
    public void onCompleted() {
      if (!isNotificationOnly || LOG.isDebugEnabled()) {
        LOG.info("{}: follower responded installSnapshot COMPLETED", this);
      }
      replyState.process(Event.COMPLETE);
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
      snapshotRequestObserver = getClient().installSnapshot(
          getFollower().getName() + "-installSnapshot-" + requestId,
          installSnapshotStreamTimeout, maxOutstandingInstallSnapshots, responseHandler);
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
      LOG.warn(this + ": failed to installSnapshot " + snapshot, e);
      if (snapshotRequestObserver != null) {
        snapshotRequestObserver.onError(e);
      }
      return;
    }

    while (isRunning() && !responseHandler.isDone()) {
      try {
        getEventAwaitForSignal().await(getWaitTimeMs(), TimeUnit.MILLISECONDS);
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
   * Send an installSnapshot notification request to the Follower.
   * @param firstAvailable the first available log's index on the Leader
   */
  private void notifyInstallSnapshot(TermIndex firstAvailable) {
    LOG.info("{}: notifyInstallSnapshot with firstAvailable={}, followerNextIndex={}",
        this, firstAvailable, getFollower().getNextIndex());

    final InstallSnapshotResponseHandler responseHandler = new InstallSnapshotResponseHandler(true);
    StreamObserver<InstallSnapshotRequestProto> snapshotRequestObserver = null;
    // prepare and enqueue the notify install snapshot request.
    final InstallSnapshotRequestProto request = newInstallSnapshotNotificationRequest(firstAvailable);
    if (LOG.isInfoEnabled()) {
      LOG.info("{}: send {}", this, ServerStringUtils.toInstallSnapshotRequestString(request));
    }
    try {
      snapshotRequestObserver = getClient().installSnapshot(getFollower().getName() + "-notifyInstallSnapshot",
          requestTimeoutDuration, 0, responseHandler);

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
        getEventAwaitForSignal().await(getWaitTimeMs(), TimeUnit.MILLISECONDS);
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
    private final Timekeeper timer;
    @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
    private volatile Timekeeper.Context timerContext;

    private final long callId;
    private final TermIndex previousLog;
    private final int entriesCount;

    private final TermIndex firstEntry;
    private final TermIndex lastEntry;
    @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
    private volatile Timestamp sendTime;

    AppendEntriesRequest(AppendEntriesRequestProto proto, RaftPeerId followerId, GrpcServerMetrics grpcServerMetrics) {
      this.callId = proto.getServerRequest().getCallId();
      this.previousLog = proto.hasPreviousLog()? TermIndex.valueOf(proto.getPreviousLog()): null;
      this.entriesCount = proto.getEntriesCount();
      this.firstEntry = entriesCount > 0? TermIndex.valueOf(proto.getEntries(0)): null;
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

    long getFirstIndex() {
      return Optional.ofNullable(firstEntry).map(TermIndex::getIndex).orElse(RaftLog.INVALID_LOG_INDEX);
    }

    Timestamp getSendTime() {
      return sendTime;
    }

    void startRequestTimer() {
      timerContext = timer.time();
      sendTime = Timestamp.currentTime();
    }

    void stopRequestTimer() {
      timerContext.stop();
    }

    boolean isHeartbeat() {
      return entriesCount == 0;
    }

    @Override
    public String toString() {
      final String entries = entriesCount == 0? ""
          : entriesCount == 1? ",entry=" + firstEntry
          : ",entries=" + firstEntry + "..." + lastEntry;
      return JavaUtils.getClassSimpleName(getClass())
          + ":cid=" + callId
          + ",entriesCount=" + entriesCount
          + entries;
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
