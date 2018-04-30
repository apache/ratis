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
package org.apache.ratis.server.impl;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerMXBean;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.ratis.server.impl.ServerProtoUtils.toRaftConfiguration;
import static org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto.AppendResult.*;
import static org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto.LogEntryBodyCase.CONFIGURATIONENTRY;
import static org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto.LogEntryBodyCase.SMLOGENTRY;
import static org.apache.ratis.util.LifeCycle.State.*;

public class RaftServerImpl implements RaftServerProtocol, RaftServerAsynchronousProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerImpl.class);

  private static final String CLASS_NAME = RaftServerImpl.class.getSimpleName();
  static final String REQUEST_VOTE = CLASS_NAME + ".requestVote";
  static final String APPEND_ENTRIES = CLASS_NAME + ".appendEntries";
  static final String INSTALL_SNAPSHOT = CLASS_NAME + ".installSnapshot";


  /** Role of raft peer */
  enum Role {
    LEADER, CANDIDATE, FOLLOWER
  }

  private final RaftServerProxy proxy;
  private final int minTimeoutMs;
  private final int maxTimeoutMs;

  private final LifeCycle lifeCycle;
  private final ServerState state;
  private final RaftGroupId groupId;
  private final Supplier<RaftPeer> peerSupplier = JavaUtils.memoize(() -> new RaftPeer(getId(), getServerRpc().getInetSocketAddress()));
  private volatile Role role;

  /** used when the peer is follower, to monitor election timeout */
  private volatile FollowerState heartbeatMonitor;

  /** used when the peer is candidate, to request votes from other peers */
  private volatile LeaderElection electionDaemon;

  /** used when the peer is leader */
  private volatile LeaderState leaderState;

  private final RetryCache retryCache;
  private final CommitInfoCache commitInfoCache = new CommitInfoCache();

  private final RaftServerJmxAdapter jmxAdapter;

  RaftServerImpl(RaftPeerId id, RaftGroup group, RaftServerProxy proxy,
      RaftProperties properties) throws IOException {
    LOG.debug("new RaftServerImpl {}, {}", id , group);
    this.groupId = group.getGroupId();
    this.lifeCycle = new LifeCycle(id);
    minTimeoutMs = RaftServerConfigKeys.Rpc.timeoutMin(properties).toInt(TimeUnit.MILLISECONDS);
    maxTimeoutMs = RaftServerConfigKeys.Rpc.timeoutMax(properties).toInt(TimeUnit.MILLISECONDS);
    Preconditions.assertTrue(maxTimeoutMs > minTimeoutMs,
        "max timeout: %s, min timeout: %s", maxTimeoutMs, minTimeoutMs);
    this.proxy = proxy;

    this.state = new ServerState(id, group, properties, this, proxy.getStateMachine());
    this.retryCache = initRetryCache(properties);

    this.jmxAdapter = new RaftServerJmxAdapter();
  }

  private RetryCache initRetryCache(RaftProperties prop) {
    final int capacity = RaftServerConfigKeys.RetryCache.capacity(prop);
    final TimeDuration expireTime = RaftServerConfigKeys.RetryCache.expiryTime(prop);
    return new RetryCache(capacity, expireTime);
  }

  LogAppender newLogAppender(
      LeaderState state, RaftPeer peer, Timestamp lastRpcTime, long nextIndex,
      boolean attendVote) {
    final FollowerInfo f = new FollowerInfo(peer, lastRpcTime, nextIndex, attendVote);
    return getProxy().getFactory().newLogAppender(this, state, f);
  }

  RaftPeer getPeer() {
    return peerSupplier.get();
  }

  int getMinTimeoutMs() {
    return minTimeoutMs;
  }

  int getMaxTimeoutMs() {
    return maxTimeoutMs;
  }

  int getRandomTimeoutMs() {
    return minTimeoutMs + ThreadLocalRandom.current().nextInt(
        maxTimeoutMs - minTimeoutMs + 1);
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  public StateMachine getStateMachine() {
    return proxy.getStateMachine();
  }

  @VisibleForTesting
  public RetryCache getRetryCache() {
    return retryCache;
  }

  public RaftServerProxy getProxy() {
    return proxy;
  }

  public RaftServerRpc getServerRpc() {
    return proxy.getServerRpc();
  }

  private void setRole(Role newRole, String op) {
    LOG.info("{} changes role from {} to {} at term {} for {}",
        getId(), this.role, newRole, state.getCurrentTerm(), op);
    this.role = newRole;
  }

  void start() {
    lifeCycle.transition(STARTING);
    state.start();
    RaftConfiguration conf = getRaftConf();
    if (conf != null && conf.contains(getId())) {
      LOG.debug("{} starts as a follower, conf={}", getId(), conf);
      startAsFollower();
    } else {
      LOG.debug("{} starts with initializing state, conf={}", getId(), conf);
      startInitializing();
    }

    registerMBean(getId(), getGroupId(), jmxAdapter, jmxAdapter);
  }

  static boolean registerMBean(
      RaftPeerId id, RaftGroupId groupdId, RaftServerMXBean mBean, JmxRegister jmx) {
    final String prefix = "Ratis:service=RaftServer,group=" + groupdId + ",id=";
    final String registered = jmx.register(mBean, Arrays.asList(
        () -> prefix + id,
        () -> prefix + ObjectName.quote(id.toString())));
    return registered != null;
  }

  /**
   * The peer belongs to the current configuration, should start as a follower
   */
  private void startAsFollower() {
    setRole(Role.FOLLOWER, "startAsFollower");
    startHeartbeatMonitor();
    lifeCycle.transition(RUNNING);
  }

  /**
   * The peer does not have any configuration (maybe it will later be included
   * in some configuration). Start still as a follower but will not vote or
   * start election.
   */
  private void startInitializing() {
    setRole(Role.FOLLOWER, "startInitializing");
    // do not start heartbeatMonitoring
  }

  public ServerState getState() {
    return state;
  }

  LeaderState getLeaderState() {
    return leaderState;
  }

  public RaftPeerId getId() {
    return getState().getSelfId();
  }

  RaftConfiguration getRaftConf() {
    return getState().getRaftConf();
  }

  void shutdown() {
    lifeCycle.checkStateAndClose(() -> {
      try {
        jmxAdapter.unregister();
      } catch (Exception ignored) {
        LOG.warn("Failed to un-register RaftServer JMX bean for " + getId(), ignored);
      }
      try {
        shutdownHeartbeatMonitor();
      } catch (Exception ignored) {
        LOG.warn("Failed to shutdown heartbeat monitor for " + getId(), ignored);
      }
      try{
        shutdownElectionDaemon();
      } catch (Exception ignored) {
        LOG.warn("Failed to shutdown election daemon for " + getId(), ignored);
      }
      try{
        shutdownLeaderState(true);
      } catch (Exception ignored) {
        LOG.warn("Failed to shutdown leader state monitor for " + getId(), ignored);
      }
      try{
        state.close();
      } catch (Exception ignored) {
        LOG.warn("Failed to close state for " + getId(), ignored);
      }
    });
  }

  public boolean isAlive() {
    return !lifeCycle.getCurrentState().isOneOf(CLOSING, CLOSED);
  }

  public boolean isFollower() {
    return role == Role.FOLLOWER;
  }

  public boolean isCandidate() {
    return role == Role.CANDIDATE;
  }

  public boolean isLeader() {
    return role == Role.LEADER;
  }

  /**
   * Change the server state to Follower if necessary
   * @param newTerm The new term.
   * @param sync We will call {@link ServerState#persistMetadata()} if this is
   *             set to true and term/votedFor get updated.
   * @return if the term/votedFor should be updated to the new term
   * @throws IOException if term/votedFor persistence failed.
   */
  synchronized boolean changeToFollower(long newTerm, boolean sync)
      throws IOException {
    final Role old = role;
    final boolean metadataUpdated = state.updateCurrentTerm(newTerm);

    if (old != Role.FOLLOWER) {
      setRole(Role.FOLLOWER, "changeToFollower");
      if (old == Role.LEADER) {
        shutdownLeaderState(false);
      } else if (old == Role.CANDIDATE) {
        shutdownElectionDaemon();
      }
      startHeartbeatMonitor();
    }

    if (metadataUpdated && sync) {
      state.persistMetadata();
    }
    return metadataUpdated;
  }

  private synchronized void shutdownLeaderState(boolean allowNull) {
    if (leaderState == null) {
      if (!allowNull) {
        throw new NullPointerException("leaderState == null");
      }
    } else {
      leaderState.stop();
      leaderState = null;
    }
    // TODO: make sure that StateMachineUpdater has applied all transactions that have context
  }

  private void shutdownElectionDaemon() {
    final LeaderElection election = electionDaemon;
    if (election != null) {
      election.stopRunning();
      // no need to interrupt the election thread
    }
    electionDaemon = null;
  }

  synchronized void changeToLeader() {
    Preconditions.assertTrue(isCandidate());
    shutdownElectionDaemon();
    setRole(Role.LEADER, "changeToLeader");
    state.becomeLeader();

    // start sending AppendEntries RPC to followers
    leaderState = new LeaderState(this, getProxy().getProperties());
    leaderState.start();
  }

  private void startHeartbeatMonitor() {
    Preconditions.assertTrue(heartbeatMonitor == null, "heartbeatMonitor != null");
    LOG.debug("{} starts heartbeatMonitor", getId());
    heartbeatMonitor = new FollowerState(this);
    heartbeatMonitor.start();
  }

  private void shutdownHeartbeatMonitor() {
    final FollowerState hm = heartbeatMonitor;
    if (hm != null) {
      hm.stopRunning();
      hm.interrupt();
    }
    heartbeatMonitor = null;
  }

  Collection<CommitInfoProto> getCommitInfos() {
    final List<CommitInfoProto> infos = new ArrayList<>();
    // add the commit info of this server
    infos.add(commitInfoCache.update(getPeer(), state.getLog().getLastCommittedIndex()));

    // add the commit infos of other servers
    if (isLeader()) {
      Optional.of(leaderState).ifPresent(
          leader -> leader.updateFollowerCommitInfos(commitInfoCache, infos));
    } else {
      getRaftConf().getPeers().stream()
          .filter(p -> !p.getId().equals(state.getSelfId()))
          .map(RaftPeer::getId)
          .map(commitInfoCache::get)
          .filter(i -> i != null)
          .forEach(infos::add);
    }
    return infos;
  }

  ServerInformationReply getServerInformation(ServerInformationRequest request) {
    final RaftGroup group = new RaftGroup(groupId, getRaftConf().getPeers());
    return new ServerInformationReply(request, getCommitInfos(), group);
  }

  synchronized void changeToCandidate() {
    Preconditions.assertTrue(isFollower());
    shutdownHeartbeatMonitor();
    setRole(Role.CANDIDATE, "changeToCandidate");
    // start election
    electionDaemon = new LeaderElection(this);
    electionDaemon.start();
  }

  @Override
  public String toString() {
    return String.format("%8s ", role) + groupId + " " + state
        + " " + lifeCycle.getCurrentState();
  }

  /**
   * @return null if the server is in leader state.
   */
  private CompletableFuture<RaftClientReply> checkLeaderState(
      RaftClientRequest request, RetryCache.CacheEntry entry) {
    try {
      assertGroup(request.getRequestorId(), request.getRaftGroupId());
    } catch (GroupMismatchException e) {
      return RetryCache.failWithException(e, entry);
    }

    if (!isLeader()) {
      NotLeaderException exception = generateNotLeaderException();
      final RaftClientReply reply = new RaftClientReply(request, exception, getCommitInfos());
      return RetryCache.failWithReply(reply, entry);
    } else if (leaderState == null || !leaderState.isReady()) {
      RetryCache.CacheEntry cacheEntry = retryCache.get(request.getClientId(), request.getCallId());
      if (cacheEntry != null && cacheEntry.isCompletedNormally()) {
        return cacheEntry.getReplyFuture();
      }
      return RetryCache.failWithException(new LeaderNotReadyException(getId()), entry);
    }
    return null;
  }

  NotLeaderException generateNotLeaderException() {
    if (lifeCycle.getCurrentState() != RUNNING) {
      return new NotLeaderException(getId(), null, null);
    }
    RaftPeerId leaderId = state.getLeaderId();
    if (leaderId == null || leaderId.equals(state.getSelfId())) {
      // No idea about who is the current leader. Or the peer is the current
      // leader, but it is about to step down
      RaftPeer suggestedLeader = getRaftConf().getRandomPeer(state.getSelfId());
      leaderId = suggestedLeader == null ? null : suggestedLeader.getId();
    }
    RaftConfiguration conf = getRaftConf();
    Collection<RaftPeer> peers = conf.getPeers();
    return new NotLeaderException(getId(), conf.getPeer(leaderId),
        peers.toArray(new RaftPeer[peers.size()]));
  }

  private void assertLifeCycleState(LifeCycle.State... expected) throws ServerNotReadyException {
    lifeCycle.assertCurrentState((n, c) -> new ServerNotReadyException("Server " + n
        + " is not " + Arrays.toString(expected) + ": current state is " + c),
        expected);
  }

  void assertGroup(Object requestorId, RaftGroupId requestorGroupId) throws GroupMismatchException {
    if (!groupId.equals(requestorGroupId)) {
      throw new GroupMismatchException(getId()
          + ": The group (" + requestorGroupId + ") of requestor " + requestorId
          + " does not match the group (" + groupId + ") of the server " + getId());
    }
  }

  /**
   * Handle a normal update request from client.
   */
  private CompletableFuture<RaftClientReply> appendTransaction(
      RaftClientRequest request, TransactionContext context,
      RetryCache.CacheEntry cacheEntry) throws IOException {
    assertLifeCycleState(RUNNING);
    CompletableFuture<RaftClientReply> reply;

    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request, cacheEntry);
      if (reply != null) {
        return reply;
      }

      // append the message to its local log
      final long entryIndex;
      try {
        entryIndex = state.applyLog(context, request.getClientId(),
            request.getCallId());
      } catch (StateMachineException e) {
        // the StateMachineException is thrown by the SM in the preAppend stage.
        // Return the exception in a RaftClientReply.
        RaftClientReply exceptionReply = new RaftClientReply(request, e, getCommitInfos());
        cacheEntry.failWithReply(exceptionReply);
        return CompletableFuture.completedFuture(exceptionReply);
      }

      // put the request into the pending queue
      pending = leaderState.addPendingRequest(entryIndex, request, context);
      leaderState.notifySenders();
    }
    return pending.getFuture();
  }

  @Override
  public CompletableFuture<RaftClientReply> submitClientRequestAsync(
      RaftClientRequest request) throws IOException {
    assertLifeCycleState(RUNNING);
    LOG.debug("{}: receive client request({})", getId(), request);
    if (request.is(RaftClientRequestProto.TypeCase.STALEREAD)) {
      return staleReadAsync(request);
    }

    // first check the server's leader state
    CompletableFuture<RaftClientReply> reply = checkLeaderState(request, null);
    if (reply != null) {
      return reply;
    }

    // let the state machine handle read-only request from client
    final StateMachine stateMachine = getStateMachine();
    if (request.is(RaftClientRequestProto.TypeCase.READ)) {
      // TODO: We might not be the leader anymore by the time this completes.
      // See the RAFT paper section 8 (last part)
      return processQueryFuture(stateMachine.query(request.getMessage()), request);
    }

    // query the retry cache
    RetryCache.CacheQueryResult previousResult = retryCache.queryCache(
        request.getClientId(), request.getCallId());
    if (previousResult.isRetry()) {
      // if the previous attempt is still pending or it succeeded, return its
      // future
      return previousResult.getEntry().getReplyFuture();
    }
    final RetryCache.CacheEntry cacheEntry = previousResult.getEntry();

    // TODO: this client request will not be added to pending requests until
    // later which means that any failure in between will leave partial state in
    // the state machine. We should call cancelTransaction() for failed requests
    TransactionContext context = stateMachine.startTransaction(request);
    if (context.getException() != null) {
      RaftClientReply exceptionReply = new RaftClientReply(request,
          new StateMachineException(getId(), context.getException()), getCommitInfos());
      cacheEntry.failWithReply(exceptionReply);
      return CompletableFuture.completedFuture(exceptionReply);
    }
    return appendTransaction(request, context, cacheEntry);
  }

  private CompletableFuture<RaftClientReply> staleReadAsync(RaftClientRequest request) {
    final long minIndex = request.getType().getStaleRead().getMinIndex();
    final long commitIndex = state.getLog().getLastCommittedIndex();
    LOG.debug("{}: minIndex={}, commitIndex={}", getId(), minIndex, commitIndex);
    if (commitIndex < minIndex) {
      final StaleReadException e = new StaleReadException(
          "Unable to serve stale-read due to server commit index = " + commitIndex + " < min = " + minIndex);
      return CompletableFuture.completedFuture(
          new RaftClientReply(request, new StateMachineException(getId(), e), getCommitInfos()));
    }
    return processQueryFuture(getStateMachine().queryStale(request.getMessage(), minIndex), request);
  }

  CompletableFuture<RaftClientReply> processQueryFuture(
      CompletableFuture<Message> queryFuture, RaftClientRequest request) {
    return queryFuture.thenApply(r -> new RaftClientReply(request, r, getCommitInfos()))
        .exceptionally(e -> {
          e = JavaUtils.unwrapCompletionException(e);
          if (e instanceof StateMachineException) {
            return new RaftClientReply(request, (StateMachineException)e, getCommitInfos());
          }
          throw new CompletionException(e);
        });
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return waitForReply(getId(), request, submitClientRequestAsync(request));
  }

  RaftClientReply waitForReply(RaftPeerId id,
      RaftClientRequest request, CompletableFuture<RaftClientReply> future)
      throws IOException {
    return waitForReply(id, request, future, e -> new RaftClientReply(request, e, getCommitInfos()));
  }

  static <REPLY extends RaftClientReply> REPLY waitForReply(
      RaftPeerId id, RaftClientRequest request, CompletableFuture<REPLY> future,
      Function<RaftException, REPLY> exceptionReply)
      throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      final String s = id + ": Interrupted when waiting for reply, request=" + request;
      LOG.info(s, e);
      throw IOUtils.toInterruptedIOException(s, e);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause == null) {
        throw new IOException(e);
      }
      if (cause instanceof NotLeaderException ||
          cause instanceof StateMachineException) {
        final REPLY reply = exceptionReply.apply((RaftException) cause);
        if (reply != null) {
          return reply;
        }
      }
      throw IOUtils.asIOException(cause);
    }
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    return waitForReply(getId(), request, setConfigurationAsync(request));
  }

  /**
   * Handle a raft configuration change request from client.
   */
  @Override
  public CompletableFuture<RaftClientReply> setConfigurationAsync(
      SetConfigurationRequest request) throws IOException {
    LOG.debug("{}: receive setConfiguration({})", getId(), request);
    assertLifeCycleState(RUNNING);
    assertGroup(request.getRequestorId(), request.getRaftGroupId());

    CompletableFuture<RaftClientReply> reply = checkLeaderState(request, null);
    if (reply != null) {
      return reply;
    }

    final RaftPeer[] peersInNewConf = request.getPeersInNewConf();
    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request, null);
      if (reply != null) {
        return reply;
      }

      final RaftConfiguration current = getRaftConf();
      // make sure there is no other raft reconfiguration in progress
      if (!current.isStable() || leaderState.inStagingState() || !state.isConfCommitted()) {
        throw new ReconfigurationInProgressException(
            "Reconfiguration is already in progress: " + current);
      }

      // return success with a null message if the new conf is the same as the current
      if (current.hasNoChange(peersInNewConf)) {
        pending = new PendingRequest(request);
        pending.setReply(new RaftClientReply(request, getCommitInfos()));
        return pending.getFuture();
      }

      // add new peers into the rpc service
      getServerRpc().addPeers(Arrays.asList(peersInNewConf));
      // add staging state into the leaderState
      pending = leaderState.startSetConfiguration(request);
    }
    return pending.getFuture();
  }

  private boolean shouldWithholdVotes(long candidateTerm) {
    if (state.getCurrentTerm() < candidateTerm) {
      return false;
    } else if (isLeader()) {
      return true;
    } else {
      return isFollower() && state.hasLeader() && heartbeatMonitor.shouldWithholdVotes();
    }
  }

  /**
   * check if the remote peer is not included in the current conf
   * and should shutdown. should shutdown if all the following stands:
   * 1. this is a leader
   * 2. current conf is stable and has been committed
   * 3. candidate id is not included in conf
   * 4. candidate's last entry's index < conf's index
   */
  private boolean shouldSendShutdown(RaftPeerId candidateId,
      TermIndex candidateLastEntry) {
    return isLeader()
        && getRaftConf().isStable()
        && getState().isConfCommitted()
        && !getRaftConf().containsInConf(candidateId)
        && candidateLastEntry.getIndex() < getRaftConf().getLogEntryIndex()
        && !leaderState.isBootStrappingPeer(candidateId);
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto r)
      throws IOException {
    final RaftRpcRequestProto request = r.getServerRequest();
    return requestVote(RaftPeerId.valueOf(request.getRequestorId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        r.getCandidateTerm(),
        ServerProtoUtils.toTermIndex(r.getCandidateLastEntry()));
  }

  private RequestVoteReplyProto requestVote(
      RaftPeerId candidateId, RaftGroupId candidateGroupId,
      long candidateTerm, TermIndex candidateLastEntry) throws IOException {
    CodeInjectionForTesting.execute(REQUEST_VOTE, getId(),
        candidateId, candidateTerm, candidateLastEntry);
    LOG.debug("{}: receive requestVote({}, {}, {}, {})",
        getId(), candidateId, candidateGroupId, candidateTerm, candidateLastEntry);
    assertLifeCycleState(RUNNING);
    assertGroup(candidateId, candidateGroupId);

    boolean voteGranted = false;
    boolean shouldShutdown = false;
    final RequestVoteReplyProto reply;
    synchronized (this) {
      if (shouldWithholdVotes(candidateTerm)) {
        LOG.info("{}-{}: Withhold vote from candidate {} with term {}. State: leader={}, term={}, lastRpcElapsed={}",
            getId(), role, candidateId, candidateTerm, state.getLeaderId(), state.getCurrentTerm(),
            isFollower()? heartbeatMonitor.getLastRpcTime().elapsedTimeMs() + "ms": null);
      } else if (state.recognizeCandidate(candidateId, candidateTerm)) {
        boolean termUpdated = changeToFollower(candidateTerm, false);
        // see Section 5.4.1 Election restriction
        if (state.isLogUpToDate(candidateLastEntry)) {
          heartbeatMonitor.updateLastRpcTime(false);
          state.grantVote(candidateId);
          voteGranted = true;
        }
        if (termUpdated || voteGranted) {
          state.persistMetadata(); // sync metafile
        }
      }
      if (!voteGranted && shouldSendShutdown(candidateId, candidateLastEntry)) {
        shouldShutdown = true;
      }
      reply = ServerProtoUtils.toRequestVoteReplyProto(candidateId, getId(),
          groupId, voteGranted, state.getCurrentTerm(), shouldShutdown);
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} replies to vote request: {}. Peer's state: {}",
            getId(), ProtoUtils.toString(reply), state);
      }
    }
    return reply;
  }

  private void validateEntries(long expectedTerm, TermIndex previous,
      LogEntryProto... entries) {
    if (entries != null && entries.length > 0) {
      final long index0 = entries[0].getIndex();

      if (previous == null || previous.getTerm() == 0) {
        Preconditions.assertTrue(index0 == 0,
            "Unexpected Index: previous is null but entries[%s].getIndex()=%s",
            0, index0);
      } else {
        Preconditions.assertTrue(previous.getIndex() == index0 - 1,
            "Unexpected Index: previous is %s but entries[%s].getIndex()=%s",
            previous, 0, index0);
      }

      for (int i = 0; i < entries.length; i++) {
        final long t = entries[i].getTerm();
        Preconditions.assertTrue(expectedTerm >= t,
            "Unexpected Term: entries[%s].getTerm()=%s but expectedTerm=%s",
            i, t, expectedTerm);

        final long indexi = entries[i].getIndex();
        Preconditions.assertTrue(indexi == index0 + i,
            "Unexpected Index: entries[%s].getIndex()=%s but entries[0].getIndex()=%s",
            i, indexi, index0);
      }
    }
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto r)
      throws IOException {
    try {
      return appendEntriesAsync(r).join();
    } catch (CompletionException e) {
      throw IOUtils.asIOException(JavaUtils.unwrapCompletionException(e));
    }
  }

  @Override
  public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(AppendEntriesRequestProto r)
      throws IOException {
    // TODO avoid converting list to array
    final RaftRpcRequestProto request = r.getServerRequest();
    final LogEntryProto[] entries = r.getEntriesList()
        .toArray(new LogEntryProto[r.getEntriesCount()]);
    final TermIndex previous = r.hasPreviousLog() ?
        ServerProtoUtils.toTermIndex(r.getPreviousLog()) : null;
    return appendEntriesAsync(RaftPeerId.valueOf(request.getRequestorId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()), r.getLeaderTerm(),
        previous, r.getLeaderCommit(), request.getCallId(), r.getInitializing(),
        r.getCommitInfosList(), entries);
  }

  static void logAppendEntries(boolean isHeartbeat, Supplier<String> message) {
    if (isHeartbeat) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("HEARTBEAT: " + message.get());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(message.get());
      }
    }
  }

  private CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(
      RaftPeerId leaderId, RaftGroupId leaderGroupId, long leaderTerm,
      TermIndex previous, long leaderCommit, long callId, boolean initializing,
      List<CommitInfoProto> commitInfos, LogEntryProto... entries) throws IOException {
    CodeInjectionForTesting.execute(APPEND_ENTRIES, getId(),
        leaderId, leaderTerm, previous, leaderCommit, initializing, entries);
    final boolean isHeartbeat = entries.length == 0;
    logAppendEntries(isHeartbeat,
        () -> getId() + ": receive appendEntries(" + leaderId + ", " + leaderGroupId + ", "
            + leaderTerm + ", " + previous + ", " + leaderCommit + ", " + initializing
            + ", commits" + ProtoUtils.toString(commitInfos)
            + ", entries: " + ServerProtoUtils.toString(entries));

    assertLifeCycleState(STARTING, RUNNING);
    assertGroup(leaderId, leaderGroupId);

    try {
      validateEntries(leaderTerm, previous, entries);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }

    final List<CompletableFuture<Long>> futures;

    final long currentTerm;
    long nextIndex = state.getLog().getNextIndex();
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final AppendEntriesReplyProto reply = ServerProtoUtils.toAppendEntriesReplyProto(
            leaderId, getId(), groupId, currentTerm, nextIndex, NOT_LEADER, callId);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Not recognize {} (term={}) as leader, state: {} reply: {}",
              getId(), leaderId, leaderTerm, state, ProtoUtils.toString(reply));
        }
        return CompletableFuture.completedFuture(reply);
      }
      changeToFollower(leaderTerm, true);
      state.setLeader(leaderId, "appendEntries");

      if (!initializing && lifeCycle.compareAndTransition(STARTING, RUNNING)) {
        startHeartbeatMonitor();
      }
      if (lifeCycle.getCurrentState() == RUNNING) {
        heartbeatMonitor.updateLastRpcTime(true);
      }

      // We need to check if "previous" is in the local peer. Note that it is
      // possible that "previous" is covered by the latest snapshot: e.g.,
      // it's possible there's no log entries outside of the latest snapshot.
      // However, it is not possible that "previous" index is smaller than the
      // last index included in snapshot. This is because indices <= snapshot's
      // last index should have been committed.
      if (previous != null && !containPrevious(previous)) {
        final AppendEntriesReplyProto reply =
            ServerProtoUtils.toAppendEntriesReplyProto(leaderId, getId(), groupId,
                currentTerm, Math.min(nextIndex, previous.getIndex()), INCONSISTENCY, callId);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: inconsistency entries. Leader previous:{}, Reply:{}",
              getId(), previous, ServerProtoUtils.toString(reply));
        }
        return CompletableFuture.completedFuture(reply);
      }

      futures = state.getLog().append(entries);

      state.updateConfiguration(entries);
      state.updateStatemachine(leaderCommit, currentTerm);

      commitInfos.stream().forEach(c -> commitInfoCache.update(c));
    }
    if (entries.length > 0) {
      CodeInjectionForTesting.execute(RaftLog.LOG_SYNC, getId(), null);
      nextIndex = entries[entries.length - 1].getIndex() + 1;
    }
    final AppendEntriesReplyProto reply = ServerProtoUtils.toAppendEntriesReplyProto(
        leaderId, getId(), groupId, currentTerm, nextIndex, SUCCESS, callId);
    logAppendEntries(isHeartbeat,
        () -> getId() + ": succeeded to handle AppendEntries. Reply: "
            + ServerProtoUtils.toString(reply));
    return JavaUtils.allOf(futures)
        .thenApply(v -> {
          // reset election timer to avoid punishing the leader for our own
          // long disk writes
          synchronized (this) {
            if (lifeCycle.getCurrentState() == RUNNING && isFollower()
                && getState().getCurrentTerm() == currentTerm) {
              // reset election timer to avoid punishing the leader for our own
              // long disk writes
              heartbeatMonitor.updateLastRpcTime(false);
            }
          }
          return reply;
        });
  }

  private boolean containPrevious(TermIndex previous) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: prev:{}, latestSnapshot:{}, latestInstalledSnapshot:{}",
          getId(), previous, state.getLatestSnapshot(), state.getLatestInstalledSnapshot());
    }
    return state.getLog().contains(previous)
        ||  (state.getLatestSnapshot() != null
             && state.getLatestSnapshot().getTermIndex().equals(previous))
        || (state.getLatestInstalledSnapshot() != null)
             && state.getLatestInstalledSnapshot().equals(previous);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    final RaftRpcRequestProto r = request.getServerRequest();
    final RaftPeerId leaderId = RaftPeerId.valueOf(r.getRequestorId());
    final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(r.getRaftGroupId());
    CodeInjectionForTesting.execute(INSTALL_SNAPSHOT, getId(),
        leaderId, request);
    LOG.debug("{}: receive installSnapshot({})", getId(), request);

    assertLifeCycleState(STARTING, RUNNING);
    assertGroup(leaderId, leaderGroupId);

    final long currentTerm;
    final long leaderTerm = request.getLeaderTerm();
    final TermIndex lastTermIndex = ServerProtoUtils.toTermIndex(
        request.getTermIndex());
    final long lastIncludedIndex = lastTermIndex.getIndex();
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final InstallSnapshotReplyProto reply = ServerProtoUtils
            .toInstallSnapshotReplyProto(leaderId, getId(), groupId, currentTerm,
                request.getRequestIndex(), InstallSnapshotResult.NOT_LEADER);
        LOG.debug("{}: do not recognize leader for installing snapshot." +
            " Reply: {}", getId(), reply);
        return reply;
      }
      changeToFollower(leaderTerm, true);
      state.setLeader(leaderId, "installSnapshot");

      if (lifeCycle.getCurrentState() == RUNNING) {
        heartbeatMonitor.updateLastRpcTime(true);
      }

      // Check and append the snapshot chunk. We simply put this in lock
      // considering a follower peer requiring a snapshot installation does not
      // have a lot of requests
      Preconditions.assertTrue(
          state.getLog().getNextIndex() <= lastIncludedIndex,
          "%s log's next id is %s, last included index in snapshot is %s",
          getId(),  state.getLog().getNextIndex(), lastIncludedIndex);

      //TODO: We should only update State with installed snapshot once the request is done.
      state.installSnapshot(request);

      // update the committed index
      // re-load the state machine if this is the last chunk
      if (request.getDone()) {
        state.reloadStateMachine(lastIncludedIndex, leaderTerm);
      }
      if (lifeCycle.getCurrentState() == RUNNING) {
        heartbeatMonitor.updateLastRpcTime(false);
      }
    }
    if (request.getDone()) {
      LOG.info("{}: successfully install the whole snapshot-{}", getId(),
          lastIncludedIndex);
    }
    return ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getId(), groupId,
        currentTerm, request.getRequestIndex(), InstallSnapshotResult.SUCCESS);
  }

  synchronized InstallSnapshotRequestProto createInstallSnapshotRequest(
      RaftPeerId targetId, String requestId, int requestIndex,
      SnapshotInfo snapshot, List<FileChunkProto> chunks, boolean done) {
    OptionalLong totalSize = snapshot.getFiles().stream()
        .mapToLong(FileInfo::getFileSize).reduce(Long::sum);
    assert totalSize.isPresent();
    return ServerProtoUtils.toInstallSnapshotRequestProto(getId(), targetId, groupId,
        requestId, requestIndex, state.getCurrentTerm(), snapshot.getTermIndex(),
        chunks, totalSize.getAsLong(), done);
  }

  synchronized RequestVoteRequestProto createRequestVoteRequest(
      RaftPeerId targetId, long term, TermIndex lastEntry) {
    return ServerProtoUtils.toRequestVoteRequestProto(getId(), targetId,
        groupId, term, lastEntry);
  }

  public synchronized void submitLocalSyncEvent() {
    if (isLeader() && leaderState != null) {
      leaderState.submitUpdateStateEvent(LeaderState.UPDATE_COMMIT_EVENT);
    }
  }

  /**
   * The log has been submitted to the state machine. Use the future to update
   * the pending requests and retry cache.
   * @param logEntry the log entry that has been submitted to the state machine
   * @param stateMachineFuture the future returned by the state machine
   *                           from which we will get transaction result later
   */
  private CompletableFuture<Message> replyPendingRequest(
      LogEntryProto logEntry, CompletableFuture<Message> stateMachineFuture) {
    // update the retry cache
    final ClientId clientId = ClientId.valueOf(logEntry.getClientId());
    final long callId = logEntry.getCallId();
    final RaftPeerId serverId = getId();
    final RetryCache.CacheEntry cacheEntry = retryCache.getOrCreateEntry(
        clientId, logEntry.getCallId());
    if (cacheEntry.isFailed()) {
      retryCache.refreshEntry(new RetryCache.CacheEntry(cacheEntry.getKey()));
    }

    return stateMachineFuture.whenComplete((reply, exception) -> {
      final RaftClientReply r;
      if (exception == null) {
        r = new RaftClientReply(clientId, serverId, groupId, callId, true, reply, null, getCommitInfos());
      } else {
        // the exception is coming from the state machine. wrap it into the
        // reply as a StateMachineException
        final StateMachineException e = new StateMachineException(getId(), exception);
        r = new RaftClientReply(clientId, serverId, groupId, callId, false, null, e, getCommitInfos());
      }
      // update retry cache
      cacheEntry.updateResult(r);
      // update pending request
      synchronized (RaftServerImpl.this) {
        if (isLeader() && leaderState != null) { // is leader and is running
          leaderState.replyPendingRequest(logEntry.getIndex(), r);
        }
      }
    });
  }

  private TransactionContext getTransactionContext(long index) {
    if (leaderState != null) { // is leader and is running
      return leaderState.getTransactionContext(index);
    }
    return null;
  }

  public synchronized long[] getFollowerNextIndices() {
    LeaderState s = this.leaderState;
    if (s == null || !isLeader()) {
      return null;
    }
    return s.getFollowerNextIndices();
  }

  CompletableFuture<Message> applyLogToStateMachine(LogEntryProto next) {
    final StateMachine stateMachine = getStateMachine();
    if (next.getLogEntryBodyCase() == CONFIGURATIONENTRY) {
      // the reply should have already been set. only need to record
      // the new conf in the state machine.
      stateMachine.setRaftConfiguration(toRaftConfiguration(next.getIndex(),
          next.getConfigurationEntry()));
    } else if (next.getLogEntryBodyCase() == SMLOGENTRY) {
      // check whether there is a TransactionContext because we are the leader.
      TransactionContext trx = getTransactionContext(next.getIndex());
      if (trx == null) {
        trx = new TransactionContextImpl(stateMachine, next);
      }

      // Let the StateMachine inject logic for committed transactions in sequential order.
      trx = stateMachine.applyTransactionSerial(trx);

      // TODO: This step can be parallelized
      CompletableFuture<Message> stateMachineFuture =
          stateMachine.applyTransaction(trx);
      return replyPendingRequest(next, stateMachineFuture);
    }
    return null;
  }

  public void failClientRequest(LogEntryProto logEntry) {
    if (logEntry.getLogEntryBodyCase() == LogEntryProto.LogEntryBodyCase.SMLOGENTRY) {
      final ClientId clientId = ClientId.valueOf(logEntry.getClientId());
      final RetryCache.CacheEntry cacheEntry = getRetryCache().get(clientId, logEntry.getCallId());
      if (cacheEntry != null) {
        final RaftClientReply reply = new RaftClientReply(clientId, getId(), getGroupId(),
            logEntry.getCallId(), false, null, generateNotLeaderException(), getCommitInfos());
        cacheEntry.failWithReply(reply);
      }
    }
  }

  private class RaftServerJmxAdapter extends JmxRegister implements RaftServerMXBean {
    @Override
    public String getId() {
      return getState().getSelfId().toString();
    }

    @Override
    public String getLeaderId() {
      return getState().getLeaderId().toString();
    }

    @Override
    public long getCurrentTerm() {
      return getState().getCurrentTerm();
    }

    @Override
    public String getGroupId() {
      return RaftServerImpl.this.getGroupId().toString();
    }

    @Override
    public String getRole() {
      return role.toString();
    }

    @Override
    public List<String> getFollowers() {
      return Optional.ofNullable(leaderState)
          .map(leader ->
              leader.getFollowers().stream()
                  .map(RaftPeer::toString)
                  .collect(Collectors.toList()))
          .orElse(Collections.emptyList());
    }
  }
}
