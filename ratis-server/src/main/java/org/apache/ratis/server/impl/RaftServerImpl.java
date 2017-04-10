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

import static org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto.AppendResult.INCONSISTENCY;
import static org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto.AppendResult.NOT_LEADER;
import static org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto.AppendResult.SUCCESS;
import static org.apache.ratis.util.LifeCycle.State.CLOSED;
import static org.apache.ratis.util.LifeCycle.State.CLOSING;
import static org.apache.ratis.util.LifeCycle.State.RUNNING;
import static org.apache.ratis.util.LifeCycle.State.STARTING;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.LeaderNotReadyException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftException;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.ReconfigurationInProgressException;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftServerImpl implements RaftServer {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerImpl.class);

  private static final String CLASS_NAME = RaftServerImpl.class.getSimpleName();
  static final String REQUEST_VOTE = CLASS_NAME + ".requestVote";
  static final String APPEND_ENTRIES = CLASS_NAME + ".appendEntries";
  static final String INSTALL_SNAPSHOT = CLASS_NAME + ".installSnapshot";


  /** Role of raft peer */
  enum Role {
    LEADER, CANDIDATE, FOLLOWER
  }

  private final int minTimeoutMs;
  private final int maxTimeoutMs;

  private final LifeCycle lifeCycle;
  private final ServerState state;
  private final StateMachine stateMachine;
  private final RaftProperties properties;
  private volatile Role role;

  /** used when the peer is follower, to monitor election timeout */
  private volatile FollowerState heartbeatMonitor;

  /** used when the peer is candidate, to request votes from other peers */
  private volatile LeaderElection electionDaemon;

  /** used when the peer is leader */
  private volatile LeaderState leaderState;

  private final RaftServerRpc serverRpc;

  private final ServerFactory factory;

  private final RetryCache retryCache;

  RaftServerImpl(RaftPeerId id, StateMachine stateMachine,
      RaftConfiguration raftConf, RaftProperties properties, Parameters parameters)
      throws IOException {
    this.lifeCycle = new LifeCycle(id);
    minTimeoutMs = RaftServerConfigKeys.Rpc.timeoutMin(properties).toInt(TimeUnit.MILLISECONDS);
    maxTimeoutMs = RaftServerConfigKeys.Rpc.timeoutMax(properties).toInt(TimeUnit.MILLISECONDS);
    Preconditions.assertTrue(maxTimeoutMs > minTimeoutMs,
        "max timeout: %s, min timeout: %s", maxTimeoutMs, minTimeoutMs);
    this.properties = properties;
    this.stateMachine = stateMachine;
    this.state = new ServerState(id, raftConf, properties, this, stateMachine);

    final RpcType rpcType = RaftConfigKeys.Rpc.type(properties);
    this.factory = ServerFactory.cast(rpcType.newFactory(properties, parameters));
    this.serverRpc = initRaftServerRpc();
    retryCache = initRetryCache(properties);
  }

  private RetryCache initRetryCache(RaftProperties prop) {
    final int capacity = RaftServerConfigKeys.RetryCache.capacity(prop);
    final TimeDuration expireTime = RaftServerConfigKeys.RetryCache.expiryTime(prop);
    return new RetryCache(capacity, expireTime);
  }

  @Override
  public RpcType getRpcType() {
    return getFactory().getRpcType();
  }

  @Override
  public ServerFactory getFactory() {
    return factory;
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

  @Override
  public StateMachine getStateMachine() {
    return this.stateMachine;
  }

  @VisibleForTesting
  RetryCache getRetryCache() {
    return retryCache;
  }

  private RaftServerRpc initRaftServerRpc() {
    final RaftServerRpc rpc = getFactory().newRaftServerRpc(this);
    // add peers into rpc service
    RaftConfiguration conf = getRaftConf();
    if (conf != null) {
      rpc.addPeers(conf.getPeers());
    }
    return rpc;
  }

  public RaftServerRpc getServerRpc() {
    return serverRpc;
  }

  @Override
  public void start() {
    lifeCycle.transition(STARTING);
    state.start();
    RaftConfiguration conf = getRaftConf();
    if (conf != null && conf.contains(getId())) {
      LOG.debug("{} starts as a follower", getId());
      startAsFollower();
    } else {
      LOG.debug("{} starts with initializing state", getId());
      startInitializing();
    }
  }

  /**
   * The peer belongs to the current configuration, should start as a follower
   */
  private void startAsFollower() {
    role = Role.FOLLOWER;
    heartbeatMonitor = new FollowerState(this);
    heartbeatMonitor.start();

    getServerRpc().start();
    lifeCycle.transition(RUNNING);
  }

  /**
   * The peer does not have any configuration (maybe it will later be included
   * in some configuration). Start still as a follower but will not vote or
   * start election.
   */
  private void startInitializing() {
    role = Role.FOLLOWER;
    // do not start heartbeatMonitoring
    getServerRpc().start();
  }

  public ServerState getState() {
    return this.state;
  }

  @Override
  public RaftPeerId getId() {
    return getState().getSelfId();
  }

  RaftConfiguration getRaftConf() {
    return getState().getRaftConf();
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> {
      try {
        shutdownHeartbeatMonitor();
        shutdownElectionDaemon();
        shutdownLeaderState();

        getServerRpc().close();
        state.close();
      } catch (Exception ignored) {
        LOG.warn("Failed to kill " + state.getSelfId(), ignored);
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
    role = Role.FOLLOWER;

    boolean metadataUpdated = false;
    if (newTerm > state.getCurrentTerm()) {
      state.setCurrentTerm(newTerm);
      state.resetLeaderAndVotedFor();
      metadataUpdated = true;
    }

    if (old == Role.LEADER) {
      assert leaderState != null;
      shutdownLeaderState();
    } else if (old == Role.CANDIDATE) {
      shutdownElectionDaemon();
    }

    if (old != Role.FOLLOWER) {
      heartbeatMonitor = new FollowerState(this);
      heartbeatMonitor.start();
    }

    if (metadataUpdated && sync) {
      state.persistMetadata();
    }
    return metadataUpdated;
  }

  private synchronized void shutdownLeaderState() {
    final LeaderState leader = leaderState;
    if (leader != null) {
      leader.stop();
    }
    leaderState = null;
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
    role = Role.LEADER;
    state.becomeLeader();
    // start sending AppendEntries RPC to followers
    leaderState = new LeaderState(this, properties);
    leaderState.start();
  }

  private void shutdownHeartbeatMonitor() {
    final FollowerState hm = heartbeatMonitor;
    if (hm != null) {
      hm.stopRunning();
      hm.interrupt();
    }
    heartbeatMonitor = null;
  }

  synchronized void changeToCandidate() {
    Preconditions.assertTrue(isFollower());
    shutdownHeartbeatMonitor();
    role = Role.CANDIDATE;
    // start election
    electionDaemon = new LeaderElection(this);
    electionDaemon.start();
  }

  @Override
  public String toString() {
    return role + " " + state + " " + lifeCycle.getCurrentState();
  }

  /**
   * @return null if the server is in leader state.
   */
  private CompletableFuture<RaftClientReply> checkLeaderState(
      RaftClientRequest request, RetryCache.CacheEntry entry) {
    if (!isLeader()) {
      NotLeaderException exception = generateNotLeaderException();
      final RaftClientReply reply = new RaftClientReply(request, exception);
      if (entry != null) {
        entry.failWithReply(reply);
      }
      return entry != null ?
          entry.getReplyFuture() : CompletableFuture.completedFuture(reply);
    } else {
      if (leaderState == null || !leaderState.isReady()) {
        final Exception e = new LeaderNotReadyException();
        if (entry != null) {
          entry.failWithException(e);
          return entry.getReplyFuture();
        } else {
          CompletableFuture<RaftClientReply> future = new CompletableFuture<>();
          future.completeExceptionally(e);
          return future;
        }
      }
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
      RaftPeer suggestedLeader = state.getRaftConf()
          .getRandomPeer(state.getSelfId());
      leaderId = suggestedLeader == null ? null : suggestedLeader.getId();
    }
    RaftConfiguration conf = getRaftConf();
    Collection<RaftPeer> peers = conf.getPeers();
    return new NotLeaderException(getId(), conf.getPeer(leaderId),
        peers.toArray(new RaftPeer[peers.size()]));
  }

  /**
   * Handle a normal update request from client.
   */
  private CompletableFuture<RaftClientReply> appendTransaction(
      RaftClientRequest request, TransactionContext context,
      RetryCache.CacheEntry cacheEntry) throws RaftException {
    LOG.debug("{}: receive client request({})", getId(), request);
    lifeCycle.assertCurrentState(RUNNING);
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
        RaftClientReply exceptionReply = new RaftClientReply(request, e);
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
    // first check the server's leader state
    CompletableFuture<RaftClientReply> reply = checkLeaderState(request, null);
    if (reply != null) {
      return reply;
    }

    // let the state machine handle read-only request from client
    if (request.isReadOnly()) {
      // TODO: We might not be the leader anymore by the time this completes.
      // See the RAFT paper section 8 (last part)
      return stateMachine.query(request);
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
    if (context.getException().isPresent()) {
      RaftClientReply exceptionReply = new RaftClientReply(request,
          new StateMachineException(getId().toString(),
              context.getException().get()));
      cacheEntry.failWithReply(exceptionReply);
      return CompletableFuture.completedFuture(exceptionReply);
    }
    return appendTransaction(request, context, cacheEntry);
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return waitForReply(getId(), request, submitClientRequestAsync(request));
  }

  private static RaftClientReply waitForReply(RaftPeerId id,
      RaftClientRequest request, CompletableFuture<RaftClientReply> future)
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
        return new RaftClientReply(request, (RaftException) cause);
      } else {
        throw IOUtils.asIOException(cause);
      }
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
    lifeCycle.assertCurrentState(RUNNING);
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
      if (!current.isStable() || leaderState.inStagingState() ||
          !state.isCurrentConfCommitted()) {
        throw new ReconfigurationInProgressException(
            "Reconfiguration is already in progress: " + current);
      }

      // return true if the new configuration is the same with the current one
      if (current.hasNoChange(peersInNewConf)) {
        pending = leaderState.returnNoConfChange(request);
        return pending.getFuture();
      }

      // add new peers into the rpc service
      getServerRpc().addPeers(Arrays.asList(peersInNewConf));
      // add staging state into the leaderState
      pending = leaderState.startSetConfiguration(request);
    }
    return pending.getFuture();
  }

  private boolean shouldWithholdVotes() {
    return isLeader() || (isFollower() && state.hasLeader()
        && heartbeatMonitor.shouldWithholdVotes());
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
    final RaftPeerId candidateId =
        new RaftPeerId(r.getServerRequest().getRequestorId());
    return requestVote(candidateId, r.getCandidateTerm(),
        ServerProtoUtils.toTermIndex(r.getCandidateLastEntry()));
  }

  private RequestVoteReplyProto requestVote(RaftPeerId candidateId,
      long candidateTerm, TermIndex candidateLastEntry) throws IOException {
    CodeInjectionForTesting.execute(REQUEST_VOTE, getId(),
        candidateId, candidateTerm, candidateLastEntry);
    LOG.debug("{}: receive requestVote({}, {}, {})",
        getId(), candidateId, candidateTerm, candidateLastEntry);
    lifeCycle.assertCurrentState(RUNNING);

    boolean voteGranted = false;
    boolean shouldShutdown = false;
    final RequestVoteReplyProto reply;
    synchronized (this) {
      if (shouldWithholdVotes()) {
        LOG.info("{} Withhold vote from server {} with term {}. " +
            "This server:{}, last rpc time from leader {} is {}", getId(),
            candidateId, candidateTerm, this, this.getState().getLeaderId(),
            (isFollower() ? heartbeatMonitor.getLastRpcTime() : -1));
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
          voteGranted, state.getCurrentTerm(), shouldShutdown);
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
    // TODO avoid converting list to array
    final LogEntryProto[] entries = r.getEntriesList()
        .toArray(new LogEntryProto[r.getEntriesCount()]);
    final TermIndex previous = r.hasPreviousLog() ?
        ServerProtoUtils.toTermIndex(r.getPreviousLog()) : null;
    return appendEntries(new RaftPeerId(r.getServerRequest().getRequestorId()),
        r.getLeaderTerm(), previous, r.getLeaderCommit(), r.getInitializing(),
        entries);
  }

  private AppendEntriesReplyProto appendEntries(RaftPeerId leaderId, long leaderTerm,
      TermIndex previous, long leaderCommit, boolean initializing,
      LogEntryProto... entries) throws IOException {
    CodeInjectionForTesting.execute(APPEND_ENTRIES, getId(),
        leaderId, leaderTerm, previous, leaderCommit, initializing,
        entries);
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: receive appendEntries({}, {}, {}, {}, {}, {})", getId(),
          leaderId, leaderTerm, previous, leaderCommit, initializing,
          ServerProtoUtils.toString(entries));
    }
    lifeCycle.assertCurrentState(STARTING, RUNNING);

    try {
      validateEntries(leaderTerm, previous, entries);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }

    final long currentTerm;
    long nextIndex = state.getLog().getNextIndex();
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final AppendEntriesReplyProto reply = ServerProtoUtils.toAppendEntriesReplyProto(
            leaderId, getId(), currentTerm, nextIndex, NOT_LEADER);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: do not recognize leader. Reply: {}",
              getId(), ProtoUtils.toString(reply));
        }
        return reply;
      }
      changeToFollower(leaderTerm, true);
      state.setLeader(leaderId);

      if (!initializing && lifeCycle.compareAndTransition(STARTING, RUNNING)) {
        heartbeatMonitor = new FollowerState(this);
        heartbeatMonitor.start();
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
            ServerProtoUtils.toAppendEntriesReplyProto(leaderId, getId(),
                currentTerm, Math.min(nextIndex, previous.getIndex()), INCONSISTENCY);
        LOG.debug("{}: inconsistency entries. Leader previous:{}, Reply:{}",
            getId(), previous, ServerProtoUtils.toString(reply));
        return reply;
      }

      state.getLog().append(entries);
      state.updateConfiguration(entries);
      state.updateStatemachine(leaderCommit, currentTerm);
    }
    if (entries != null && entries.length > 0) {
      try {
        state.getLog().logSync();
      } catch (InterruptedException e) {
        throw new InterruptedIOException("logSync got interrupted");
      }
      nextIndex = entries[entries.length - 1].getIndex() + 1;
    }
    synchronized (this) {
      if (lifeCycle.getCurrentState() == RUNNING && isFollower()
          && getState().getCurrentTerm() == currentTerm) {
        // reset election timer to avoid punishing the leader for our own
        // long disk writes
        heartbeatMonitor.updateLastRpcTime(false);
      }
    }
    final AppendEntriesReplyProto reply = ServerProtoUtils.toAppendEntriesReplyProto(
        leaderId, getId(), currentTerm, nextIndex, SUCCESS);
    LOG.debug("{}: succeeded to handle AppendEntries. Reply: {}", getId(),
        ServerProtoUtils.toString(reply));
    return reply;
  }

  private boolean containPrevious(TermIndex previous) {
    LOG.debug("{}: prev:{}, latestSnapshot:{}, getLatestInstalledSnapshot:{}",
        getId(), previous, state.getLatestSnapshot(), state.getLatestInstalledSnapshot());
    return state.getLog().contains(previous)
        ||  (state.getLatestSnapshot() != null
             && state.getLatestSnapshot().getTermIndex().equals(previous))
        || (state.getLatestInstalledSnapshot() != null)
             && state.getLatestInstalledSnapshot().equals(previous);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    final RaftPeerId leaderId =
        new RaftPeerId(request.getServerRequest().getRequestorId());
    CodeInjectionForTesting.execute(INSTALL_SNAPSHOT, getId(),
        leaderId, request);
    LOG.debug("{}: receive installSnapshot({})", getId(), request);

    lifeCycle.assertCurrentState(STARTING, RUNNING);

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
            .toInstallSnapshotReplyProto(leaderId, getId(), currentTerm,
                request.getRequestIndex(), InstallSnapshotResult.NOT_LEADER);
        LOG.debug("{}: do not recognize leader for installing snapshot." +
            " Reply: {}", getId(), reply);
        return reply;
      }
      changeToFollower(leaderTerm, true);
      state.setLeader(leaderId);

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
    return ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getId(),
        currentTerm, request.getRequestIndex(), InstallSnapshotResult.SUCCESS);
  }

  AppendEntriesRequestProto createAppendEntriesRequest(long leaderTerm,
      RaftPeerId targetId, TermIndex previous, List<LogEntryProto> entries,
      boolean initializing) {
    return ServerProtoUtils.toAppendEntriesRequestProto(getId(), targetId,
        leaderTerm, entries, state.getLog().getLastCommittedIndex(),
        initializing, previous);
  }

  synchronized InstallSnapshotRequestProto createInstallSnapshotRequest(
      RaftPeerId targetId, String requestId, int requestIndex,
      SnapshotInfo snapshot, List<FileChunkProto> chunks, boolean done) {
    OptionalLong totalSize = snapshot.getFiles().stream()
        .mapToLong(FileInfo::getFileSize).reduce(Long::sum);
    assert totalSize.isPresent();
    return ServerProtoUtils.toInstallSnapshotRequestProto(getId(), targetId,
        requestId, requestIndex, state.getCurrentTerm(), snapshot.getTermIndex(),
        chunks, totalSize.getAsLong(), done);
  }

  synchronized RequestVoteRequestProto createRequestVoteRequest(
      RaftPeerId targetId, long term, TermIndex lastEntry) {
    return ServerProtoUtils.toRequestVoteRequestProto(getId(), targetId, term,
        lastEntry);
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
  void replyPendingRequest(LogEntryProto logEntry,
      CompletableFuture<Message> stateMachineFuture) {
    // update the retry cache
    final ClientId clientId = new ClientId(logEntry.getClientId().toByteArray());
    final long callId = logEntry.getCallId();
    final RaftPeerId serverId = getId();
    final RetryCache.CacheEntry cacheEntry = retryCache.getOrCreateEntry(
        clientId, logEntry.getCallId());
    stateMachineFuture.whenComplete((reply, exception) -> {
      final RaftClientReply r;
      if (exception == null) {
        r = new RaftClientReply(clientId, serverId, callId, true, reply, null);
      } else {
        // the exception is coming from the state machine. wrap it into the
        // reply as a StateMachineException
        final StateMachineException e = new StateMachineException(
            getId().toString(), exception);
        r = new RaftClientReply(clientId, serverId, callId, false, null, e);
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

  TransactionContext getTransactionContext(long index) {
    if (leaderState != null) { // is leader and is running
      return leaderState.getTransactionContext(index);
    }
    return null;
  }

  @Override
  public RaftProperties getProperties() {
    return this.properties;
  }
}
