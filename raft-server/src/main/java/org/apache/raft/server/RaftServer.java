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
package org.apache.raft.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Time;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotResult;
import org.apache.raft.protocol.*;
import org.apache.raft.server.protocol.*;
import org.apache.raft.server.protocol.AppendEntriesReply.AppendResult;
import org.apache.raft.statemachine.SnapshotInfo;
import org.apache.raft.statemachine.TrxContext;
import org.apache.raft.util.CodeInjectionForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.raft.server.LeaderState.UPDATE_COMMIT_EVENT;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RaftServer implements RaftServerProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  private static final String CLASS_NAME = RaftServer.class.getSimpleName();
  static final String REQUEST_VOTE = CLASS_NAME + ".requestVote";
  static final String APPEND_ENTRIES = CLASS_NAME + ".appendEntries";
  static final String INSTALL_SNAPSHOT = CLASS_NAME + ".installSnapshot";

  private enum RunningState {
    /**
     * the peer does not belong to any configuration yet, need to catchup
     */
    INITIALIZING,
    RUNNING,
    STOPPED
  }

  public final int minTimeout;
  public final int maxTimeout;

  private final ServerState state;
  private final StateMachine stateMachine;
  private final RaftProperties properties;
  private volatile Role role;
  private final AtomicReference<RunningState> runningState
      = new AtomicReference<>(RunningState.INITIALIZING);

  /** used when the peer is follower, to monitor election timeout */
  private volatile FollowerState heartbeatMonitor;

  /** used when the peer is candidate, to request votes from other peers */
  private volatile LeaderElection electionDaemon;

  /** used when the peer is leader */
  private volatile LeaderState leaderState;

  private RaftServerRpc serverRpc;

  public RaftServer(String id, RaftConfiguration raftConf,
      RaftProperties properties, StateMachine stateMachine) throws IOException {
    minTimeout = properties.getInt(
        RaftServerConfigKeys.RAFT_SERVER_RPC_TIMEOUT_MIN_MS_KEY,
        RaftServerConfigKeys.RAFT_SERVER_RPC_TIMEOUT_MIN_MS_DEFAULT);
    maxTimeout = properties.getInt(
        RaftServerConfigKeys.RAFT_SERVER_RPC_TIMEOUT_MAX_MS_KEY,
        RaftServerConfigKeys.RAFT_SERVER_RPC_TIMEOUT_MAX_MS_DEFAULT);
    Preconditions.checkArgument(maxTimeout > minTimeout,
        "max timeout: %s, min timeout: %s", maxTimeout, minTimeout);
    this.properties = properties;
    this.stateMachine = stateMachine;
    this.state = new ServerState(id, raftConf, properties, this, stateMachine);
  }

  public StateMachine getStateMachine() {
    return this.stateMachine;
  }

  /**
   * Used by tests to set initial raft configuration with correct port bindings.
   */
  @VisibleForTesting
  public void setInitialConf(RaftConfiguration conf) {
    this.state.setInitialConf(conf);
  }

  public void setServerRpc(RaftServerRpc serverRpc) {
    this.serverRpc = serverRpc;
    // add peers into rpc service
    RaftConfiguration conf = getRaftConf();
    if (conf != null) {
      addPeersToRPC(conf.getPeers());
    }
  }

  public RaftServerRpc getServerRpc() {
    return serverRpc;
  }

  public void start() {
    state.start();
    RaftConfiguration conf = getRaftConf();
    if (conf != null && conf.contains(getId())) {
      startAsFollower();
    } else {
      startInitializing();
    }
  }

  private void changeRunningState(RunningState oldState, RunningState newState) {
    final boolean changed = runningState.compareAndSet(oldState, newState);
    Preconditions.checkState(changed);
  }

  private void changeRunningState(RunningState newState) {
    runningState.set(newState);
  }

  /**
   * The peer belongs to the current configuration, should start as a follower
   */
  private void startAsFollower() {
    changeRunningState(RunningState.INITIALIZING, RunningState.RUNNING);

    role = Role.FOLLOWER;
    heartbeatMonitor = new FollowerState(this);
    heartbeatMonitor.start();

    serverRpc.start();
  }

  /**
   * The peer does not have any configuration (maybe it will later be included
   * in some configuration). Start still as a follower but will not vote or
   * start election.
   */
  private void startInitializing() {
    role = Role.FOLLOWER;
    // do not start heartbeatMonitoring
    serverRpc.start();
  }

  public ServerState getState() {
    return this.state;
  }

  public String getId() {
    return getState().getSelfId();
  }

  public RaftConfiguration getRaftConf() {
    return getState().getRaftConf();
  }

  public boolean isRunning() {
    return runningState.get() != RunningState.STOPPED;
  }

  public void kill() {
    changeRunningState(RunningState.STOPPED);

    try {
      serverRpc.interruptAndJoin();

      shutdownHeartbeatMonitor();
      shutdownElectionDaemon();
      shutdownLeaderState();

      serverRpc.shutdown();
      state.close();
    } catch (Exception ignored) {
    }
  }

  private void assertRunningState(RunningState... allowedStates)
      throws RaftException {
    if (!Arrays.asList(allowedStates).contains(runningState.get())) {
      throw new RaftException(getId() + " is not running.");
    }
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

  Role getRole() {
    return role;
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
    Preconditions.checkState(isCandidate());
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
    Preconditions.checkState(isFollower());
    shutdownHeartbeatMonitor();
    role = Role.CANDIDATE;
    // start election
    electionDaemon = new LeaderElection(this);
    electionDaemon.start();
  }

  @Override
  public String toString() {
    return role + " " + state + " " + runningState;
  }

  /**
   * @return null if the server is in leader state.
   */
  CompletableFuture<RaftClientReply> checkLeaderState(
      RaftClientRequest request) {
    if (!isLeader()) {
      NotLeaderException exception = generateNotLeaderException();
      CompletableFuture<RaftClientReply> future = new CompletableFuture<>();
      future.complete(new RaftClientReply(request, exception));
      return future;
    }
    return null;
  }

  NotLeaderException generateNotLeaderException() {
    if (runningState.get() != RunningState.RUNNING) {
      return new NotLeaderException(getId(), null, RaftPeer.EMPTY_PEERS);
    }
    String leaderId = state.getLeaderId();
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
  public CompletableFuture<RaftClientReply> appendTransaction(
      RaftClientRequest request, TrxContext entry)
      throws RaftException {
    LOG.debug("{}: receive client request({})", getId(), request);
    assertRunningState(RunningState.RUNNING);
    CompletableFuture<RaftClientReply> reply;

    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request);
      if (reply != null) {
        return reply;
      }

      // append the message to its local log
      final long entryIndex = state.applyLog(entry);

      // put the request into the pending queue
      pending = leaderState.addPendingRequest(entryIndex, request, entry);
      leaderState.notifySenders();
    }
    return pending.getFuture();
  }

  /**
   * Handle a raft configuration change request from client.
   */
  public CompletableFuture<RaftClientReply> setConfiguration(
      SetConfigurationRequest request) throws IOException {
    LOG.debug("{}: receive setConfiguration({})", getId(), request);
    assertRunningState(RunningState.RUNNING);
    CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    final RaftPeer[] peersInNewConf = request.getPeersInNewConf();
    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request);
      if (reply != null) {
        return reply;
      }

      final RaftConfiguration current = getRaftConf();
      // make sure there is no other raft reconfiguration in progress
      if (!current.inStableState() || leaderState.inStagingState() ||
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
      addPeersToRPC(Arrays.asList(peersInNewConf));
      // add staging state into the leaderState
      pending = leaderState.startSetConfiguration(request);
    }
    return pending.getFuture();
  }

  private boolean shouldWithholdVotes(long now) {
    return isLeader() || (isFollower() && state.hasLeader()
        && heartbeatMonitor.shouldWithholdVotes(now));
  }

  /**
   * check if the remote peer is not included in the current conf
   * and should shutdown. should shutdown if all the following stands:
   * 1. this is a leader
   * 2. current conf is stable and has been committed
   * 3. candidate id is not included in conf
   * 4. candidate's last entry's index < conf's index
   */
  private boolean shouldSendShutdown(String candidateId,
      TermIndex candidateLastEntry) {
    return isLeader()
        && getRaftConf().inStableState()
        && getState().isConfCommitted()
        && !getRaftConf().containsInConf(candidateId)
        && candidateLastEntry.getIndex() < getRaftConf().getLogEntryIndex()
        && !leaderState.isBootStrappingPeer(candidateId);
  }

  @Override
  public RequestVoteReply requestVote(RequestVoteRequest r) throws IOException {
    return requestVote(r.getCandidateId(), r.getCandidateTerm(),
        r.getCandidateLastEntry());
  }

  private RequestVoteReply requestVote(String candidateId, long candidateTerm,
      TermIndex candidateLastEntry) throws IOException {
    CodeInjectionForTesting.execute(REQUEST_VOTE, getId(),
        candidateId, candidateTerm, candidateLastEntry);
    LOG.debug("{}: receive requestVote({}, {}, {})",
        getId(), candidateId, candidateTerm, candidateLastEntry);
    assertRunningState(RunningState.RUNNING);

    final long startTime = Time.monotonicNow();
    boolean voteGranted = false;
    boolean shouldShutdown = false;
    final RequestVoteReply reply;
    synchronized (this) {
      if (shouldWithholdVotes(startTime)) {
        LOG.info("{} Withhold vote from server {} with term {}. " +
            "This server:{}, last rpc time from leader {} is {}", getId(),
            candidateId, candidateTerm, this, this.getState().getLeaderId(),
            (isFollower() ? heartbeatMonitor.getLastRpcTime() : -1));
      } else if (state.recognizeCandidate(candidateId, candidateTerm)) {
        boolean termUpdated = changeToFollower(candidateTerm, false);
        // see Section 5.4.1 Election restriction
        if (state.isLogUpToDate(candidateLastEntry)) {
          heartbeatMonitor.updateLastRpcTime(startTime, false);
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
      reply = new RequestVoteReply(candidateId, getId(),
          state.getCurrentTerm(), voteGranted, shouldShutdown);
      LOG.debug("{} replies to vote request: {}. Peer's state: {}",
          getId(), reply, state);
    }
    return reply;
  }

  private void validateEntries(long expectedTerm, TermIndex previous,
      LogEntryProto... entries) {
    if (entries != null && entries.length > 0) {
      final long index0 = entries[0].getIndex();

      if (previous == null || previous.getTerm() == 0) {
        Preconditions.checkArgument(index0 == 0,
            "Unexpected Index: previous is null but entries[%s].getIndex()=%s",
            0, index0);
      } else {
        Preconditions.checkArgument(previous.getIndex() == index0 - 1,
            "Unexpected Index: previous is %s but entries[%s].getIndex()=%s",
            previous, 0, index0);
      }

      for (int i = 0; i < entries.length; i++) {
        final long t = entries[i].getTerm();
        Preconditions.checkArgument(expectedTerm >= t,
            "Unexpected Term: entries[%s].getTerm()=%s but expectedTerm=%s",
            i, t, expectedTerm);

        final long indexi = entries[i].getIndex();
        Preconditions.checkArgument(indexi == index0 + i,
            "Unexpected Index: entries[%s].getIndex()=%s but entries[0].getIndex()=%s",
            i, indexi, index0);
      }
    }
  }

  @Override
  public AppendEntriesReply appendEntries(AppendEntriesRequest r) throws IOException {
    return appendEntries(r.getLeaderId(), r.getLeaderTerm(),
        r.getPreviousLog(), r.getLeaderCommit(), r.isInitializing(),
        r.getEntries());
  }

  private AppendEntriesReply appendEntries(String leaderId, long leaderTerm,
      TermIndex previous, long leaderCommit, boolean initializing,
      LogEntryProto... entries) throws IOException {
    CodeInjectionForTesting.execute(APPEND_ENTRIES, getId(),
        leaderId, leaderTerm, previous, leaderCommit, initializing, entries);
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: receive appendEntries({}, {}, {}, {}, {})", getId(),
          leaderId, leaderTerm, previous, leaderCommit,
          ServerProtoUtils.toString(entries));
    }
    assertRunningState(RunningState.RUNNING, RunningState.INITIALIZING);

    try {
      validateEntries(leaderTerm, previous, entries);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }

    final long currentTerm;
    final long nextIndex;
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      nextIndex = state.getLog().getNextIndex();
      if (!recognized) {
        final AppendEntriesReply reply = new AppendEntriesReply(leaderId,
            getId(), currentTerm, nextIndex, AppendResult.NOT_LEADER);
        LOG.debug("{}: do not recognize leader. Reply: {}", getId(), reply);
        return reply;
      }
      changeToFollower(leaderTerm, true);
      state.setLeader(leaderId);

      if (runningState.get() == RunningState.INITIALIZING && !initializing) {
        changeRunningState(RunningState.INITIALIZING, RunningState.RUNNING);
        heartbeatMonitor = new FollowerState(this);
        heartbeatMonitor.start();
      }
      if (runningState.get() == RunningState.RUNNING) {
        heartbeatMonitor.updateLastRpcTime(Time.monotonicNow(), true);
      }

      // We need to check if "previous" is in the local peer. Note that it is
      // possible that "previous" is covered by the latest snapshot: e.g.,
      // it's possible there's no log entries outside of the latest snapshot.
      // However, it is not possible that "previous" index is smaller than the
      // last index included in snapshot. This is because indices <= snapshot's
      // last index should have been committed.
      if (previous != null && !containPrevious(previous)) {
        final AppendEntriesReply reply =  new AppendEntriesReply(leaderId,
            getId(), currentTerm, nextIndex, AppendResult.INCONSISTENCY);
        LOG.debug("{}: inconsistency entries. Previous:{} Reply: {}", getId(), previous, reply);
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
    }
    synchronized (this) {
      if (runningState.get() == RunningState.RUNNING && isFollower()
          && getState().getCurrentTerm() == currentTerm) {
        // reset election timer to avoid punishing the leader for our own
        // long disk writes
        heartbeatMonitor.updateLastRpcTime(Time.monotonicNow(), false);
      }
    }
    final AppendEntriesReply reply = new AppendEntriesReply(leaderId, getId(),
        currentTerm, nextIndex, AppendResult.SUCCESS);
    LOG.debug("{}: succeeded to append entries. Reply: {}", getId(), reply);
    return reply;
  }

  private boolean containPrevious(TermIndex previous) {
    LOG.debug("prev:{}, latestSnapshot:{}, getLatestInstalledSnapshot:{}",
        previous, state.getLatestSnapshot(), state.getLatestInstalledSnapshot());
    return state.getLog().contains(previous)
        ||  (state.getLatestSnapshot() != null
             && state.getLatestSnapshot().getTermIndex().equals(previous))
        || (state.getLatestInstalledSnapshot() != null)
             && state.getLatestInstalledSnapshot().equals(previous);
  }

  @Override
  public InstallSnapshotReply installSnapshot(InstallSnapshotRequest request)
      throws IOException {
    CodeInjectionForTesting.execute(INSTALL_SNAPSHOT, getId(),
        request.getRequestorId(), request);
    LOG.debug("{}: receive installSnapshot({})", getId(), request);

    assertRunningState(RunningState.RUNNING, RunningState.INITIALIZING);

    final long currentTerm;
    final String leaderId = request.getRequestorId();
    final long leaderTerm = request.getLeaderTerm();
    final long lastIncludedIndex = request.getLastIncludedIndex();
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final InstallSnapshotReply reply = new InstallSnapshotReply(leaderId,
            getId(), currentTerm, InstallSnapshotResult.NOT_LEADER);
        LOG.debug("{}: do not recognize leader for installing snapshot." +
            " Reply: {}", getId(), reply);
        return reply;
      }
      changeToFollower(leaderTerm, true);
      state.setLeader(leaderId);

      if (runningState.get() == RunningState.RUNNING) {
        heartbeatMonitor.updateLastRpcTime(Time.monotonicNow(), true);
      }

      // Check and append the snapshot chunk. We simply put this in lock
      // considering a follower peer requiring a snapshot installation does not
      // have a lot of requests
      Preconditions.checkState(
          state.getLog().getNextIndex() <= lastIncludedIndex,
          "%s log's next id is %s, last included index in snapshot is %s",
          getId(),  state.getLog().getNextIndex(), lastIncludedIndex);

      //TODO: We should only update State with installed snapshot once the request is done.
      state.installSnapshot(request);

      // update the committed index
      // re-load the state machine if this is the last chunk
      if (request.isDone()) {
        state.reloadStateMachine(lastIncludedIndex, leaderTerm);
      }
      if (runningState.get() == RunningState.RUNNING) {
        heartbeatMonitor.updateLastRpcTime(Time.monotonicNow(), false);
      }
    }
    if (request.isDone()) {
      LOG.info("{}: successfully install the whole snapshot-{}", getId(),
          lastIncludedIndex);
    }
    return new InstallSnapshotReply(leaderId, getId(), currentTerm,
        InstallSnapshotResult.SUCCESS);
  }

  synchronized AppendEntriesRequest createAppendEntriesRequest(String targetId,
      TermIndex previous, LogEntryProto[] entries, boolean initializing) {
    return new AppendEntriesRequest(getId(), targetId,
        state.getCurrentTerm(), previous, entries,
        state.getLog().getLastCommittedIndex(), initializing);
  }

  synchronized InstallSnapshotRequest createInstallSnapshotRequest(String targetId,
      String requestId, int requestIndex, SnapshotInfo snapshot,
      List<RaftProtos.FileChunkProto> chunks, boolean done) {
    return new InstallSnapshotRequest(getId(), targetId, requestId, requestIndex,
        state.getCurrentTerm(), snapshot, chunks, done);
  }

  synchronized RequestVoteRequest createRequestVoteRequest(String targetId,
      long term, TermIndex lastEntry) {
    return new RequestVoteRequest(getId(), targetId, term, lastEntry);
  }

  public synchronized void submitLocalSyncEvent() {
    if (isLeader() && leaderState != null) {
      leaderState.submitUpdateStateEvent(UPDATE_COMMIT_EVENT);
    }
  }

  public void addPeersToRPC(Iterable<RaftPeer> peers) {
    serverRpc.addPeerProxies(peers);
  }

  synchronized void replyPendingRequest(long logIndex,
      CompletableFuture<Message> message) {
    if (isLeader() && leaderState != null) { // is leader and is running
      leaderState.replyPendingRequest(logIndex, message);
    }
  }

  TrxContext getTransactionContext(long index) {
    if (leaderState != null) { // is leader and is running
      return leaderState.getTransactionContext(index);
    }
    return null;
  }
}
