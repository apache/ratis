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
package org.apache.hadoop.raft.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.protocol.*;
import org.apache.hadoop.raft.server.protocol.*;
import org.apache.hadoop.raft.server.protocol.AppendEntriesReply.AppendResult;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RaftServer implements RaftServerProtocol, RaftClientProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  enum RunningState {
    /**
     * the peer does not belong to any configuration yet, need to catchup
     */
    INITIALIZING,
    RUNNING,
    STOPPED
  }

  private final ServerState state;
  private volatile Role role;
  private final AtomicReference<RunningState> runningState
      = new AtomicReference<>(RunningState.INITIALIZING);

  /** used when the peer is follower, to monitor election timeout */
  private volatile FollowerState heartbeatMonitor;

  /** used when the peer is candidate, to request votes from other peers */
  private volatile LeaderElection electionDaemon;

  /** used when the peer is leader */
  private volatile LeaderState leaderState;

  private RaftCommunicationSystem communicationSystem;

  public RaftServer(String id, RaftConfiguration raftConf) {
    this(new ServerState(id, raftConf));
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public RaftServer(ServerState initialState) {
    this.state = initialState;
  }

  public void setCommunicationSystem(RaftCommunicationSystem communicationSystem) {
    this.communicationSystem = communicationSystem;
  }

  RaftCommunicationSystem getCommunicationSystem() {
    return communicationSystem;
  }

  public void start(RaftConfiguration conf) {
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

    communicationSystem.start();
  }

  /**
   * The peer does not have any configuration (maybe it will later be included
   * in some configuration). Start still as a follower but will not vote or
   * start election.
   */
  private void startInitializing() {
    role = Role.FOLLOWER;
    // do not start heartbeatMonitoring
    communicationSystem.start();
  }

  public ServerState getState() {
    return this.state;
  }

  public String getId() {
    return getState().getSelfId();
  }

  public RaftConfiguration getRaftConf() {
    return this.getState().getRaftConf();
  }

  public boolean isRunning() {
    return runningState.get() != RunningState.STOPPED;
  }

  public void kill() {
    changeRunningState(RunningState.STOPPED);

    try {
      communicationSystem.interruptAndJoin();

      shutdownHeartbeatMonitor();
      shutdownElectionDaemon();
      shutdownLeaderState();

      communicationSystem.shutdown();
    } catch (InterruptedException ignored) {
    }
  }

  void assertRunningState(RunningState... allowedStates) throws IOException {
    if (!Arrays.asList(allowedStates).contains(runningState.get())) {
      throw new IOException(getId() + " is not running.");
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

  synchronized void changeToFollower(long newTerm) {
    if (newTerm > state.getCurrentTerm()) {
      state.setCurrentTerm(newTerm);
      state.resetLeaderAndVotedFor();
    }
    if (isFollower()) {
      return;
    } else if (isLeader()) {
      assert leaderState != null;
      shutdownLeaderState();
    } else if (isCandidate()) {
      shutdownElectionDaemon();
    }
    role = Role.FOLLOWER;
    heartbeatMonitor = new FollowerState(this);
    heartbeatMonitor.start();
  }

  private synchronized void shutdownLeaderState() {
    final LeaderState leader = leaderState;
    if (leader != null) {
      leader.stop();
    }
    leaderState = null;
  }

  private void shutdownElectionDaemon() {
    final LeaderElection election = electionDaemon;
    if (election != null) {
      election.stopRunning();
      election.interrupt();
    }
    electionDaemon = null;
  }

  synchronized void changeToLeader() {
    Preconditions.checkState(isCandidate());
    shutdownElectionDaemon();
    role = Role.LEADER;
    state.becomeLeader();
    // start sending AppendEntries RPC to followers
    leaderState = new LeaderState(this);
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

  private void checkLeaderState() throws NotLeaderException {
    if (!isLeader()) {
      throw generateNotLeaderException();
    }
  }

  NotLeaderException generateNotLeaderException() {
    if (runningState.get() != RunningState.RUNNING) {
      return new NotLeaderException(getId(), null,
          NotLeaderException.EMPTY_PEERS);
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
   * Handle a request from client.
   */
  @Override
  public void submitClientRequest(RaftClientRequest request) throws IOException {
    LOG.debug("{}: receive submit({})", getId(), request);
    assertRunningState(RunningState.RUNNING);

    synchronized (this) {
      checkLeaderState();
      // append the message to its local log
      final long entryIndex = state.applyLog(request.getMessage());

      // put the request into the pending queue
      leaderState.addPendingRequest(entryIndex, request);
      leaderState.notifySenders();
    }
  }

  @Override
  public void setConfiguration(SetConfigurationRequest request)
      throws IOException {
    LOG.debug("{}: receive setConfiguration({})", getId(), request);
    assertRunningState(RunningState.RUNNING);

    final RaftPeer[] peersInNewConf = request.getPeersInNewConf();
    synchronized (this) {
      checkLeaderState();
      final RaftConfiguration current = getRaftConf();
      // make sure there is no other raft reconfiguration in progress
      if (!current.inStableState() || leaderState.inStagingState()) {
        throw new ReconfigurationInProgressException(current,
            "Reconfiguration in progress");
      }

      // return true if the new configuration is the same with the current one
      if (current.hasNoChange(peersInNewConf)) {
        leaderState.returnNoConfChange(request);
        return;
      }
      // add staging state into the leaderState
      leaderState.startSetConfiguration(request);
    }
    // release the handler and the LeaderState thread will trigger the next step
    // once the (old, new) entry is committed, and finally send the response
    // back the client.
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
    return requestVote(r.getCandidateId(), r.getCandidateTerm(), r.getCandidateLastEntry());
  }

  private RequestVoteReply requestVote(String candidateId, long candidateTerm,
      TermIndex candidateLastEntry) throws IOException {
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
        changeToFollower(candidateTerm);

        // see Section 5.4.1 Election restriction
        if (state.isLogUpToDate(candidateLastEntry)) {
          heartbeatMonitor.updateLastRpcTime(startTime);
          state.grantVote(candidateId);
          voteGranted = true;
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
    try {
      state.getLog().logSync(0); // TODO sync metafile instead
    } catch (InterruptedException e) {
      throw new InterruptedIOException("logSync got interrupted");
    }
    return reply;
  }

  private void validateEntries(long expectedTerm, TermIndex previous,
      LogEntryProto... entries) {
    if (entries != null && entries.length > 0) {
      final long index0 = entries[0].getIndex();

      if (previous == null) {
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
    LOG.debug("{}: receive appendEntries({}, {}, {}, {}, {})",
        getId(), leaderId, leaderTerm, previous, leaderCommit,
        (entries == null || entries.length == 0) ? "HEARTBEAT"
            : entries.length == 1? entries[0]: Arrays.asList(entries));
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
      changeToFollower(leaderTerm);
      state.setLeader(leaderId);

      if (runningState.get() == RunningState.INITIALIZING && !initializing) {
        changeRunningState(RunningState.INITIALIZING, RunningState.RUNNING);
        heartbeatMonitor = new FollowerState(this);
        heartbeatMonitor.start();
      }

      if (runningState.get() == RunningState.RUNNING) {
        heartbeatMonitor.updateLastRpcTime(Time.monotonicNow());
      }
      state.getLog().updateLastCommitted(leaderCommit, currentTerm);

      if (previous != null && !state.getLog().contains(previous)) {
        // TODO add more unit tests for inconsistency scenarios
        final AppendEntriesReply reply =  new AppendEntriesReply(leaderId,
            getId(), currentTerm, nextIndex, AppendResult.INCONSISTENCY);
        LOG.debug("{}: inconsistency entries. Reply: {}", getId(), reply);
        return reply;
      }

      state.getLog().append(entries);
      state.updateConfiguration(entries);
    }
    if (entries != null && entries.length > 0) {
      try {
        state.getLog().logSync(entries[entries.length - 1].getIndex());
      } catch (InterruptedException e) {
        throw new InterruptedIOException("logSync got interrupted");
      }
    }
    if (runningState.get() == RunningState.RUNNING) {
      // reset election timer to avoid punishing the leader for our own
      // long disk writes
      heartbeatMonitor.updateLastRpcTime(Time.monotonicNow());
    }
    final AppendEntriesReply reply = new AppendEntriesReply(leaderId, getId(),
        currentTerm, nextIndex, AppendResult.SUCCESS);
    LOG.debug("{}: succeeded to append entries. Reply: {}", getId(), reply);
    return reply;
  }
}
