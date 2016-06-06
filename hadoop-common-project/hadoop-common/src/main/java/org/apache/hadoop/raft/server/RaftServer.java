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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.raft.protocol.*;
import org.apache.hadoop.raft.server.RequestHandler.HandlerInterface;
import org.apache.hadoop.raft.server.protocol.*;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.raft.server.RaftConfiguration.computeNewPeers;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RaftServer implements RaftServerProtocol, RaftClientProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  enum RunningState {INITIALIZED, RUNNING, STOPPED}

  private final ServerState state;
  private volatile Role role;
  private final AtomicReference<RunningState> runningState
      = new AtomicReference<>(RunningState.INITIALIZED);

  /** used when the peer is follower, to monitor election timeout */
  private volatile FollowerState heartbeatMonitor;

  /** used when the peer is candidate, to request votes from other peers */
  private volatile LeaderElection electionDaemon;

  /** used when the peer is leader */
  private volatile LeaderState leaderState;

  private final RequestHandler<RaftServerRequest, RaftServerReply> serverHandler;
  final RequestHandler<RaftClientRequest, RaftClientReply> clientHandler;

  public RaftServer(String id, RaftConfiguration raftConf,
        RaftRpc<RaftServerRequest, RaftServerReply> serverRpc,
        RaftRpc<RaftClientRequest, RaftClientReply> clientRpc) {
    this.state = new ServerState(id, raftConf);
    this.serverHandler = new RequestHandler<>(id, "serverHandler", serverRpc,
        serverHandlerImpl);
    this.clientHandler = new RequestHandler<>(id, "clientHandler", clientRpc,
        clientHandlerImpl);
  }

  void changeRunningState(RunningState oldState, RunningState newState) {
    final boolean changed = runningState.compareAndSet(oldState, newState);
    Preconditions.checkState(changed);
  }

  public void start() {
    changeRunningState(RunningState.INITIALIZED, RunningState.RUNNING);

    role = Role.FOLLOWER;
    heartbeatMonitor = new FollowerState(this);
    heartbeatMonitor.start();

    serverHandler.startDaemon();
    clientHandler.startDaemon();
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
    return runningState.get() == RunningState.RUNNING;
  }

  public void kill() {
    changeRunningState(RunningState.RUNNING, RunningState.STOPPED);

    try {
      clientHandler.interruptAndJoinDaemon();
      serverHandler.interruptAndJoinDaemon();

      shutdownHeartbeatMonitor();
      shutdownElectionDaemon();
      shutdownLeaderState();
    } catch (InterruptedException ignored) {
    }
  }

  void assertRunningState() throws IOException {
    if (!isRunning()) {
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
    return new NotLeaderException(getId(),
        getRaftConf().getPeer(state.getLeaderId()));
  }

  /**
   * Handle a request from client.
   */
  @Override
  public void submit(RaftClientRequest request) throws IOException {
    LOG.debug("{}: receive submit({})", getId(), request);
    assertRunningState();

    synchronized (this) {
      checkLeaderState();
      // append the message to its local log
      final long entryIndex = state.applyLog(request.getMessage());

      // put the request into the pending queue
      leaderState.addPendingRequest(entryIndex, request);
      leaderState.notifySenders();
    }
    state.getLog().logSync();
  }

  @Override
  public void setConfiguration(SetConfigurationRequest request)
      throws IOException {
    LOG.debug("{}: receive setConfiguration({})", getId(), request);
    assertRunningState();

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
        return;
      }
      // add staging state into the leaderState
      leaderState.startSetConfiguration(request);
    }
    state.getLog().logSync();
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
  public RequestVoteReply requestVote(String candidateId, long candidateTerm,
      TermIndex candidateLastEntry) throws IOException {
    LOG.debug("{}: receive requestVote({}, {}, {})",
        getId(), candidateId, candidateTerm, candidateLastEntry);
    assertRunningState();

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
    state.getLog().logSync();
    return reply;
  }

  @Override
  public RaftServerReply appendEntries(String leaderId, long leaderTerm,
      TermIndex previous, long leaderCommit, Entry... entries)
          throws IOException {
    LOG.debug("{}: receive appendEntries({}, {}, {}, {}, {})",
        getId(), leaderId, leaderTerm, previous, leaderCommit,
        (entries == null || entries.length == 0) ? "HEARTBEAT"
            : entries.length == 1? entries[0]: Arrays.asList(entries));
    assertRunningState();
    Entry.assertEntries(leaderTerm, entries);

    final long currentTerm;
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        return new RaftServerReply(leaderId, getId(), currentTerm, false);
      }
      changeToFollower(leaderTerm);
      state.setLeader(leaderId);

      heartbeatMonitor.updateLastRpcTime(Time.monotonicNow());
      state.getLog().updateLastCommitted(leaderCommit, currentTerm);

      if (previous != null && !state.getLog().contains(previous)) {
        return new RaftServerReply(leaderId, getId(), currentTerm, false);
      }

      RaftConfiguration newConf = state.getLog().apply(entries);
      if (newConf != null) {
        state.setRaftConf(newConf);
      }
    }

    state.getLog().logSync();
    // reset election timer to avoid punishing the leader for our own
    // long disk writes
    heartbeatMonitor.updateLastRpcTime(Time.monotonicNow());
    return new RaftServerReply(leaderId, getId(), currentTerm, true);
  }

  synchronized AppendEntriesRequest createAppendEntriesRequest(String targetId,
      TermIndex previous, Entry[] entries) {
    return new AppendEntriesRequest(getId(), targetId,
        state.getCurrentTerm(), previous, entries,
        state.getLog().getLastCommitted().getIndex());
  }

  synchronized RequestVoteRequest createRequestVoteRequest(String targetId,
      long term, TermIndex lastEntry) {
    return new RequestVoteRequest(getId(), targetId, term, lastEntry);
  }

  RaftServerReply sendRequestVote(RequestVoteRequest request)
      throws IOException {
    return serverHandler.getRpc().sendRequest(request);
  }

  RaftServerReply sendAppendEntries(AppendEntriesRequest request)
      throws IOException {
    return serverHandler.getRpc().sendRequest(request);
  }

  final HandlerInterface<RaftServerRequest, RaftServerReply> serverHandlerImpl
      = new HandlerInterface<RaftServerRequest, RaftServerReply>() {
    @Override
    public boolean isRunning() {
      return RaftServer.this.isRunning();
    }

    @Override
    public RaftServerReply handleRequest(RaftServerRequest r)
        throws IOException {
      if (r instanceof AppendEntriesRequest) {
        final AppendEntriesRequest ap = (AppendEntriesRequest) r;
        return appendEntries(ap.getRequestorId(), ap.getLeaderTerm(),
            ap.getPreviousLog(), ap.getLeaderCommit(), ap.getEntries());
      } else if (r instanceof RequestVoteRequest) {
        final RequestVoteRequest rr = (RequestVoteRequest) r;
        return requestVote(rr.getCandidateId(), rr.getCandidateTerm(),
            rr.getLastLogIndex());
      } else { // TODO support other requests later
        // should not come here now
        return new RaftServerReply(r.getRequestorId(), getId(),
            state.getCurrentTerm(), false);
      }
    }
  };

  final HandlerInterface<RaftClientRequest, RaftClientReply> clientHandlerImpl
      = new HandlerInterface<RaftClientRequest, RaftClientReply>() {
    @Override
    public boolean isRunning() {
      return RaftServer.this.isRunning();
    }

    @Override
    public RaftClientReply handleRequest(RaftClientRequest request)
        throws IOException {
      if (request instanceof SetConfigurationRequest) {
        setConfiguration((SetConfigurationRequest) request);
      } else {
        submit(request);
      }
      return null;
    }
  };
}
