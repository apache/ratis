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
import org.apache.hadoop.raft.server.protocol.*;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.raft.server.RaftConfiguration.computeNewPeers;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RaftServer implements RaftServerProtocol, RaftClientProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  static class RpcAndHandler<REQUEST extends RaftRpcMessage,
                             REPLY   extends RaftRpcMessage>  {
    private final RaftRpc<REQUEST, REPLY> rpc;
    private final RequestHandler<REQUEST, REPLY> handler;
    private final RequestHandler.HandlerDaemon<REQUEST, REPLY> daemon;

    RpcAndHandler(String id, RaftRpc<REQUEST, REPLY> rpc,
                  RequestHandler<REQUEST, REPLY> handler) {
      this.rpc = rpc;
      this.handler = handler;
      this.daemon = new RequestHandler.HandlerDaemon<>(id, handler, rpc);
    }

    void startDaemon() {
      daemon.start();
    }

    void interruptAndJoinDaemon() throws InterruptedException {
      daemon.interrupt();
      daemon.join();
    }

    REPLY sendRequest(REQUEST r) throws IOException {
      return rpc.sendRequest(r);
    }
  }

  enum RunningState {INITIALIZED, RUNNING, STOPPED}

  private final ServerState state;
  private volatile Role role;
  private final AtomicReference<RunningState> runningState
      = new AtomicReference<>(RunningState.INITIALIZED);

  private final NavigableMap<Long, RaftClientRequest> pendingRequests;

  /** used when the peer is follower, to monitor election timeout */
  private volatile FollowerState heartbeatMonitor;

  /** used when the peer is candidate, to request votes from other peers */
  private volatile LeaderElection electionDaemon;

  /** used when the peer is leader */
  private volatile LeaderState leaderState;

  private final RpcAndHandler<RaftServerRequest, RaftServerReply> serverHandler;
  private final RpcAndHandler<RaftClientRequest, RaftClientReply> clientHandler;

  public RaftServer(String id, RaftConfiguration raftConf,
        RaftRpc<RaftServerRequest, RaftServerReply> serverRpc,
        RaftRpc<RaftClientRequest, RaftClientReply> clientRpc) {
    this.state = new ServerState(id, raftConf);
    this.pendingRequests = new ConcurrentSkipListMap<>();
    this.serverHandler = new RpcAndHandler<>(id, serverRpc, serverRequestHandler);
    this.clientHandler = new RpcAndHandler<>(id, clientRpc, clientRequestHandler);
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

  synchronized void changeToFollower() {
    if (isFollower()) {
      return;
    }
    if (isLeader()) {
      assert leaderState != null;
      shutdownLeaderState();
      // TODO: handle pending requests: we can send back
      // NotLeaderException and let the client retry
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
      leader.stopRunning();
      leader.interrupt();
    }
    leaderState = null;
  }

  private void shutdownElectionDaemon() {
    final LeaderElection election = electionDaemon;
    if (election != null) {
      election.stopRunning();
      election.interrupt();
      try {
        election.join();
      } catch (InterruptedException ignored) {
      }
    }
    electionDaemon = null;
  }

  synchronized void changeToLeader() {
    Preconditions.checkState(isCandidate());
    shutdownElectionDaemon();
    role = Role.LEADER;
    // start sending AppendEntries RPC to followers
    leaderState = new LeaderState(this);
    leaderState.start();
  }

  private void shutdownHeartbeatMonitor() {
    final FollowerState hm = heartbeatMonitor;
    if (hm != null) {
      hm.stopRunning();
      hm.interrupt();
      try {
        hm.join();
      } catch (InterruptedException ignored) {
      }
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

  synchronized long initElection() {
    return state.initElection();
  }

  @Override
  public String toString() {
    return role + " " + state + " " + runningState;
  }

  private void checkLeaderState() throws NotLeaderException {
    if (!isLeader()) {
      throw new NotLeaderException(getId(),
          getRaftConf().getPeer(state.getLeaderId()));
    }
  }

  /**
   * Handle a request from client.
   */
  @Override
  public void submit(RaftClientRequest request) throws IOException {
    synchronized (this) {
      // append the message to its local log
      final long entryIndex = state.getLog().apply(state.getCurrentTerm(),
          request.getMessage());
      checkLeaderState();
      // put the request into the pending queue
      pendingRequests.put(entryIndex, request);
      leaderState.notifySenders();
    }
    state.getLog().logSync();
    // return without waiting.
    // TODO: after the corresponding log entry is committed,
    // a background thread will remove the request from the pending queue and
    // send back the response
  }

  @Override
  public void setConfiguration(SetConfigurationRequest request)
      throws IOException {
    final RaftPeer[] newMembers = request.getNewMembers();
    synchronized (this) {
      checkLeaderState();
      final RaftConfiguration current = getRaftConf();
      // make sure there is no other raft reconfiguration in progress
      if (!current.inStableState()) {
        throw new ReconfigurationInProgressException(current,
            "Reconfiguration in progress");
      }
      // todo: wait for staging peers to catch up

      // return true if the new configuration is the same with the current one
      if (current.hasNoChange(newMembers)) {
        return;
      }

      final RaftConfiguration newConf= current.generateOldNewConf(newMembers,
          state.getLog().getNextIndex());
      // apply the new configuration to log, and use it as the current conf
      final long entryIndex = state.getLog().apply(state.getCurrentTerm(),
          current, newConf);
      state.setRaftConf(newConf);

      // update the LeaderState's sender list
      leaderState.addSenders(computeNewPeers(newMembers, current));

      // start replicating the configuration change
      pendingRequests.put(entryIndex, request);
      leaderState.notifySenders();
    }
    state.getLog().logSync();
    // release the handler and the LeaderState thread will trigger the next step
    // once the (old, new) entry is committed, and finally send the response
    // back the client.
  }

  @Override
  public RaftServerReply requestVote(String candidateId, long candidateTerm,
                                     TermIndex candidateLastEntry) throws IOException {
    LOG.debug("{}: receive requestVote({}, {}, {})",
        getId(), candidateId, candidateTerm, candidateLastEntry);
    assertRunningState();

    final long startTime = Time.monotonicNow();
    boolean voteGranted = false;
    final RaftServerReply reply;
    synchronized (this) {
      if (state.recognizeCandidate(candidateId, candidateTerm)) {
        changeToFollower();

        // see Section 5.4.1 Election restriction
        if (state.isLogUpToDate(candidateLastEntry)) {
          heartbeatMonitor.updateLastRpcTime(startTime);
          state.grantVote(candidateId, candidateTerm);
          voteGranted = true;
        }
      }
      reply = new RaftServerReply(candidateId, getId(),
          state.getCurrentTerm(), voteGranted);
    }
    // TODO persist the votedFor/currentTerm information
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

    final long startTime = Time.monotonicNow();
    final long currentTerm;
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        return new RaftServerReply(leaderId, getId(), currentTerm, false);
      }
      changeToFollower();

      Preconditions.checkState(currentTerm == leaderTerm);
      heartbeatMonitor.updateLastRpcTime(startTime);
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
    return serverHandler.sendRequest(request);
  }

  RaftServerReply sendAppendEntries(AppendEntriesRequest request)
      throws IOException {
    return serverHandler.sendRequest(request);
  }

  final RequestHandler<RaftServerRequest, RaftServerReply> serverRequestHandler
      = new RequestHandler<RaftServerRequest, RaftServerReply>() {
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

  final RequestHandler<RaftClientRequest, RaftClientReply> clientRequestHandler
      = new RequestHandler<RaftClientRequest, RaftClientReply>() {
    @Override
    public boolean isRunning() {
      return RaftServer.this.isRunning();
    }

    @Override
    public RaftClientReply handleRequest(RaftClientRequest r) throws IOException {
      submit(r);
      return new RaftClientReply(r.getRequestorId(), r.getReplierId());
    }
  };
}
