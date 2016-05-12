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
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.raft.server.RaftConfiguration.computeNewPeers;
import static org.apache.hadoop.util.ExitUtil.terminate;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RaftServer implements RaftServerProtocol, RaftClientProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  private static class PendingRequestElement implements Comparable<PendingRequestElement> {
    private final long index;
    private final RaftClientRequest request;
    private final RaftClientReply reply;

    PendingRequestElement(long index, RaftClientRequest request, RaftClientReply reply) {
      this.index = index;
      this.request = request;
      this.reply = reply;
    }

    @Override
    public int compareTo(PendingRequestElement that) {
      return Long.compare(this.index, that.index);
    }
  }

  class PendingRequests {
    private final PriorityBlockingQueue<PendingRequestElement> queue
        = new PriorityBlockingQueue<>();
    private final PendingRequestDaemon daemon = new PendingRequestDaemon();

    void startDaemon() {
      daemon.start();
    }

    void put(long index, RaftClientRequest request) {
      put(index, request, new RaftClientReply(request));
    }

    void put(long index, RaftClientRequest request, RaftClientReply reply) {
      final boolean b = queue.offer(new PendingRequestElement(index, request, reply));
      Preconditions.checkState(b);
    }

    class PendingRequestDaemon extends Daemon {
      @Override
      public String toString() {
        return getClass().getSimpleName();
      }

      @Override
      public void run() {
        for(; isRunning();) {
          try {
            final long lastCommitted = state.getLog().waitLastCommitted();
            for (PendingRequestElement pre;
                 (pre = queue.peek()) != null && pre.index <= lastCommitted; ) {
              pre = queue.poll();
              try {
                clientHandler.sendReply(pre.request, pre.reply, null);
              } catch (IOException ioe) {
                LOG.error(this + " has " + ioe);
                LOG.trace("TRACE", ioe);
              }
            }
          } catch (InterruptedException e) {
            LOG.info(this + " is interrupted by " + e);
            LOG.trace("TRACE", e);
            break;
          } catch(Throwable t) {
            if (!isRunning()) {
              LOG.info(this + " is stopped.");
              break;
            }
            LOG.error(this + " is terminating due to", t);
            terminate(1, t);
          }
        }
      }
    }
  }

  enum RunningState {INITIALIZED, RUNNING, STOPPED}

  private final ServerState state;
  private volatile Role role;
  private final AtomicReference<RunningState> runningState
      = new AtomicReference<>(RunningState.INITIALIZED);

  private final PendingRequests pendingRequests = new PendingRequests();

  /** used when the peer is follower, to monitor election timeout */
  private volatile FollowerState heartbeatMonitor;

  /** used when the peer is candidate, to request votes from other peers */
  private volatile LeaderElection electionDaemon;

  /** used when the peer is leader */
  private volatile LeaderState leaderState;

  private final RequestHandler<RaftServerRequest, RaftServerReply> serverHandler;
  private final RequestHandler<RaftClientRequest, RaftClientReply> clientHandler;

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

    pendingRequests.startDaemon();
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
    LOG.debug("{}: receive submit({})", getId(), request);
    assertRunningState();

    synchronized (this) {
      checkLeaderState();
      // append the message to its local log
      final long entryIndex = state.applyLog(request.getMessage());

      // TODO: process the request and generate the reply here.
      final RaftClientReply reply = new RaftClientReply(request);

      // put the request into the pending queue
      pendingRequests.put(entryIndex, request, reply);
      leaderState.notifySenders();
    }
    state.getLog().logSync();
  }

  @Override
  public void setConfiguration(SetConfigurationRequest request)
      throws IOException {
    LOG.debug("{}: receive setConfiguration({})", getId(), request);
    assertRunningState();

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

  private boolean shouldWithholdVotes(long now) {
    return isLeader() ||
        (isFollower() && heartbeatMonitor.shouldWithholdVotes(now));
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
        && candidateLastEntry.getIndex() < getRaftConf().getLogEntryIndex();
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
        LOG.info("Withhold vote for term {} from server {}. " +
            "This server:{}, last rpc time from leader {} is {}",
            candidateTerm, candidateId, this, this.getState().getLeaderId(),
            (isFollower() ? heartbeatMonitor.getLastRpcTime() : -1));
      } else if (state.recognizeCandidate(candidateId, candidateTerm)) {
        changeToFollower();

        // see Section 5.4.1 Election restriction
        if (state.isLogUpToDate(candidateLastEntry)) {
          heartbeatMonitor.updateLastRpcTime(startTime);
          state.grantVote(candidateId, candidateTerm);
          voteGranted = true;
        }
      }
      if (!voteGranted && shouldSendShutdown(candidateId, candidateLastEntry)) {
        shouldShutdown = true;
      }
      reply = new RequestVoteReply(candidateId, getId(),
          state.getCurrentTerm(), voteGranted, shouldShutdown);
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

    final long currentTerm;
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        return new RaftServerReply(leaderId, getId(), currentTerm, false);
      }
      changeToFollower();

      Preconditions.checkState(currentTerm == leaderTerm);
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

    state.getLog().logSync(); // TODO logSync should be called in an async way
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

  final RequestHandler.HandlerInterface<RaftServerRequest, RaftServerReply> serverHandlerImpl
      = new RequestHandler.HandlerInterface<RaftServerRequest, RaftServerReply>() {
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

  final RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply> clientHandlerImpl
      = new RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply>() {
    @Override
    public boolean isRunning() {
      return RaftServer.this.isRunning();
    }

    @Override
    public RaftClientReply handleRequest(RaftClientRequest request)
        throws IOException {
      submit(request);
      return null;
    }
  };
}
