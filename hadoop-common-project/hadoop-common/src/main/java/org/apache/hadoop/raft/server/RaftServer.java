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
import org.apache.hadoop.raft.protocol.RaftClientProtocol;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.protocol.NotLeaderException;
import org.apache.hadoop.raft.protocol.Response;
import org.apache.hadoop.raft.server.protocol.AppendEntriesRequest;
import org.apache.hadoop.raft.server.protocol.Entry;
import org.apache.hadoop.raft.server.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerRequest;
import org.apache.hadoop.raft.server.protocol.RaftServerResponse;
import org.apache.hadoop.raft.server.protocol.RaftServerProtocol;
import org.apache.hadoop.raft.server.protocol.RequestVoteRequest;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.raft.server.simulation.EventQueues;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.hadoop.util.ExitUtil.terminate;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RaftServer implements RaftServerProtocol, RaftClientProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  /**
   * Used when the peer is a follower. Used to track the election timeout.
   */
  private class HeartbeatMonitor extends Daemon {
    private volatile long lastRpcTime = Time.monotonicNow();
    private final long electionTimeout = RaftConstants.getRandomElectionWaitTime();
    private volatile boolean monitorRunning = true;

    void updateLastRpcTime(long now) {
      LOG.debug("{} update last rpc time to {}", state.getSelfId(), now);
      lastRpcTime = now;
    }

    void stopRunning() {
      this.monitorRunning = false;
    }

    @Override
    public  void run() {
      while (monitorRunning && isFollower()) {
        try {
          Thread.sleep(electionTimeout);
          if (!monitorRunning || !isFollower()) {
            LOG.info("{} heartbeat monitor quit", state.getSelfId());
            break;
          }
          final long now = Time.monotonicNow();
          if (now >= lastRpcTime + electionTimeout) {
            LOG.info("{} changing to " + Role.CANDIDATE +
                " now:{}, last rpc time:{}, electionTimeout:{}",
                state.getSelfId(), now, lastRpcTime, electionTimeout);
            // election timeout, should become a candidate
            RaftServer.this.changeToCandidate();
            break;
          }
        } catch (InterruptedException e) {
          LOG.info(this + " was interrupted: " + e);
          LOG.trace("TRACE", e);
          return;
        } catch (Exception e) {
          LOG.warn(this + " caught an excpetion", e);
        }
      }
    }

    @Override
    public String toString() {
      return RaftServer.this.getState().getSelfId()
          + ": " + getClass().getSimpleName();
    }
  }

  private final ServerState state;
  private final RaftConfiguration raftConf;
  private volatile Role role;
  private volatile boolean isRunning = true;

  private final NavigableMap<Long, Response> pendingRequests;

  /** used when the peer is follower, to monitor election timeout */
  private volatile HeartbeatMonitor heartbeatMonitor;

  /** used when the peer is candidate, to request votes from other peers */
  private volatile LeaderElection electionDaemon;

  /** used when the peer is leader */
  private volatile LeaderState leaderState;

  private final EventQueues rpcQueues;
  private final RequestHandler handler;

  public RaftServer(String id, RaftConfiguration raftConf, EventQueues rpcQueues) {
    this.raftConf = raftConf;
    this.state = new ServerState(id);
    this.pendingRequests = new ConcurrentSkipListMap<>();
    this.rpcQueues = rpcQueues;
    this.handler = new RequestHandler();
  }

  public void start() {
    handler.start();
    role = Role.FOLLOWER;
    heartbeatMonitor = new HeartbeatMonitor();
    heartbeatMonitor.start();
  }

  public ServerState getState() {
    return this.state;
  }

  public boolean isRunning() {
    return isRunning;
  }

  public void kill() {
    isRunning = false;
    try {
      handler.interrupt();
      handler.join();

      shutdownHeartbeatMonitor();
      shutdownElectionDaemon();
      final LeaderState leader = leaderState;
      if (leader != null) {
        leader.stop();
      }
    } catch (InterruptedException ignored) {
    }
  }

  void assertRunningState() throws IOException {
    if (!isRunning()) {
      throw new IOException(getState().getSelfId() + " is not running.");
    }
  }

  Collection<RaftPeer> getOtherPeers() {
    List<RaftPeer> others = new ArrayList<>(raftConf.getSize() - 1);
    for (RaftPeer peer : raftConf.getPeers()) {
      if (!state.getSelfId().equals(peer.getId())) {
        others.add(peer);
      }
    }
    return Collections.unmodifiableList(others);
  }

  boolean isFollower() {
    return role == Role.FOLLOWER;
  }

  boolean isCandidate() {
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
      leaderState.stop();
      leaderState = null;
      // TODO: handle pending requests: we can send back
      // NotLeaderException and let the client retry
    } else if (isCandidate()) {
      shutdownElectionDaemon();
    }
    role = Role.FOLLOWER;
    heartbeatMonitor = new HeartbeatMonitor();
    heartbeatMonitor.start();
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
    final HeartbeatMonitor hm = heartbeatMonitor;
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
    return role + " " + state + " " + (isRunning()? "RUNNING": "STOPPED");
  }

  private void checkLeaderState() throws NotLeaderException {
    if (!isLeader()) {
      throw new NotLeaderException(raftConf.getPeer(state.getLeaderId()));
    }
  }

  /**
   * Handle a request from client.
   */
  @Override
  public void submit(Message message) throws IOException {
    checkLeaderState();
    // append the message to its local log
    final long entryIndex = state.getLog().apply(state.getCurrentTerm(),
        message);
    synchronized (this) {
      checkLeaderState();
      // put the request into the pending queue
      pendingRequests.put(entryIndex, new Response());
      leaderState.notifySenders();
    }
    // return without waiting.
    // TODO: after the corresponding log entry is committed,
    // a background thread will remove the request from the pending queue and
    // send back the response
  }

  @Override
  public RaftServerResponse requestVote(String candidateId, long candidateTerm,
      TermIndex candidateLastEntry) throws IOException {
    LOG.debug("{}: receive requestVote({}, {}, {})",
        state.getSelfId(), candidateId, candidateTerm, candidateLastEntry);
    assertRunningState();

    final long startTime = Time.monotonicNow();
    boolean voteGranted = false;
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
      return new RaftServerResponse(state.getSelfId(),
          state.getCurrentTerm(), voteGranted);
    }
  }

  @Override
  public RaftServerResponse appendEntries(String leaderId, long leaderTerm,
      TermIndex previous, long leaderCommit, Entry... entries)
          throws IOException {
    LOG.debug("{}: receive appendEntries({}, {}, {}, {}, {})",
        state.getSelfId(), leaderId, leaderTerm, previous, leaderCommit,
        (entries == null || entries.length == 0) ? "[]"
            : entries.length + " entries start with " + entries[0]);
    assertRunningState();

    final long startTime = Time.monotonicNow();
    Entry.assertEntries(leaderTerm, entries);

    final long currentTerm;
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        return new RaftServerResponse(state.getSelfId(), currentTerm, false);
      }
      changeToFollower();

      Preconditions.checkState(currentTerm == leaderTerm);
      heartbeatMonitor.updateLastRpcTime(startTime);

      if (previous != null && !state.getLog().contains(previous)) {
        return new RaftServerResponse(state.getSelfId(), currentTerm, false);
      }
    }
    state.getLog().apply(entries);
    return new RaftServerResponse(state.getSelfId(), currentTerm, true);
  }

  synchronized AppendEntriesRequest createAppendEntriesRequest(String targetId,
      TermIndex previous, Entry[] entries) {
    return new AppendEntriesRequest(state.getSelfId(), targetId,
        state.getCurrentTerm(), previous, entries,
        state.getLog().getLastCommitted().getIndex());
  }

  synchronized RequestVoteRequest createRequestVoteRequest(String targetId,
      long term, TermIndex lastEntry) {
    return new RequestVoteRequest(state.getSelfId(), targetId, term,
        state.getSelfId(), lastEntry);
  }

  RaftServerResponse sendRequestVote(RequestVoteRequest request)
      throws IOException {
    try {
      return rpcQueues.request(request);
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
  }

  RaftServerResponse sendAppendEntries(AppendEntriesRequest request)
      throws RaftServerException, InterruptedException {
    return rpcQueues.request(request);
  }

  private class ClientResponder extends Daemon {
    @Override
    public void run() {
      // TODO: based on committed index, send back response to client
    }
  }

  /**
   * A thread keep polling requests from the request queue. Used for simulation.
   */
  private class RequestHandler extends Thread {
    @Override
    public void run() {
      while (isRunning()) {
        try {
          RaftServerRequest request = rpcQueues.takeRequest(state.getSelfId());
          RaftServerResponse response = handleRequest(request);
          rpcQueues.response(request, response);
        } catch (Throwable t) {
          if (!isRunning()) {
            LOG.info("The Raft peer {} is stopped", state.getSelfId());
            break;
          } else if (t instanceof InterruptedException) {
            LOG.info("Stopping Raft peer {} for testing.", state.getSelfId());
            break;
          }
          LOG.error("Raft peer {}'s request handler received exception.",
              state.getSelfId(), t);
          terminate(1, t);
        }
      }
    }

    private RaftServerResponse handleRequest(RaftServerRequest r)
        throws IOException {
      if (r instanceof AppendEntriesRequest) {
        final AppendEntriesRequest ap = (AppendEntriesRequest) r;
        return appendEntries(ap.getFromId(), ap.getLeaderTerm(),
            ap.getPreviousLog(), ap.getLeaderCommit(), ap.getEntries());
      } else if (r instanceof RequestVoteRequest) {
        final RequestVoteRequest rr = (RequestVoteRequest) r;
        return requestVote(rr.getCandidateId(), rr.getCandidateTerm(),
            rr.getLastLogIndex());
      } else { // TODO support other requests later
        // should not come here now
        return new RaftServerResponse(state.getSelfId(),
            state.getCurrentTerm(), false);
      }
    }
  }
}