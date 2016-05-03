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
import org.apache.hadoop.raft.protocol.ClientRaftProtocol;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.protocol.NotLeaderException;
import org.apache.hadoop.raft.protocol.Response;
import org.apache.hadoop.raft.server.protocol.AppendEntriesRequest;
import org.apache.hadoop.raft.server.protocol.Entry;
import org.apache.hadoop.raft.server.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerResponse;
import org.apache.hadoop.raft.server.protocol.RaftServerProtocol;
import org.apache.hadoop.raft.server.protocol.RequestVoteRequest;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RaftServer implements RaftServerProtocol, ClientRaftProtocol {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  /**
   * Used when the peer is a follower. Used to track the election timeout.
   */
  private class HeartbeatMonitor implements Runnable {
    private final AtomicLong lastRpcTime = new AtomicLong(Time.monotonicNow());
    private final long electionTimeout = RaftConstants.getRandomElectionWaitTime();

    void updateLastRpcTime(long now) {
      lastRpcTime.set(now);
    }

    @Override
    public  void run() {
      while (isFollower()) {
        try {
          long waitTime = electionTimeout -
              (Time.monotonicNow() - lastRpcTime.get());
          if (waitTime > 0 ) {
            Thread.sleep(waitTime);
          }
          final long now = Time.monotonicNow();
          if (now >= lastRpcTime.get() + electionTimeout) {
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
      return getState().getSelfId() + ": " + getClass().getSimpleName();
    }
  }

  private final ServerState state;
  private final RaftConfiguration raftConf;
  private volatile Role role = Role.FOLLOWER;

  private final NavigableMap<Long, Response> pendingRequests;

  /** used when the peer is follower, to monitor election timeout */
  private HeartbeatMonitor heartbeatMonitor;
  private Daemon heartbeatMonitorDaemon;

  /** used when the peer is candidate, to request votes from other peers */
  private LeaderElection electionDaemon;

  /** used when the peer is leader */
  private LeaderState leaderState;

  public RaftServer(String id, RaftConfiguration raftConf) {
    this.raftConf = raftConf; // TODO: how to init raft conf
    this.state = new ServerState(id);
    this.pendingRequests = new ConcurrentSkipListMap<>();
    changeToFollower();
  }

  ServerState getState() {
    return this.state;
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

  boolean isLeader() {
    return role == Role.LEADER;
  }

  Role getRole() {
    return role;
  }

  synchronized void changeToFollower() {
    if (isLeader()) {
      assert leaderState != null;
      leaderState.stop();
      leaderState = null;
      // TODO: handle pending requests: we can send back
      // NotLeaderException and let the client retry
    } else if (isCandidate()) {
      electionDaemon.stopRunning();
      electionDaemon.interrupt();
    }
    role = Role.FOLLOWER;
    heartbeatMonitor = new HeartbeatMonitor();
    heartbeatMonitorDaemon = new Daemon(heartbeatMonitor);
    heartbeatMonitorDaemon.start();
  }

  synchronized void changeToLeader() {
    Preconditions.checkState(isCandidate());
    electionDaemon = null;
    role = Role.LEADER;
    // start sending AppendEntries RPC to followers
    leaderState = new LeaderState(this);
    leaderState.start();
  }

  synchronized void changeToCandidate() {
    Preconditions.checkState(isFollower());
    heartbeatMonitorDaemon = null;
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
    return role.toString() + ": " + state.toString();
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

  RaftServerResponse sendRequestVote(RaftPeer peer, RequestVoteRequest request)
      throws RaftServerException {
    // TODO
    return null;
  }

  RaftServerResponse sendAppendEntries(RaftPeer peer,
      AppendEntriesRequest request) throws RaftServerException {
    // TODO
    return null;
  }
}