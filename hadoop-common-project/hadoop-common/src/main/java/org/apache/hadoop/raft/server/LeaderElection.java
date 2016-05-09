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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.raft.server.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerResponse;
import org.apache.hadoop.raft.server.protocol.RequestVoteRequest;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class LeaderElection extends Daemon {
  public static final Logger LOG = RaftServer.LOG;

  private Result logAndReturn(Result r, List<RaftServerResponse> responses,
                              List<Exception> exceptions) {
    LOG.info(raftServer.getState().getSelfId()
        + ": Election " + r + "; received "
        + responses.size() + " response(s) " + responses + " and "
        + exceptions.size() + " exception(s)");
    int i = 0;
    for(Exception e : exceptions) {
      LOG.info("  " + i++ + ": " + e);
    }
    return r;
  }

  enum Result {PASSED, REJECTED, TIMEOUT, DISCOVERED_A_NEW_TERM}

  private final RaftServer raftServer;
  private ExecutorCompletionService<RaftServerResponse> service;
  private ExecutorService executor;
  private volatile boolean running;
  /**
   * The Raft configuration should not change while the peer is in candidate
   * state. If the configuration changes, another peer should be acting as a
   * leader and this LeaderElection session should end.
   */
  private final RaftConfiguration conf;
  private final Collection<RaftPeer> others;

  LeaderElection(RaftServer raftServer) {
    this.raftServer = raftServer;
    conf = raftServer.getRaftConf();
    others = conf.getOtherPeers(raftServer.getState().getSelfId());
    this.running = true;
  }

  void stopRunning() {
    this.running = false;
  }

  private void initExecutor() {
    executor = Executors.newFixedThreadPool(others.size(),
        new ThreadFactoryBuilder().setDaemon(true).build());
    service = new ExecutorCompletionService<>(executor);
  }

  @Override
  public void run() {
    try {
      askForVotes();
    } catch (InterruptedException e) {
      // the leader election thread is interrupted. The peer may already step
      // down to a follower. The leader election should skip.
      LOG.info("The leader election thread of peer {} is interrupted. " +
          "Currently role: {}.", raftServer.getState().getSelfId(),
          raftServer.getRole());
    }
  }

  /**
   * After a peer changes its role to candidate, it invokes this method to
   * send out requestVote rpc to all other peers.
   */
  private void askForVotes() throws InterruptedException {
    final ServerState state = raftServer.getState();
    while (running && raftServer.isCandidate()) {
      // one round of requestVotes
      final long electionTerm = raftServer.initElection();
      LOG.info(state.getSelfId() + ": begin an election in Term "
          + electionTerm);

      final TermIndex lastEntry = state.getLog().getLastEntry();

      final Result r;
      try {
        initExecutor();
        int submitted = submitRequests(electionTerm, lastEntry);
        r = waitForResults(electionTerm, submitted);
      } finally {
        executor.shutdown();
      }

      synchronized (raftServer) {
        if (electionTerm != state.getCurrentTerm() || !running ||
            !raftServer.isCandidate()) {
          return; // term already passed or no longer a candidate.
        }

        switch (r) {
          case PASSED:
            raftServer.changeToLeader();
            return;
          case REJECTED:
          case DISCOVERED_A_NEW_TERM:
            raftServer.changeToFollower();
            return;
          case TIMEOUT:
            // should start another election
        }
      }
    }
  }

  private int submitRequests(final long electionTerm, final TermIndex lastEntry) {
    int submitted = 0;
    for (final RaftPeer peer : others) {
      final RequestVoteRequest r = raftServer.createRequestVoteRequest(
          peer.getId(), electionTerm, lastEntry);
      service.submit(new Callable<RaftServerResponse>() {
        @Override
        public RaftServerResponse call() throws IOException {
          return raftServer.sendRequestVote(r);
        }
      });
      submitted++;
    }
    return submitted;
  }

  private Result waitForResults(final long electionTerm, final int submitted)
      throws InterruptedException {
    final long startTime = Time.monotonicNow();
    final long timeout = startTime + RaftConstants.getRandomElectionWaitTime();
    final List<RaftServerResponse> responses = new ArrayList<>();
    final List<Exception> exceptions = new ArrayList<>();
    int waitForNum = submitted;
    Collection<String> votedPeers = new ArrayList<>();
    while (waitForNum > 0 && running && raftServer.isCandidate()) {
      final long waitTime = timeout - Time.monotonicNow();
      if (waitTime <= 0) {
        return logAndReturn(Result.TIMEOUT, responses, exceptions);
      }

      try {
        RaftServerResponse r = service.poll(waitTime, TimeUnit.MILLISECONDS).get();
        if (r.getTerm() > electionTerm) {
          return logAndReturn(Result.DISCOVERED_A_NEW_TERM, responses, exceptions);
        }
        responses.add(r);
        if (r.isSuccess()) {
          votedPeers.add(r.getReplierId());
          if (conf.hasMajorities(votedPeers, raftServer.getState().getSelfId())) {
            return logAndReturn(Result.PASSED, responses, exceptions);
          }
        }
      } catch(ExecutionException e) {
        LOG.warn("", e);
        exceptions.add(e);
      }
      waitForNum--;
    }
    // received all the responses
    return logAndReturn(Result.REJECTED, responses, exceptions);
  }
}
