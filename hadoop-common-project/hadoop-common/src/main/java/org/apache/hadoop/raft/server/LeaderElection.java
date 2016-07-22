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
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerReply;
import org.apache.hadoop.raft.server.protocol.RequestVoteReply;
import org.apache.hadoop.raft.server.protocol.RequestVoteRequest;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.raft.server.protocol.pb.ProtoUtils;
import org.apache.hadoop.raft.util.RaftUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.hadoop.raft.server.LeaderElection.Result.PASSED;

class LeaderElection extends Daemon {
  static final Logger LOG = RaftServer.LOG;

  private ResultAndTerm logAndReturn(Result r, List<RaftServerReply> responses,
      List<Exception> exceptions, long newTerm) {
    LOG.info(server.getId() + ": Election " + r + "; received "
        + responses.size() + " response(s) " + responses + " and "
        + exceptions.size() + " exception(s); " + conf);
    int i = 0;
    for(Exception e : exceptions) {
      LOG.info("  " + i++ + ": " + e);
    }
    return new ResultAndTerm(r, newTerm);
  }

  enum Result {PASSED, REJECTED, TIMEOUT, DISCOVERED_A_NEW_TERM, SHUTDOWN}

  private static class ResultAndTerm {
    final Result result;
    final long term;

    ResultAndTerm(Result result, long term) {
      this.result = result;
      this.term = term;
    }
  }

  private final RaftServer server;
  private ExecutorCompletionService<RequestVoteReply> service;
  private ExecutorService executor;
  private volatile boolean running;
  /**
   * The Raft configuration should not change while the peer is in candidate
   * state. If the configuration changes, another peer should be acting as a
   * leader and this LeaderElection session should end.
   */
  private final RaftConfiguration conf;
  private final Collection<RaftPeer> others;

  LeaderElection(RaftServer server) {
    this.server = server;
    conf = server.getRaftConf();
    others = conf.getOtherPeers(server.getId());
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
          "Currently role: {}.", server.getId(), server.getRole());
    } catch (IOException e) {
      LOG.warn("Failed to persist votedFor/term. Exit the leader election.", e);
      stopRunning();
    }
  }

  /**
   * After a peer changes its role to candidate, it invokes this method to
   * send out requestVote rpc to all other peers.
   */
  private void askForVotes() throws InterruptedException, IOException {
    final ServerState state = server.getState();
    while (running && server.isCandidate()) {
      // one round of requestVotes
      final long electionTerm;
      synchronized (server) {
        electionTerm = state.initElection();
        server.getState().persistMetadata(); // TODO add tests for failure here
      }
      LOG.info(state.getSelfId() + ": begin an election in Term "
          + electionTerm);

      final TermIndex lastEntry = ProtoUtils.toTermIndex(
          state.getLog().getLastEntry());

      final ResultAndTerm r;
      try {
        initExecutor();
        int submitted = submitRequests(electionTerm, lastEntry);
        r = waitForResults(electionTerm, submitted);
      } finally {
        executor.shutdown();
      }

      synchronized (server) {
        if (electionTerm != state.getCurrentTerm() || !running ||
            !server.isCandidate()) {
          return; // term already passed or no longer a candidate.
        }

        switch (r.result) {
          case PASSED:
            server.changeToLeader();
            return;
          case SHUTDOWN:
            LOG.info("{} received shutdown response when requesting votes.",
                server.getId());
            server.kill();
            return;
          case REJECTED:
          case DISCOVERED_A_NEW_TERM:
            final long term = r.term > server.getState().getCurrentTerm() ?
                r.term : server.getState().getCurrentTerm();
            server.changeToFollower(term, true);
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
      final RequestVoteRequest r = server.createRequestVoteRequest(peer.getId(),
          electionTerm, lastEntry);
      service.submit(
          () -> (RequestVoteReply) server.getServerRpc().sendServerRequest(r));
      submitted++;
    }
    return submitted;
  }

  private ResultAndTerm waitForResults(final long electionTerm,
      final int submitted) throws InterruptedException {
    final long startTime = Time.monotonicNow();
    final long timeout = startTime + RaftConstants.getRandomElectionWaitTime();
    final List<RaftServerReply> responses = new ArrayList<>();
    final List<Exception> exceptions = new ArrayList<>();
    int waitForNum = submitted;
    Collection<String> votedPeers = new ArrayList<>();
    while (waitForNum > 0 && running && server.isCandidate()) {
      final long waitTime = timeout - Time.monotonicNow();
      if (waitTime <= 0) {
        return logAndReturn(Result.TIMEOUT, responses, exceptions, -1);
      }

      try {
        final Future<RequestVoteReply> future = service.poll(
            waitTime, TimeUnit.MILLISECONDS);
        if (future == null) {
          continue; // poll timeout, continue to return Result.TIMEOUT
        }

        final RequestVoteReply r = future.get();
        responses.add(r);
        if (r.shouldShutdown()) {
          return logAndReturn(Result.SHUTDOWN, responses, exceptions, -1);
        }
        if (r.getTerm() > electionTerm) {
          return logAndReturn(Result.DISCOVERED_A_NEW_TERM, responses,
              exceptions, r.getTerm());
        }
        if (r.isSuccess()) {
          votedPeers.add(r.getReplierId());
          if (conf.hasMajorities(votedPeers, server.getId())) {
            return logAndReturn(PASSED, responses, exceptions, -1);
          }
        }
      } catch(ExecutionException e) {
        LOG.warn("", e);
        exceptions.add(e);
      }
      waitForNum--;
    }
    // received all the responses
    return logAndReturn(Result.REJECTED, responses, exceptions, -1);
  }
}
