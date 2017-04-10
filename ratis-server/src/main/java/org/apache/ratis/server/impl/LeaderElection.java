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

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

class LeaderElection extends Daemon {
  public static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);

  private ResultAndTerm logAndReturn(Result result,
      List<RequestVoteReplyProto> responses,
      List<Exception> exceptions, long newTerm) {
    LOG.info(server.getId() + ": Election " + result + "; received "
        + responses.size() + " response(s) "
        + responses.stream().map(r -> ProtoUtils.toString(r)).collect(Collectors.toList())
        + " and " + exceptions.size() + " exception(s); " + server.getState());
    int i = 0;
    for(Exception e : exceptions) {
      LOG.info("  " + i++ + ": " + e);
      LOG.trace("TRACE", e);
    }
    return new ResultAndTerm(result, newTerm);
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

  private final RaftServerImpl server;
  private ExecutorCompletionService<RequestVoteReplyProto> service;
  private ExecutorService executor;
  private volatile boolean running;
  /**
   * The Raft configuration should not change while the peer is in candidate
   * state. If the configuration changes, another peer should be acting as a
   * leader and this LeaderElection session should end.
   */
  private final RaftConfiguration conf;
  private final Collection<RaftPeer> others;

  LeaderElection(RaftServerImpl server) {
    this.server = server;
    conf = server.getRaftConf();
    others = conf.getOtherPeers(server.getId());
    this.running = true;
  }

  void stopRunning() {
    this.running = false;
  }

  private void initExecutor() {
    Preconditions.assertTrue(!others.isEmpty());
    executor = Executors.newFixedThreadPool(others.size(), Daemon::new);
    service = new ExecutorCompletionService<>(executor);
  }

  @Override
  public void run() {
    try {
      askForVotes();
    } catch (InterruptedException e) {
      // the leader election thread is interrupted. The peer may already step
      // down to a follower. The leader election should skip.
      LOG.info(server.getId() + " " + getClass().getSimpleName()
          + " thread is interrupted gracefully; server=" + server);
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
        server.getState().persistMetadata();
      }
      LOG.info(state.getSelfId() + ": begin an election in Term "
          + electionTerm);

      TermIndex lastEntry = ServerProtoUtils.toTermIndex(
          state.getLog().getLastEntry());
      if (lastEntry == null) {
        // lastEntry may need to be derived from snapshot
        SnapshotInfo snapshot = state.getLatestSnapshot();
        if (snapshot != null) {
          lastEntry = snapshot.getTermIndex();
        }
      }

      final ResultAndTerm r;
      if (others.isEmpty()) {
        r = new ResultAndTerm(Result.PASSED, electionTerm);
      } else {
        try {
          initExecutor();
          int submitted = submitRequests(electionTerm, lastEntry);
          r = waitForResults(electionTerm, submitted);
        } finally {
          if (executor != null) {
            executor.shutdown();
          }
        }
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
            server.close();
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
      final RequestVoteRequestProto r = server.createRequestVoteRequest(
          peer.getId(), electionTerm, lastEntry);
      service.submit(
          () -> server.getServerRpc().requestVote(r));
      submitted++;
    }
    return submitted;
  }

  private ResultAndTerm waitForResults(final long electionTerm,
      final int submitted) throws InterruptedException {
    final Timestamp timeout = new Timestamp().addTimeMs(server.getRandomTimeoutMs());
    final List<RequestVoteReplyProto> responses = new ArrayList<>();
    final List<Exception> exceptions = new ArrayList<>();
    int waitForNum = submitted;
    Collection<RaftPeerId> votedPeers = new ArrayList<>();
    while (waitForNum > 0 && running && server.isCandidate()) {
      final long waitTime = -timeout.elapsedTimeMs();
      if (waitTime <= 0) {
        return logAndReturn(Result.TIMEOUT, responses, exceptions, -1);
      }

      try {
        final Future<RequestVoteReplyProto> future = service.poll(
            waitTime, TimeUnit.MILLISECONDS);
        if (future == null) {
          continue; // poll timeout, continue to return Result.TIMEOUT
        }

        final RequestVoteReplyProto r = future.get();
        responses.add(r);
        if (r.getShouldShutdown()) {
          return logAndReturn(Result.SHUTDOWN, responses, exceptions, -1);
        }
        if (r.getTerm() > electionTerm) {
          return logAndReturn(Result.DISCOVERED_A_NEW_TERM, responses,
              exceptions, r.getTerm());
        }
        if (r.getServerReply().getSuccess()) {
          votedPeers.add(new RaftPeerId(r.getServerReply().getReplyId()));
          if (conf.hasMajority(votedPeers, server.getId())) {
            return logAndReturn(Result.PASSED, responses, exceptions, -1);
          }
        }
      } catch(ExecutionException e) {
        LOG.info("Got exception when requesting votes: " + e);
        LOG.trace("TRACE", e);
        exceptions.add(e);
      }
      waitForNum--;
    }
    // received all the responses
    return logAndReturn(Result.REJECTED, responses, exceptions, -1);
  }
}
