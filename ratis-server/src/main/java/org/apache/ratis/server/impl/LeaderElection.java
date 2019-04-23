/*
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

import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class LeaderElection implements Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);

  private ResultAndTerm logAndReturn(Result result,
      Map<RaftPeerId, RequestVoteReplyProto> responses,
      List<Exception> exceptions, long newTerm) {
    LOG.info(this + ": Election " + result + "; received " + responses.size() + " response(s) "
        + responses.values().stream().map(ServerProtoUtils::toString).collect(Collectors.toList())
        + " and " + exceptions.size() + " exception(s); " + server.getState());
    int i = 0;
    for(Exception e : exceptions) {
      final int j = i++;
      LogUtils.infoOrTrace(LOG, () -> "  Exception " + j, e);
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

  static class Executor {
    private final ExecutorCompletionService<RequestVoteReplyProto> service;
    private final ExecutorService executor;

    private final AtomicInteger count = new AtomicInteger();

    Executor(Object name, int size) {
      Preconditions.assertTrue(size > 0);
      executor = Executors.newFixedThreadPool(size, r -> new Daemon(r, name + "-" + count.incrementAndGet()));
      service = new ExecutorCompletionService<>(executor);
    }

    void shutdown() {
      executor.shutdown();
    }

    void submit(Callable<RequestVoteReplyProto> task) {
      service.submit(task);
    }

    Future<RequestVoteReplyProto> poll(TimeDuration waitTime) throws InterruptedException {
      return service.poll(waitTime.getDuration(), waitTime.getUnit());
    }
  }

  private static final AtomicInteger COUNT = new AtomicInteger();

  private final String name;
  private final LifeCycle lifeCycle;
  private final Daemon daemon;

  private final RaftServerImpl server;

  LeaderElection(RaftServerImpl server) {
    this.name = server.getId() + ":" + server.getGroupId() + ":" + getClass().getSimpleName() + COUNT.incrementAndGet();
    this.lifeCycle = new LifeCycle(this);
    this.daemon = new Daemon(this);
    this.server = server;
  }

  void start() {
    lifeCycle.startAndTransition(daemon::start);
  }

  void shutdown() {
    lifeCycle.checkStateAndClose();
  }

  @Override
  public void run() {
    try {
      askForVotes();
    } catch (InterruptedException e) {
      // the leader election thread is interrupted. The peer may already step
      // down to a follower. The leader election should skip.
      LOG.info("{} thread is interrupted gracefully; server={}", this, server);
    } catch(Throwable e) {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      final String message = "Failed " + this + ", state=" + state;

      if (state.isClosingOrClosed()) {
        LogUtils.infoOrTrace(LOG, message, e);
        LOG.info("{}: {} is safely ignored since this is already {}",
            this, e.getClass().getSimpleName(), state);
      } else {
        if (!server.isAlive()) {
          LogUtils.infoOrTrace(LOG, message, e);
          LOG.info("{}: {} is safely ignored since the server is not alive: {}",
              this, e.getClass().getSimpleName(), server);
        } else {
          LOG.error(message, e);
        }
        shutdown();
      }
    } finally {
      lifeCycle.transition(LifeCycle.State.CLOSED);
    }
  }

  private boolean shouldRun() {
    return lifeCycle.getCurrentState().isRunning() && server.isCandidate() && server.isAlive();
  }

  private boolean shouldRun(long electionTerm) {
    return shouldRun() && server.getState().getCurrentTerm() == electionTerm;
  }

  /**
   * After a peer changes its role to candidate, it invokes this method to
   * send out requestVote rpc to all other peers.
   */
  private void askForVotes() throws InterruptedException, IOException {
    final ServerState state = server.getState();
    while (shouldRun()) {
      // one round of requestVotes
      final long electionTerm;
      final RaftConfiguration conf;
      synchronized (server) {
        electionTerm = state.initElection();
        conf = state.getRaftConf();
        state.persistMetadata();
      }
      LOG.info("{}: begin an election at term {} for {}", this, electionTerm, conf);

      TermIndex lastEntry = state.getLog().getLastEntryTermIndex();
      if (lastEntry == null) {
        // lastEntry may need to be derived from snapshot
        SnapshotInfo snapshot = state.getLatestSnapshot();
        if (snapshot != null) {
          lastEntry = snapshot.getTermIndex();
        }
      }

      final ResultAndTerm r;
      final Collection<RaftPeer> others = conf.getOtherPeers(server.getId());
      if (others.isEmpty()) {
        r = new ResultAndTerm(Result.PASSED, electionTerm);
      } else {
        final Executor voteExecutor = new Executor(this, others.size());
        try {
          final int submitted = submitRequests(electionTerm, lastEntry, others, voteExecutor);
          r = waitForResults(electionTerm, submitted, conf, voteExecutor);
        } finally {
          voteExecutor.shutdown();
        }
      }

      synchronized (server) {
        if (!shouldRun(electionTerm)) {
          return; // term already passed or this should not run anymore.
        }

        switch (r.result) {
          case PASSED:
            server.changeToLeader();
            return;
          case SHUTDOWN:
            LOG.info("{} received shutdown response when requesting votes.", this);
            server.getProxy().close();
            return;
          case REJECTED:
          case DISCOVERED_A_NEW_TERM:
            final long term = Math.max(r.term, state.getCurrentTerm());
            server.changeToFollowerAndPersistMetadata(term, Result.DISCOVERED_A_NEW_TERM);
            return;
          case TIMEOUT:
            // should start another election
        }
      }
    }
  }

  private int submitRequests(final long electionTerm, final TermIndex lastEntry,
      Collection<RaftPeer> others, Executor voteExecutor) {
    int submitted = 0;
    for (final RaftPeer peer : others) {
      final RequestVoteRequestProto r = server.createRequestVoteRequest(
          peer.getId(), electionTerm, lastEntry);
      voteExecutor.submit(() -> server.getServerRpc().requestVote(r));
      submitted++;
    }
    return submitted;
  }

  private ResultAndTerm waitForResults(final long electionTerm, final int submitted,
      RaftConfiguration conf, Executor voteExecutor) throws InterruptedException {
    final Timestamp timeout = Timestamp.currentTime().addTimeMs(server.getRandomTimeoutMs());
    final Map<RaftPeerId, RequestVoteReplyProto> responses = new HashMap<>();
    final List<Exception> exceptions = new ArrayList<>();
    int waitForNum = submitted;
    Collection<RaftPeerId> votedPeers = new ArrayList<>();
    while (waitForNum > 0 && shouldRun(electionTerm)) {
      final TimeDuration waitTime = timeout.elapsedTime().apply(n -> -n);
      if (waitTime.isNonPositive()) {
        return logAndReturn(Result.TIMEOUT, responses, exceptions, -1);
      }

      try {
        final Future<RequestVoteReplyProto> future = voteExecutor.poll(waitTime);
        if (future == null) {
          continue; // poll timeout, continue to return Result.TIMEOUT
        }

        final RequestVoteReplyProto r = future.get();
        final RaftPeerId replierId = RaftPeerId.valueOf(r.getServerReply().getReplyId());
        final RequestVoteReplyProto previous = responses.putIfAbsent(replierId, r);
        if (previous != null) {
          LOG.warn("{} received duplicated replies from {}, the 2nd reply is ignored: 1st = {}, 2nd = {}",
              server.getId(), replierId, ServerProtoUtils.toString(previous), ServerProtoUtils.toString(r));
          continue;
        }
        if (r.getShouldShutdown()) {
          return logAndReturn(Result.SHUTDOWN, responses, exceptions, -1);
        }
        if (r.getTerm() > electionTerm) {
          return logAndReturn(Result.DISCOVERED_A_NEW_TERM, responses,
              exceptions, r.getTerm());
        }
        if (r.getServerReply().getSuccess()) {
          votedPeers.add(replierId);
          if (conf.hasMajority(votedPeers, server.getId())) {
            return logAndReturn(Result.PASSED, responses, exceptions, -1);
          }
        }
      } catch(ExecutionException e) {
        LogUtils.infoOrTrace(LOG, () -> this + " got exception when requesting votes", e);
        exceptions.add(e);
      }
      waitForNum--;
    }
    // received all the responses
    return logAndReturn(Result.REJECTED, responses, exceptions, -1);
  }

  @Override
  public String toString() {
    return name;
  }
}
