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
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
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
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.ratis.util.LifeCycle.State.NEW;
import static org.apache.ratis.util.LifeCycle.State.RUNNING;
import static org.apache.ratis.util.LifeCycle.State.STARTING;

import com.codahale.metrics.Timer;

/**
 * For a candidate to start an election for becoming the leader.
 * There are two phases: Pre-Vote and Election.
 *
 * In Pre-Vote, the candidate does not change its term and try to learn
 * if a majority of the cluster would be willing to grant the candidate their votes
 * (if the candidate’s log is sufficiently up-to-date,
 * and the voters have not received heartbeats from a valid leader
 * for at least a baseline election timeout).
 *
 * Once the Pre-Vote has passed, the candidate increments its term and starts a real Election.
 *
 * See
 * Ongaro, D. Consensus: Bridging Theory and Practice. PhD thesis, Stanford University, 2014.
 * Available at https://github.com/ongardie/dissertation
 */
class LeaderElection implements Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);

  private ResultAndTerm logAndReturn(Phase phase, Result result, Map<RaftPeerId, RequestVoteReplyProto> responses,
      List<Exception> exceptions) {
    return logAndReturn(phase, result, responses, exceptions, null);
  }

  private ResultAndTerm logAndReturn(Phase phase, Result result, Map<RaftPeerId, RequestVoteReplyProto> responses,
      List<Exception> exceptions, Long newTerm) {
    final ResultAndTerm resultAndTerm = new ResultAndTerm(result, newTerm);
    LOG.info("{}: {} {} received {} response(s) and {} exception(s):",
        this, phase, resultAndTerm, responses.size(), exceptions.size());
    int i = 0;
    for(RequestVoteReplyProto reply : responses.values()) {
      LOG.info("  Response {}: {}", i++, ServerStringUtils.toRequestVoteReplyString(reply));
    }
    for(Exception e : exceptions) {
      final int j = i++;
      LogUtils.infoOrTrace(LOG, () -> "  Exception " + j, e);
    }
    return resultAndTerm;
  }

  enum Phase {
    PRE_VOTE,
    ELECTION
  }

  enum Result {PASSED, REJECTED, TIMEOUT, DISCOVERED_A_NEW_TERM, SHUTDOWN, NOT_IN_CONF}

  private static class ResultAndTerm {
    private final Result result;
    private final Long term;

    ResultAndTerm(Result result, Long term) {
      this.result = result;
      this.term = term;
    }

    long maxTerm(long thatTerm) {
      return this.term != null && this.term > thatTerm ? this.term: thatTerm;
    }

    Result getResult() {
      return result;
    }

    @Override
    public String toString() {
      return result + (term != null? " (term=" + term + ")": "");
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

  static class ConfAndTerm {
    private final RaftConfigurationImpl conf;
    private final long term;

    ConfAndTerm(RaftConfigurationImpl conf, long term) {
      this.conf = conf;
      this.term = term;
    }

    long getTerm() {
      return term;
    }

    RaftConfigurationImpl getConf() {
      return conf;
    }

    @Override
    public String toString() {
      return "term=" + term + ", " + conf;
    }
  }

  private static final AtomicInteger COUNT = new AtomicInteger();

  private final String name;
  private final LifeCycle lifeCycle;
  private final Daemon daemon;

  private final RaftServerImpl server;
  private final boolean skipPreVote;

  LeaderElection(RaftServerImpl server, boolean skipPreVote) {
    this.name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass()) + COUNT.incrementAndGet();
    this.lifeCycle = new LifeCycle(this);
    this.daemon = new Daemon(this);
    this.server = server;
    this.skipPreVote = skipPreVote ||
        !RaftServerConfigKeys.LeaderElection.preVote(
            server.getRaftServer().getProperties());
  }

  void start() {
    startIfNew(daemon::start);
  }

  @VisibleForTesting
  void startInForeground() {
    startIfNew(this);
  }

  private void startIfNew(Runnable starter) {
    if (lifeCycle.compareAndTransition(NEW, STARTING)) {
      starter.run();
    } else {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      LOG.info("{}: skip starting since this is already {}", this, state);
    }
  }

  void shutdown() {
    lifeCycle.checkStateAndClose();
  }

  @VisibleForTesting
  LifeCycle.State getCurrentState() {
    return lifeCycle.getCurrentState();
  }

  @Override
  public void run() {
    if (!lifeCycle.compareAndTransition(STARTING, RUNNING)) {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      LOG.info("{}: skip running since this is already {}", this, state);
      return;
    }

    final Timer.Context electionContext = server.getLeaderElectionMetrics().getLeaderElectionTimer().time();
    try {
      if (skipPreVote || askForVotes(Phase.PRE_VOTE)) {
        if (askForVotes(Phase.ELECTION)) {
          server.changeToLeader();
        }
      }
    } catch(Exception e) {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      if (state.isClosingOrClosed()) {
        LOG.info("{}: {} is safely ignored since this is already {}",
            this, JavaUtils.getClassSimpleName(e.getClass()), state, e);
      } else {
        if (!server.getInfo().isAlive()) {
          LOG.info("{}: {} is safely ignored since the server is not alive: {}",
              this, JavaUtils.getClassSimpleName(e.getClass()), server, e);
        } else {
          LOG.error("{}: Failed, state={}", this, state, e);
        }
        shutdown();
      }
    } finally {
      // Update leader election completion metric(s).
      electionContext.stop();
      server.getLeaderElectionMetrics().onNewLeaderElectionCompletion();
      lifeCycle.checkStateAndClose(() -> {});
    }
  }

  private boolean shouldRun() {
    final DivisionInfo info = server.getInfo();
    return lifeCycle.getCurrentState().isRunning() && info.isCandidate() && info.isAlive();
  }

  private boolean shouldRun(long electionTerm) {
    return shouldRun() && server.getState().getCurrentTerm() == electionTerm;
  }

  private ResultAndTerm submitRequestAndWaitResult(Phase phase, RaftConfigurationImpl conf, long electionTerm)
      throws InterruptedException {
    if (!conf.containsInConf(server.getId())) {
      return new ResultAndTerm(Result.NOT_IN_CONF, electionTerm);
    }
    final ResultAndTerm r;
    final Collection<RaftPeer> others = conf.getOtherPeers(server.getId());
    if (others.isEmpty()) {
      r = new ResultAndTerm(Result.PASSED, electionTerm);
    } else {
      final TermIndex lastEntry = server.getState().getLastEntry();
      final Executor voteExecutor = new Executor(this, others.size());
      try {
        final int submitted = submitRequests(phase, electionTerm, lastEntry, others, voteExecutor);
        r = waitForResults(phase, electionTerm, submitted, conf, voteExecutor);
      } finally {
        voteExecutor.shutdown();
      }
    }

    return r;
  }

  /** Send requestVote rpc to all other peers for the given phase. */
  private boolean askForVotes(Phase phase) throws InterruptedException, IOException {
    for(int round = 0; shouldRun(); round++) {
      final long electionTerm;
      final RaftConfigurationImpl conf;
      synchronized (server) {
        if (!shouldRun()) {
          return false;
        }
        final ConfAndTerm confAndTerm = server.getState().initElection(phase);
        electionTerm = confAndTerm.getTerm();
        conf = confAndTerm.getConf();
      }

      LOG.info("{} {} round {}: submit vote requests at term {} for {}", this, phase, round, electionTerm, conf);
      final ResultAndTerm r = submitRequestAndWaitResult(phase, conf, electionTerm);
      LOG.info("{} {} round {}: result {}", this, phase, round, r);

      synchronized (server) {
        if (!shouldRun(electionTerm)) {
          return false; // term already passed or this should not run anymore.
        }

        switch (r.getResult()) {
          case PASSED:
            return true;
          case NOT_IN_CONF:
          case SHUTDOWN:
            server.getRaftServer().close();
            return false;
          case TIMEOUT:
            continue; // should retry
          case REJECTED:
          case DISCOVERED_A_NEW_TERM:
            final long term = r.maxTerm(server.getState().getCurrentTerm());
            server.changeToFollowerAndPersistMetadata(term, r);
            return false;
          default: throw new IllegalArgumentException("Unable to process result " + r.result);
        }
      }
    }
    return false;
  }

  private int submitRequests(Phase phase, long electionTerm, TermIndex lastEntry,
      Collection<RaftPeer> others, Executor voteExecutor) {
    int submitted = 0;
    for (final RaftPeer peer : others) {
      final RequestVoteRequestProto r = ServerProtoUtils.toRequestVoteRequestProto(
          server.getMemberId(), peer.getId(), electionTerm, lastEntry, phase == Phase.PRE_VOTE);
      voteExecutor.submit(() -> server.getServerRpc().requestVote(r));
      submitted++;
    }
    return submitted;
  }

  private Set<RaftPeerId> getHigherPriorityPeers(RaftConfiguration conf) {
    final Optional<Integer> priority = Optional.ofNullable(conf.getPeer(server.getId()))
        .map(RaftPeer::getPriority);
    return conf.getAllPeers().stream()
        .filter(peer -> priority.filter(p -> peer.getPriority() > p).isPresent())
        .map(RaftPeer::getId)
        .collect(Collectors.toSet());
  }

  private ResultAndTerm waitForResults(Phase phase, long electionTerm, int submitted,
      RaftConfigurationImpl conf, Executor voteExecutor) throws InterruptedException {
    final Timestamp timeout = Timestamp.currentTime().addTime(server.getRandomElectionTimeout());
    final Map<RaftPeerId, RequestVoteReplyProto> responses = new HashMap<>();
    final List<Exception> exceptions = new ArrayList<>();
    int waitForNum = submitted;
    Collection<RaftPeerId> votedPeers = new ArrayList<>();
    Collection<RaftPeerId> rejectedPeers = new ArrayList<>();
    Set<RaftPeerId> higherPriorityPeers = getHigherPriorityPeers(conf);

    while (waitForNum > 0 && shouldRun(electionTerm)) {
      final TimeDuration waitTime = timeout.elapsedTime().apply(n -> -n);
      if (waitTime.isNonPositive()) {
        if (conf.hasMajority(votedPeers, server.getId())) {
          // if some higher priority peer did not response when timeout, but candidate get majority,
          // candidate pass vote
          return logAndReturn(phase, Result.PASSED, responses, exceptions);
        } else {
          return logAndReturn(phase, Result.TIMEOUT, responses, exceptions);
        }
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
          if (LOG.isWarnEnabled()) {
            LOG.warn("{} received duplicated replies from {}, the 2nd reply is ignored: 1st={}, 2nd={}",
                this, replierId,
                ServerStringUtils.toRequestVoteReplyString(previous),
                ServerStringUtils.toRequestVoteReplyString(r));
          }
          continue;
        }
        if (r.getShouldShutdown()) {
          return logAndReturn(phase, Result.SHUTDOWN, responses, exceptions);
        }
        if (r.getTerm() > electionTerm) {
          return logAndReturn(phase, Result.DISCOVERED_A_NEW_TERM, responses, exceptions, r.getTerm());
        }

        // If any peer with higher priority rejects vote, candidate can not pass vote
        if (!r.getServerReply().getSuccess() && higherPriorityPeers.contains(replierId)) {
          return logAndReturn(phase, Result.REJECTED, responses, exceptions);
        }

        // remove higher priority peer, so that we check higherPriorityPeers empty to make sure
        // all higher priority peers have replied
        higherPriorityPeers.remove(replierId);

        if (r.getServerReply().getSuccess()) {
          votedPeers.add(replierId);
          // If majority and all peers with higher priority have voted, candidate pass vote
          if (higherPriorityPeers.size() == 0 && conf.hasMajority(votedPeers, server.getId())) {
            return logAndReturn(phase, Result.PASSED, responses, exceptions);
          }
        } else {
          rejectedPeers.add(replierId);
          if (conf.majorityRejectVotes(rejectedPeers)) {
            return logAndReturn(phase, Result.REJECTED, responses, exceptions);
          }
        }
      } catch(ExecutionException e) {
        LogUtils.infoOrTrace(LOG, () -> this + " got exception when requesting votes", e);
        exceptions.add(e);
      }
      waitForNum--;
    }
    // received all the responses
    if (conf.hasMajority(votedPeers, server.getId())) {
      return logAndReturn(phase, Result.PASSED, responses, exceptions);
    } else {
      return logAndReturn(phase, Result.REJECTED, responses, exceptions);
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
