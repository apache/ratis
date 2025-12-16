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

import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.TermIndexProto;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
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
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
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

/**
 * For a candidate to start an election for becoming the leader.
 * There are two phases: Pre-Vote and Election.
 * <p>
 * In Pre-Vote, the candidate does not change its term and try to learn
 * if a majority of the cluster would be willing to grant the candidate their votes
 * (if the candidate’s log is sufficiently up-to-date,
 * and the voters have not received heartbeats from a valid leader
 * for at least a baseline election timeout).
 * <p>
 * Once the Pre-Vote has passed, the candidate increments its term and starts a real Election.
 * <p>
 * See
 * Ongaro, D. Consensus: Bridging Theory and Practice. PhD thesis, Stanford University, 2014.
 * Available at https://github.com/ongardie/dissertation
 */
final class LeaderElection implements Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);

  interface ServerInterface {
    default RaftPeerId getId() {
      return getMemberId().getPeerId();
    }

    RaftGroupMemberId getMemberId();
    boolean isAlive();
    boolean isCandidate();

    long getCurrentTerm();
    long getLastCommittedIndex();
    TermIndex getLastEntry();

    boolean isPreVoteEnabled();
    ConfAndTerm initElection(Phase phase) throws IOException;
    RequestVoteReplyProto requestVote(RequestVoteRequestProto r) throws IOException;

    void changeToLeader();
    void rejected(long term, ResultAndTerm result) throws IOException;
    void shutdown();

    Timekeeper getLeaderElectionTimer();
    void onNewLeaderElectionCompletion();

    TimeDuration getRandomElectionTimeout();
    ThreadGroup getThreadGroup();

    static ServerInterface get(RaftServerImpl server) {
      final boolean preVote = RaftServerConfigKeys.LeaderElection.preVote(server.getRaftServer().getProperties());

      return new ServerInterface() {
        @Override
        public RaftGroupMemberId getMemberId() {
          return server.getMemberId();
        }

        @Override
        public boolean isAlive() {
          return server.getInfo().isAlive();
        }

        @Override
        public boolean isCandidate() {
          return server.getInfo().isCandidate();
        }

        @Override
        public long getCurrentTerm() {
          return server.getState().getCurrentTerm();
        }

        @Override
        public long getLastCommittedIndex() {
          return server.getRaftLog().getLastCommittedIndex();
        }

        @Override
        public TermIndex getLastEntry() {
          return server.getState().getLastEntry();
        }

        @Override
        public boolean isPreVoteEnabled() {
          return preVote;
        }

        @Override
        public ConfAndTerm initElection(Phase phase) throws IOException {
          return server.getState().initElection(phase);
        }

        @Override
        public RequestVoteReplyProto requestVote(RequestVoteRequestProto r) throws IOException {
          return server.getServerRpc().requestVote(r);
        }

        @Override
        public void changeToLeader() {
          server.changeToLeader();
        }

        @Override
        public void rejected(long term, ResultAndTerm result) throws IOException {
          server.changeToFollowerAndPersistMetadata(term, false, result);
        }

        @Override
        public void shutdown() {
          server.close();
          server.getStateMachine().event().notifyServerShutdown(server.getRoleInfoProto(), false);
        }

        @Override
        public Timekeeper getLeaderElectionTimer() {
          return server.getLeaderElectionMetrics().getLeaderElectionTimer();
        }

        @Override
        public void onNewLeaderElectionCompletion() {
          server.getLeaderElectionMetrics().onNewLeaderElectionCompletion();
        }

        @Override
        public TimeDuration getRandomElectionTimeout() {
          return server.getRandomElectionTimeout();
        }

        @Override
        public ThreadGroup getThreadGroup() {
          return server.getThreadGroup();
        }
      };
    }
  }

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

  enum Result {PASSED, SINGLE_MODE_PASSED, REJECTED, TIMEOUT, DISCOVERED_A_NEW_TERM, SHUTDOWN, NOT_IN_CONF}

  static class ResultAndTerm {
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
      executor = Executors.newFixedThreadPool(size, r ->
          Daemon.newBuilder().setName(name + "-" + count.incrementAndGet()).setRunnable(r).build());
      service = new ExecutorCompletionService<>(executor);
    }

    void shutdown() {
      executor.shutdownNow();
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
  private final CompletableFuture<Void> stopped = new CompletableFuture<>();

  private final ServerInterface server;
  private final boolean skipPreVote;
  private final ConfAndTerm round0;

  static LeaderElection newInstance(RaftServerImpl server, boolean force) {
    return newInstance(ServerInterface.get(server), force);
  }

  static LeaderElection newInstance(ServerInterface server, boolean force) {
    String name = ServerStringUtils.generateUnifiedName(server.getMemberId(), LeaderElection.class)
            + COUNT.incrementAndGet();
    try {
      // increase term of the candidate in advance if it's forced to election
      final ConfAndTerm round0 = force ? server.initElection(Phase.ELECTION) : null;
      return new LeaderElection(name, server, force, round0);
    } catch (IOException e) {
      throw new IllegalStateException(name + ": Failed to initialize election", e);
    }
  }


  private LeaderElection(String name, ServerInterface server, boolean force, ConfAndTerm round0) {
    this.name = name;
    this.lifeCycle = new LifeCycle(this);
    this.daemon = Daemon.newBuilder().setName(name).setRunnable(this)
        .setThreadGroup(server.getThreadGroup()).build();
    this.server = server;
    this.skipPreVote = force || !server.isPreVoteEnabled();
    this.round0 = round0;
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

  CompletableFuture<Void> shutdown() {
    lifeCycle.checkStateAndClose();
    return stopped;
  }

  @VisibleForTesting
  LifeCycle.State getCurrentState() {
    return lifeCycle.getCurrentState();
  }

  @Override
  public void run() {
    try {
      runImpl();
    } finally {
      stopped.complete(null);
    }
  }

  private void runImpl() {
    if (!lifeCycle.compareAndTransition(STARTING, RUNNING)) {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      LOG.info("{}: skip running since this is already {}", this, state);
      return;
    }

    try (AutoCloseable ignored = Timekeeper.start(server.getLeaderElectionTimer())) {
      for (int round = 0; shouldRun(); round++) {
        if (skipPreVote || askForVotes(Phase.PRE_VOTE, round)) {
          if (askForVotes(Phase.ELECTION, round)) {
            server.changeToLeader();
          }
        }
      }
    } catch(Exception e) {
      if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
      }
      final LifeCycle.State state = lifeCycle.getCurrentState();
      if (state.isClosingOrClosed()) {
        LOG.info("{}: since this is already {}, safely ignore {}", this, state, e.toString());
      } else {
        if (!server.isAlive()) {
          LOG.info("{}: since the server is not alive, safely ignore {}", this, e.toString());
        } else {
          LOG.error("{}: Failed, state={}", this, state, e);
        }
        shutdown();
      }
    } finally {
      // Update leader election completion metric(s).
      server.onNewLeaderElectionCompletion();
      lifeCycle.checkStateAndClose(() -> {});
    }
  }

  private boolean shouldRun() {
    return lifeCycle.getCurrentState().isRunning() && server.isCandidate() && server.isAlive();
  }

  private boolean shouldRun(long electionTerm) {
    return shouldRun() && server.getCurrentTerm() == electionTerm;
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
      final TermIndex lastEntry = server.getLastEntry();
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
  private boolean askForVotes(Phase phase, int round) throws InterruptedException, IOException {
    final long electionTerm;
    final RaftConfigurationImpl conf;
    synchronized (server) {
      if (!shouldRun()) {
        return false;
      }
      // If round0 is non-null, we have already called initElection in the constructor,
      // reuse round0 to avoid initElection again for the first round
      final ConfAndTerm confAndTerm = (round == 0 && round0 != null) ? round0 : server.initElection(phase);
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
        case SINGLE_MODE_PASSED:
          return true;
        case NOT_IN_CONF:
        case SHUTDOWN:
          server.shutdown();
          return false;
        case TIMEOUT:
          return false; // should retry
        case REJECTED:
        case DISCOVERED_A_NEW_TERM:
          final long term = r.maxTerm(server.getCurrentTerm());
          server.rejected(term, r);
          return false;
        default: throw new IllegalArgumentException("Unable to process result " + r.result);
      }
    }
  }

  private int submitRequests(Phase phase, long electionTerm, TermIndex lastEntry,
      Collection<RaftPeer> others, Executor voteExecutor) {
    int submitted = 0;
    for (final RaftPeer peer : others) {
      final RequestVoteRequestProto r = ServerProtoUtils.toRequestVoteRequestProto(
          server.getMemberId(), peer.getId(), electionTerm, lastEntry, phase == Phase.PRE_VOTE);
      voteExecutor.submit(() -> server.requestVote(r));
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
    final boolean singleMode = conf.isSingleMode(server.getId());

    // true iff this server does not have any commits
    final boolean emptyCommit = server.getLastCommittedIndex() < RaftLog.LEAST_VALID_LOG_INDEX;

    while (waitForNum > 0 && shouldRun(electionTerm)) {
      final TimeDuration waitTime = timeout.elapsedTime().apply(n -> -n);
      if (waitTime.isNonPositive()) {
        if (conf.hasMajority(votedPeers, server.getId())) {
          // if some higher priority peer did not response when timeout, but candidate get majority,
          // candidate pass vote
          return logAndReturn(phase, Result.PASSED, responses, exceptions);
        } else if (singleMode) {
          // if candidate is in single mode, candidate pass vote.
          return logAndReturn(phase, Result.SINGLE_MODE_PASSED, responses, exceptions);
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
        if (!r.getServerReply().getSuccess() && higherPriorityPeers.contains(replierId) && !singleMode) {
          return logAndReturn(phase, Result.REJECTED, responses, exceptions);
        }

        // remove higher priority peer, so that we check higherPriorityPeers empty to make sure
        // all higher priority peers have replied
        higherPriorityPeers.remove(replierId);

        final boolean acceptVote = r.getServerReply().getSuccess()
            // When the commits are non-empty, do not accept votes from empty log voters.
            && (emptyCommit || nonEmptyLog(r));
        if (acceptVote) {
          votedPeers.add(replierId);
          // If majority and all peers with higher priority have voted, candidate pass vote
          if (higherPriorityPeers.isEmpty() && conf.hasMajority(votedPeers, server.getId())) {
            return logAndReturn(phase, Result.PASSED, responses, exceptions);
          }
        } else {
          rejectedPeers.add(replierId);
          if (conf.majorityRejectVotes(rejectedPeers)) {
            LOG.info("rejectedPeers: {}, emptyCommit? {}", rejectedPeers, emptyCommit);
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
    } else if (singleMode) {
      return logAndReturn(phase, Result.SINGLE_MODE_PASSED, responses, exceptions);
    } else {
      return logAndReturn(phase, Result.REJECTED, responses, exceptions);
    }
  }

  /**
   * @return true if the given reply indicates that the voter has a non-empty raft log.
   *         Note that a voter running with an old version may not include the lastEntry in the reply.
   *         For compatibility, this method returns true for such case.
   */
  static boolean nonEmptyLog(RequestVoteReplyProto reply) {
    final TermIndexProto lastEntry = reply.getLastEntry();
    // valid term >= 1 and valid index >= 0; therefore, (0, 0) can only be the proto default
    if (lastEntry.equals(TermIndexProto.getDefaultInstance())) { // default: (0,0)
      LOG.info("Reply missing lastEntry: {} ", ServerStringUtils.toRequestVoteReplyString(reply));
      return true; // accept voters with an older version
    }
    if (lastEntry.getTerm() > 0) { // when log is empty, lastEntry is (0,-1).
      return true; // accept voters with a non-empty log
    }

    LOG.info("Replier log is empty: {} ", ServerStringUtils.toRequestVoteReplyString(reply));
    return false; // reject voters with an empty log
  }

  @Override
  public String toString() {
    return name;
  }
}
