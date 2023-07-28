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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto.LogEntryBodyCase;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.protocol.exceptions.ReconfigurationTimeoutException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.ReadIndexHeartbeats.AppendEntriesListener;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.metrics.LogAppenderMetrics;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.ratis.server.RaftServer.Division.LOG;
import static org.apache.ratis.server.RaftServerConfigKeys.Write.FOLLOWER_GAP_RATIO_MAX_KEY;

/**
 * States for leader only. It contains three different types of processors:
 * 1. RPC senders: each thread is appending log to a follower
 * 2. EventProcessor: a single thread updating the raft server's state based on
 *                    status of log appending response
 * 3. PendingRequestHandler: a handler sending back responses to clients when
 *                           corresponding log entries are committed
 */
class LeaderStateImpl implements LeaderState {
  public static final String APPEND_PLACEHOLDER = JavaUtils.getClassSimpleName(LeaderState.class) + ".placeholder";

  private enum BootStrapProgress {
    NOPROGRESS, PROGRESSING, CAUGHTUP
  }

  static class StateUpdateEvent {
    private enum Type {
      STEP_DOWN, UPDATE_COMMIT, CHECK_STAGING
    }

    private final Type type;
    private final long newTerm;
    private final Runnable handler;

    StateUpdateEvent(Type type, long newTerm, Runnable handler) {
      this.type = type;
      this.newTerm = newTerm;
      this.handler = handler;
    }

    void execute() {
      handler.run();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof StateUpdateEvent)) {
        return false;
      }
      final StateUpdateEvent that = (StateUpdateEvent)obj;
      return this.type == that.type && this.newTerm == that.newTerm;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, newTerm);
    }

    @Override
    public String toString() {
      return type + (newTerm >= 0? ":" + newTerm: "");
    }
  }

  private class EventQueue {
    private final String name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    private final BlockingQueue<StateUpdateEvent> queue = new ArrayBlockingQueue<>(4096);

    void submit(StateUpdateEvent event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        LOG.info("{}: Interrupted when submitting {} ", this, event);
        Thread.currentThread().interrupt();
      }
    }

    StateUpdateEvent poll() {
      final StateUpdateEvent e;
      try {
        e = queue.poll(server.getMaxTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        String s = this + ": poll() is interrupted";
        if (isStopped.get()) {
          LOG.info(s + " gracefully");
          return null;
        } else {
          throw new IllegalStateException(s + " UNEXPECTEDLY", ie);
        }
      }

      if (e != null) {
        // remove duplicated events from the head.
        while(e.equals(queue.peek())) {
          queue.poll();
        }
      }
      return e;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Use {@link CopyOnWriteArrayList} to implement a thread-safe list.
   * Since each mutation induces a copy of the list, only bulk operations
   * (addAll and removeAll) are supported.
   */
  static class SenderList implements Iterable<LogAppender> {
    private final List<LogAppender> senders;

    SenderList() {
      this.senders = new CopyOnWriteArrayList<>();
    }

    @Override
    public Iterator<LogAppender> iterator() {
      return senders.iterator();
    }

    void addAll(Collection<LogAppender> newSenders) {
      if (newSenders.isEmpty()) {
        return;
      }

      Preconditions.assertUnique(
          CollectionUtils.as(senders, LogAppender::getFollowerId),
          CollectionUtils.as(newSenders, LogAppender::getFollowerId));

      final boolean changed = senders.addAll(newSenders);
      Preconditions.assertTrue(changed);
    }

    boolean removeAll(Collection<LogAppender> c) {
      return senders.removeAll(c);
    }
  }

  /** For caching {@link FollowerInfo}s.  This class is immutable. */
  static class CurrentOldFollowerInfos {
    private final RaftConfigurationImpl conf;
    private final List<FollowerInfo> current;
    private final List<FollowerInfo> old;

    CurrentOldFollowerInfos(RaftConfigurationImpl conf, List<FollowerInfo> current, List<FollowerInfo> old) {
      // set null when the sizes are not the same so that it will update next time.
      this.conf = isSameSize(current, conf.getConf()) && isSameSize(old, conf.getOldConf())? conf: null;
      this.current = Collections.unmodifiableList(current);
      this.old = old == null? null: Collections.unmodifiableList(old);
    }

    RaftConfigurationImpl getConf() {
      return conf;
    }

    List<FollowerInfo> getCurrent() {
      return current;
    }

    List<FollowerInfo> getOld() {
      return old;
    }
  }

  static boolean isSameSize(List<FollowerInfo> infos, PeerConfiguration conf) {
    return conf == null? infos == null: conf.size() == infos.size();
  }

  /** Use == to compare if the confs are the same object. */
  static boolean isSameConf(CurrentOldFollowerInfos cached, RaftConfigurationImpl conf) {
    return cached != null && cached.getConf() == conf;
  }

  static class FollowerInfoMap {
    private final Map<RaftPeerId, FollowerInfo> map = new ConcurrentHashMap<>();

    private volatile CurrentOldFollowerInfos followerInfos;

    void put(RaftPeerId id, FollowerInfo info) {
      map.put(id, info);
    }

    CurrentOldFollowerInfos getFollowerInfos(RaftConfigurationImpl conf) {
      final CurrentOldFollowerInfos cached = followerInfos;
      if (isSameConf(cached, conf)) {
        return cached;
      }

      return update(conf);
    }

    synchronized CurrentOldFollowerInfos update(RaftConfigurationImpl conf) {
      if (!isSameConf(followerInfos, conf)) { // compare again synchronized
        followerInfos = new CurrentOldFollowerInfos(conf, getFollowerInfos(conf.getConf()),
            Optional.ofNullable(conf.getOldConf()).map(this::getFollowerInfos).orElse(null));
      }
      return followerInfos;
    }

    private List<FollowerInfo> getFollowerInfos(PeerConfiguration peers) {
      return peers.streamPeerIds().map(map::get).filter(Objects::nonNull).collect(Collectors.toList());
    }
  }

  private final StateUpdateEvent updateCommitEvent =
      new StateUpdateEvent(StateUpdateEvent.Type.UPDATE_COMMIT, -1, this::updateCommit);
  private final StateUpdateEvent checkStagingEvent =
      new StateUpdateEvent(StateUpdateEvent.Type.CHECK_STAGING, -1, this::checkStaging);

  private final String name;
  private final RaftServerImpl server;
  private final RaftLog raftLog;
  private final long currentTerm;
  private volatile ConfigurationStagingState stagingState;

  private final FollowerInfoMap followerInfoMap = new FollowerInfoMap();

  /**
   * The list of threads appending entries to followers.
   * The list is protected by the RaftServer's lock.
   */
  private final SenderList senders;
  private final EventQueue eventQueue;
  private final EventProcessor processor;
  private final PendingRequests pendingRequests;
  private final WatchRequests watchRequests;
  private final MessageStreamRequests messageStreamRequests;
  private final AtomicBoolean isStopped = new AtomicBoolean();

  private final int stagingCatchupGap;
  private final long placeHolderIndex;
  private final RaftServerMetricsImpl raftServerMetrics;
  private final LogAppenderMetrics logAppenderMetrics;
  private final long followerMaxGapThreshold;
  private final PendingStepDown pendingStepDown;

  private final ReadIndexHeartbeats readIndexHeartbeats;

  LeaderStateImpl(RaftServerImpl server) {
    this.name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.server = server;

    final RaftProperties properties = server.getRaftServer().getProperties();
    stagingCatchupGap = RaftServerConfigKeys.stagingCatchupGap(properties);

    final ServerState state = server.getState();
    this.raftLog = state.getLog();
    this.currentTerm = state.getCurrentTerm();

    this.eventQueue = new EventQueue();
    processor = new EventProcessor(this.name, server);
    raftServerMetrics = server.getRaftServerMetrics();
    logAppenderMetrics = new LogAppenderMetrics(server.getMemberId());
    this.pendingRequests = new PendingRequests(server.getMemberId(), properties, raftServerMetrics);
    this.watchRequests = new WatchRequests(server.getMemberId(), properties);
    this.messageStreamRequests = new MessageStreamRequests(server.getMemberId());
    this.pendingStepDown = new PendingStepDown(this);
    this.readIndexHeartbeats = new ReadIndexHeartbeats();
    long maxPendingRequests = RaftServerConfigKeys.Write.elementLimit(properties);
    double followerGapRatioMax = RaftServerConfigKeys.Write.followerGapRatioMax(properties);

    if (followerGapRatioMax == -1) {
      this.followerMaxGapThreshold = -1;
    } else if (followerGapRatioMax > 1f || followerGapRatioMax <= 0f) {
      throw new IllegalArgumentException(FOLLOWER_GAP_RATIO_MAX_KEY +
          "s value must between [1, 0) to enable the feature");
    } else {
      this.followerMaxGapThreshold = (long) (followerGapRatioMax * maxPendingRequests);
    }

    final RaftConfigurationImpl conf = state.getRaftConf();
    Collection<RaftPeer> others = conf.getOtherPeers(server.getId());
    placeHolderIndex = raftLog.getNextIndex();

    senders = new SenderList();
    addSenders(others, placeHolderIndex, true);

    final Collection<RaftPeer> listeners = conf.getAllPeers(RaftPeerRole.LISTENER);
    if (!listeners.isEmpty()) {
      addSenders(listeners, placeHolderIndex, true);
    }
  }

  LogEntryProto start() {
    // In the beginning of the new term, replicate a conf entry in order
    // to finally commit entries in the previous term.
    // Also this message can help identify the last committed index and the conf.
    final LogEntryProto placeHolder = LogProtoUtils.toLogEntryProto(
        server.getRaftConf(), server.getState().getCurrentTerm(), raftLog.getNextIndex());
    CodeInjectionForTesting.execute(APPEND_PLACEHOLDER,
        server.getId().toString(), null);
    raftLog.append(Collections.singletonList(placeHolder));
    processor.start();
    senders.forEach(LogAppender::start);
    return placeHolder;
  }

  boolean isReady() {
    return server.getState().getLastAppliedIndex() >= placeHolderIndex;
  }

  void stop() {
    if (!isStopped.compareAndSet(false, true)) {
      LOG.info("{} is already stopped", this);
      return;
    }
    // do not interrupt event processor since it may be in the middle of logSync
    senders.forEach(LogAppender::stop);
    final NotLeaderException nle = server.generateNotLeaderException();
    final Collection<CommitInfoProto> commitInfos = server.getCommitInfos();
    try {
      final Collection<TransactionContext> transactions = pendingRequests.sendNotLeaderResponses(nle, commitInfos);
      server.getStateMachine().leaderEvent().notifyNotLeader(transactions);
      watchRequests.failWatches(nle);
    } catch (IOException e) {
      LOG.warn("{}: Caught exception in sendNotLeaderResponses", this, e);
    }
    messageStreamRequests.clear();
    // TODO client should retry on NotLeaderException
    readIndexHeartbeats.failListeners(nle);
    server.getServerRpc().notifyNotLeader(server.getMemberId().getGroupId());
    logAppenderMetrics.unregister();
    raftServerMetrics.unregister();
    pendingRequests.close();
  }

  void notifySenders() {
    senders.forEach(LogAppender::notifyLogAppender);
  }

  boolean inStagingState() {
    return stagingState != null;
  }

  long getCurrentTerm() {
    return currentTerm;
  }

  TermIndex getLastEntry() {
    return server.getState().getLastEntry();
  }

  @Override
  public boolean onFollowerTerm(FollowerInfo follower, long followerTerm) {
    if (isCaughtUp(follower) && followerTerm > getCurrentTerm()) {
      submitStepDownEvent(followerTerm, StepDownReason.HIGHER_TERM);
      return true;
    }
    return false;
  }

  /**
   * Start bootstrapping new peers
   */
  PendingRequest startSetConfiguration(SetConfigurationRequest request, List<RaftPeer> peersInNewConf) {
    LOG.info("{}: startSetConfiguration {}", this, request);
    Preconditions.assertTrue(isRunning(), () -> this + " is not running.");
    Preconditions.assertTrue(!inStagingState(), () -> this + " is already in staging state " + stagingState);

    final List<RaftPeer> listenersInNewConf = request.getArguments().getPeersInNewConf(RaftPeerRole.LISTENER);
    final Collection<RaftPeer> peersToBootStrap = server.getRaftConf().filterNotContainedInConf(peersInNewConf);
    final Collection<RaftPeer> listenersToBootStrap= server.getRaftConf().filterNotContainedInConf(listenersInNewConf);

    // add the request to the pending queue
    final PendingRequest pending = pendingRequests.addConfRequest(request);

    ConfigurationStagingState configurationStagingState = new ConfigurationStagingState(
        peersToBootStrap, listenersToBootStrap, new PeerConfiguration(peersInNewConf, listenersInNewConf));
    Collection<RaftPeer> newPeers = configurationStagingState.getNewPeers();
    Collection<RaftPeer> newListeners = configurationStagingState.getNewListeners();
    // set the staging state
    this.stagingState = configurationStagingState;

    if (newPeers.isEmpty() && newListeners.isEmpty()) {
      applyOldNewConf();
    } else {
      // update the LeaderState's sender list
      addAndStartSenders(newPeers);
      addAndStartSenders(newListeners);
    }
    return pending;
  }

  PendingRequests.Permit tryAcquirePendingRequest(Message message) {
    return pendingRequests.tryAcquire(message);
  }

  PendingRequest addPendingRequest(PendingRequests.Permit permit, RaftClientRequest request, TransactionContext entry) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: addPendingRequest at {}, entry={}", this, request,
          LogProtoUtils.toLogEntryString(entry.getLogEntry()));
    }
    return pendingRequests.add(permit, request, entry);
  }

  CompletableFuture<RaftClientReply> streamAsync(RaftClientRequest request) {
    return messageStreamRequests.streamAsync(request)
        .thenApply(dummy -> server.newSuccessReply(request))
        .exceptionally(e -> exception2RaftClientReply(request, e));
  }

  CompletableFuture<RaftClientRequest> streamEndOfRequestAsync(RaftClientRequest request) {
    return messageStreamRequests.streamEndOfRequestAsync(request)
        .thenApply(bytes -> RaftClientRequest.toWriteRequest(request, Message.valueOf(bytes)));
  }

  CompletableFuture<RaftClientReply> addWatchReqeust(RaftClientRequest request) {
    LOG.debug("{}: addWatchRequest {}", this, request);
    return watchRequests.add(request)
        .thenApply(logIndex -> server.newSuccessReply(request, logIndex))
        .exceptionally(e -> exception2RaftClientReply(request, e));
  }

  private RaftClientReply exception2RaftClientReply(RaftClientRequest request, Throwable e) {
    e = JavaUtils.unwrapCompletionException(e);
    if (e instanceof NotReplicatedException) {
      final NotReplicatedException nre = (NotReplicatedException)e;
      return server.newReplyBuilder(request)
          .setException(nre)
          .setLogIndex(nre.getLogIndex())
          .build();
    } else if (e instanceof NotLeaderException) {
      return server.newExceptionReply(request, (NotLeaderException)e);
    } else if (e instanceof LeaderNotReadyException) {
      return server.newExceptionReply(request, (LeaderNotReadyException)e);
    } else {
      throw new CompletionException(e);
    }
  }

  @Override
  public void onFollowerCommitIndex(FollowerInfo follower, long commitIndex) {
    if (follower.updateCommitIndex(commitIndex)) {
      commitIndexChanged();
    }
  }

  private void commitIndexChanged() {
    getMajorityMin(FollowerInfo::getCommitIndex, raftLog::getLastCommittedIndex).ifPresent(m -> {
      // Normally, leader commit index is always ahead of followers.
      // However, after a leader change, the new leader commit index may
      // be behind some followers in the beginning.
      watchRequests.update(ReplicationLevel.ALL_COMMITTED, m.min);
      watchRequests.update(ReplicationLevel.MAJORITY_COMMITTED, m.majority);
      watchRequests.update(ReplicationLevel.MAJORITY, m.max);
    });
    notifySenders();
  }

  private void applyOldNewConf() {
    final ServerState state = server.getState();
    final RaftConfigurationImpl current = state.getRaftConf();
    final RaftConfigurationImpl oldNewConf= stagingState.generateOldNewConf(current, state.getLog().getNextIndex());
    // apply the (old, new) configuration to log, and use it as the current conf
    long index = state.getLog().append(state.getCurrentTerm(), oldNewConf);
    updateConfiguration(index, oldNewConf);

    this.stagingState = null;
    notifySenders();
  }

  private void updateConfiguration(long logIndex, RaftConfigurationImpl newConf) {
    Preconditions.assertTrue(logIndex == newConf.getLogEntryIndex());
    server.getState().setRaftConf(newConf);
  }

  void updateFollowerCommitInfos(CommitInfoCache cache, List<CommitInfoProto> protos) {
    for (LogAppender sender : senders) {
      FollowerInfo info = sender.getFollower();
      protos.add(cache.update(info.getPeer(), info.getCommitIndex()));
    }
  }

  @Override
  public AppendEntriesRequestProto newAppendEntriesRequestProto(FollowerInfo follower,
      List<LogEntryProto> entries, TermIndex previous, long callId) {
    final boolean initializing = isCaughtUp(follower);
    final RaftPeerId targetId = follower.getId();
    return ServerProtoUtils.toAppendEntriesRequestProto(server.getMemberId(), targetId, currentTerm, entries,
        ServerImplUtils.effectiveCommitIndex(raftLog.getLastCommittedIndex(), previous, entries.size()),
        initializing, previous, server.getCommitInfos(), callId);
  }

  /**
   * Update sender list for setConfiguration request
   */
  private void addAndStartSenders(Collection<RaftPeer> newPeers) {
    if (!newPeers.isEmpty()) {
      addSenders(newPeers, RaftLog.LEAST_VALID_LOG_INDEX, false).forEach(LogAppender::start);
    }
  }

  private RaftPeer getPeer(RaftPeerId id) {
    return server.getRaftConf().getPeer(id, RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER);
  }

  private Collection<LogAppender> addSenders(Collection<RaftPeer> newPeers, long nextIndex, boolean caughtUp) {
    final Timestamp t = Timestamp.currentTime().addTimeMs(-server.getMaxTimeoutMs());
    final List<LogAppender> newAppenders = newPeers.stream().map(peer -> {
      final FollowerInfo f = new FollowerInfoImpl(server.getMemberId(), peer, this::getPeer, t, nextIndex, caughtUp);
      followerInfoMap.put(peer.getId(), f);
      raftServerMetrics.addFollower(peer.getId());
      logAppenderMetrics.addFollowerGauges(peer.getId(), f::getNextIndex, f::getMatchIndex, f::getLastRpcTime);
      return server.newLogAppender(this, f);
    }).collect(Collectors.toList());
    senders.addAll(newAppenders);
    return newAppenders;
  }

  private void stopAndRemoveSenders(Predicate<LogAppender> predicate) {
    stopAndRemoveSenders(getLogAppenders().filter(predicate).collect(Collectors.toList()));
  }

  private void stopAndRemoveSenders(Collection<LogAppender> toStop) {
    toStop.forEach(LogAppender::stop);
    senders.removeAll(toStop);
  }

  boolean isRunning() {
    if (isStopped.get()) {
      return false;
    }
    final LeaderStateImpl current = server.getRole().getLeaderState().orElse(null);
    return this == current;
  }

  @Override
  public void restart(LogAppender sender) {
    if (!isRunning()) {
      LOG.warn("Failed to restart {}: {} is not running", sender, this);
      return;
    }

    final FollowerInfo info = sender.getFollower();
    LOG.info("{}: Restarting {} for {}", this, JavaUtils.getClassSimpleName(sender.getClass()), info.getName());
    stopAndRemoveSenders(Collections.singleton(sender));

    Optional.ofNullable(getPeer(info.getId()))
        .ifPresent(peer -> addAndStartSenders(Collections.singleton(peer)));
  }

  /**
   * Update the RpcSender list based on the current configuration
   */
  private void updateSenders(RaftConfigurationImpl conf) {
    Preconditions.assertTrue(conf.isStable() && !inStagingState());
    stopAndRemoveSenders(s -> !conf.containsInConf(s.getFollowerId(), RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER));
  }

  void submitStepDownEvent(StepDownReason reason) {
    submitStepDownEvent(getCurrentTerm(), reason);
  }

  void submitStepDownEvent(long term, StepDownReason reason) {
    eventQueue.submit(new StateUpdateEvent(StateUpdateEvent.Type.STEP_DOWN, term, () -> stepDown(term, reason)));
  }

  private void stepDown(long term, StepDownReason reason) {
    try {
      server.changeToFollowerAndPersistMetadata(term, false, reason);
      pendingStepDown.complete(server::newSuccessReply);
    } catch(IOException e) {
      final String s = this + ": Failed to persist metadata for term " + term;
      LOG.warn(s, e);
      // the failure should happen while changing the state to follower
      // thus the in-memory state should have been updated
      if (!isStopped.get()) {
        throw new IllegalStateException(s + " and running == true", e);
      }
    }
  }

  CompletableFuture<RaftClientReply> submitStepDownRequestAsync(TransferLeadershipRequest request) {
    return pendingStepDown.submitAsync(request);
  }

  private static LogAppender chooseUpToDateFollower(List<LogAppender> followers, TermIndex leaderLastEntry) {
    for(LogAppender f : followers) {
      if (TransferLeadership.isFollowerUpToDate(f.getFollower(), leaderLastEntry)
          == TransferLeadership.Result.SUCCESS) {
        return f;
      }
    }
    return null;
  }

  private void prepare() {
    synchronized (server) {
      if (isRunning()) {
        final ServerState state = server.getState();
        if (state.getRaftConf().isTransitional() && state.isConfCommitted()) {
          // the configuration is in transitional state, and has been committed
          // so it is time to generate and replicate (new) conf.
          replicateNewConf();
        }
      }
    }
  }

  /**
   * The processor thread takes the responsibility to update the raft server's
   * state, such as changing to follower, or updating the committed index.
   */
  private class EventProcessor extends Daemon {
    public EventProcessor(String name, RaftServerImpl server) {
      super(Daemon.newBuilder()
          .setName(name).setThreadGroup(server.getThreadGroup()));
    }
    @Override
    public void run() {
      // apply an empty message; check if necessary to replicate (new) conf
      prepare();

      while (isRunning()) {
        final StateUpdateEvent event = eventQueue.poll();
        synchronized(server) {
          if (isRunning()) {
            if (event != null) {
              event.execute();
            } else if (inStagingState()) {
              checkStaging();
            } else if (checkLeadership()) {
              checkPeersForYieldingLeader();
            }
          }
        }
      }
    }
  }

  /**
   * So far we use a simple implementation for catchup checking:
   * 1. If the latest rpc time of the remote peer is before 3 * max_timeout,
   *    the peer made no progress for that long. We should fail the whole
   *    setConfiguration request.
   * 2. If the peer's matching index is just behind for a small gap, and the
   *    peer was updated recently (within max_timeout), declare the peer as
   *    caught-up.
   * 3. Otherwise the peer is making progressing. Keep waiting.
   */
  private BootStrapProgress checkProgress(FollowerInfo follower, long committed) {
    Preconditions.assertTrue(!isCaughtUp(follower));
    final Timestamp progressTime = Timestamp.currentTime().addTimeMs(-server.getMaxTimeoutMs());
    final Timestamp timeoutTime = Timestamp.currentTime().addTimeMs(-3L * server.getMaxTimeoutMs());
    if (follower.getLastRpcResponseTime().compareTo(timeoutTime) < 0) {
      LOG.debug("{} detects a follower {} timeout ({}ms) for bootstrapping", this, follower,
          follower.getLastRpcResponseTime().elapsedTimeMs());
      return BootStrapProgress.NOPROGRESS;
    } else if (follower.getMatchIndex() + stagingCatchupGap > committed
        && follower.getLastRpcResponseTime().compareTo(progressTime) > 0
        && follower.hasAttemptedToInstallSnapshot()) {
      return BootStrapProgress.CAUGHTUP;
    } else {
      return BootStrapProgress.PROGRESSING;
    }
  }

  @Override
  public void onFollowerSuccessAppendEntries(FollowerInfo follower) {
    if (isCaughtUp(follower)) {
      submitUpdateCommitEvent();
    } else {
      eventQueue.submit(checkStagingEvent);
    }
    server.getTransferLeadership().onFollowerAppendEntriesReply(follower);
  }

  @Override
  public boolean isFollowerBootstrapping(FollowerInfo follower) {
    return isBootStrappingPeer(follower.getId());
  }

  private void checkStaging() {
    if (!inStagingState()) {
      // it is possible that the bootstrapping is done. Then, fallback to UPDATE_COMMIT
      updateCommitEvent.execute();
    } else {
      final long commitIndex = server.getState().getLog().getLastCommittedIndex();
      // check progress for the new followers
      final EnumSet<BootStrapProgress> reports = getLogAppenders()
          .map(LogAppender::getFollower)
          .filter(follower -> !isCaughtUp(follower))
          .map(follower -> checkProgress(follower, commitIndex))
          .collect(Collectors.toCollection(() -> EnumSet.noneOf(BootStrapProgress.class)));
      if (reports.contains(BootStrapProgress.NOPROGRESS)) {
        stagingState.fail(BootStrapProgress.NOPROGRESS);
      } else if (!reports.contains(BootStrapProgress.PROGRESSING)) {
        // all caught up!
        applyOldNewConf();
        getLogAppenders()
            .map(LogAppender::getFollower)
            .filter(f -> server.getRaftConf().containsInConf(f.getId()))
            .map(FollowerInfoImpl.class::cast)
            .forEach(FollowerInfoImpl::catchUp);
      }
    }
  }

  boolean isBootStrappingPeer(RaftPeerId peerId) {
    return Optional.ofNullable(stagingState).map(s -> s.contains(peerId)).orElse(false);
  }

  void submitUpdateCommitEvent() {
    eventQueue.submit(updateCommitEvent);
  }

  static class MinMajorityMax {
    private final long min;
    private final long majority;
    private final long max;

    MinMajorityMax(long min, long majority, long max) {
      this.min = min;
      this.majority = majority;
      this.max = max;
    }

    MinMajorityMax combine(MinMajorityMax that) {
      return new MinMajorityMax(
          Math.min(this.min, that.min),
          Math.min(this.majority, that.majority),
          Math.min(this.max, that.max));
    }

    static MinMajorityMax valueOf(long[] sorted) {
      return new MinMajorityMax(sorted[0], getMajority(sorted), getMax(sorted));
    }

    static MinMajorityMax valueOf(long[] sorted, long gapThreshold) {
      long majority = getMajority(sorted);
      long min = sorted[0];
      if (gapThreshold != -1 && (majority - min) > gapThreshold) {
        // The the gap between majority and min(the slow follower) is greater than gapThreshold,
        // set the majority to min, which will skip one round of lastCommittedIndex update in updateCommit().
        majority = min;
      }
      return new MinMajorityMax(min, majority, getMax(sorted));
    }

    static long getMajority(long[] sorted) {
      return sorted[(sorted.length - 1) / 2];
    }

    static long getMax(long[] sorted) {
      return sorted[sorted.length - 1];
    }
  }

  private void updateCommit() {
    getMajorityMin(FollowerInfo::getMatchIndex, raftLog::getFlushIndex,
        followerMaxGapThreshold)
    .ifPresent(m -> updateCommit(m.majority, m.min));
  }

  private Optional<MinMajorityMax> getMajorityMin(ToLongFunction<FollowerInfo> followerIndex, LongSupplier logIndex) {
    return getMajorityMin(followerIndex, logIndex, -1);
  }

  private Optional<MinMajorityMax> getMajorityMin(ToLongFunction<FollowerInfo> followerIndex,
      LongSupplier logIndex, long gapThreshold) {
    final RaftPeerId selfId = server.getId();
    final RaftConfigurationImpl conf = server.getRaftConf();

    final CurrentOldFollowerInfos infos = followerInfoMap.getFollowerInfos(conf);
    final List<FollowerInfo> followers = infos.getCurrent();
    final boolean includeSelf = conf.containsInConf(selfId);
    if (followers.isEmpty() && !includeSelf) {
      return Optional.empty();
    }

    final long[] indicesInNewConf = getSorted(followers, includeSelf, followerIndex, logIndex);
    final MinMajorityMax newConf = MinMajorityMax.valueOf(indicesInNewConf, gapThreshold);

    if (!conf.isTransitional()) {
      return Optional.of(newConf);
    } else { // configuration is in transitional state
      final List<FollowerInfo> oldFollowers = infos.getOld();
      final boolean includeSelfInOldConf = conf.containsInOldConf(selfId);
      if (oldFollowers.isEmpty() && !includeSelfInOldConf) {
        return Optional.empty();
      }

      final long[] indicesInOldConf = getSorted(oldFollowers, includeSelfInOldConf, followerIndex, logIndex);
      final MinMajorityMax oldConf = MinMajorityMax.valueOf(indicesInOldConf, gapThreshold);
      return Optional.of(newConf.combine(oldConf));
    }
  }

  private boolean hasMajority(Predicate<RaftPeerId> isAcked) {
    final RaftPeerId selfId = server.getId();
    final RaftConfigurationImpl conf = server.getRaftConf();

    final CurrentOldFollowerInfos infos = followerInfoMap.getFollowerInfos(conf);
    final List<FollowerInfo> followers = infos.getCurrent();
    final boolean includeSelf = conf.containsInConf(selfId);
    final boolean newConf = hasMajority(isAcked, followers, includeSelf);

    if (!conf.isTransitional()) {
      return newConf;
    } else {
      final List<FollowerInfo> oldFollowers = infos.getOld();
      final boolean includeSelfInOldConf = conf.containsInOldConf(selfId);
      final boolean oldConf = hasMajority(isAcked, oldFollowers, includeSelfInOldConf);
      return newConf && oldConf;
    }
  }

  private boolean hasMajority(Predicate<RaftPeerId> isAcked, List<FollowerInfo> followers, boolean includeSelf) {
    if (followers.isEmpty() && !includeSelf) {
      return true;
    }

    int count = includeSelf ? 1 : 0;
    for (FollowerInfo follower: followers) {
      if (isAcked.test(follower.getId())) {
        count++;
      }
    }
    final int size = includeSelf ? followers.size() + 1 : followers.size();
    return count > size / 2;
  }

  private void updateCommit(LogEntryHeader[] entriesToCommit) {
    final long newCommitIndex = raftLog.getLastCommittedIndex();
    logMetadata(newCommitIndex);
    commitIndexChanged();

    boolean hasConfiguration = false;
    for (LogEntryHeader entry : entriesToCommit) {
      if (entry.getIndex() > newCommitIndex) {
        break;
      }
      hasConfiguration |= entry.getLogEntryBodyCase() == LogEntryBodyCase.CONFIGURATIONENTRY;
      raftLog.getRaftLogMetrics().onLogEntryCommitted(entry);
    }
    if (hasConfiguration) {
      checkAndUpdateConfiguration();
    }
  }

  private void updateCommit(long majority, long min) {
    final long oldLastCommitted = raftLog.getLastCommittedIndex();
    if (majority > oldLastCommitted) {
      // Get the headers before updating commit index since the log can be purged after a snapshot
      final LogEntryHeader[] entriesToCommit = raftLog.getEntries(oldLastCommitted + 1, majority + 1);

      if (server.getState().updateCommitIndex(majority, currentTerm, true)) {
        updateCommit(entriesToCommit);
      }
    }
    watchRequests.update(ReplicationLevel.ALL, min);
  }

  private void logMetadata(long commitIndex) {
    raftLog.appendMetadata(currentTerm, commitIndex);
    notifySenders();
  }

  private void checkAndUpdateConfiguration() {
    final RaftConfigurationImpl conf = server.getRaftConf();
    if (conf.isTransitional()) {
      replicateNewConf();
    } else { // the (new) log entry has been committed
      pendingRequests.replySetConfiguration(server::newSuccessReply);
      // if the leader is not included in the current configuration, step down
      if (!conf.containsInConf(server.getId(), RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER)) {
        LOG.info("{} is not included in the new configuration {}. Will shutdown server...", this, conf);
        try {
          // leave some time for all RPC senders to send out new conf entry
          server.properties().minRpcTimeout().sleep();
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        // the pending request handler will send NotLeaderException for
        // pending client requests when it stops
        server.close();
      }
    }
  }

  /**
   * when the (old, new) log entry has been committed, should replicate (new):
   * 1) append (new) to log
   * 2) update conf to (new)
   * 3) update RpcSenders list
   * 4) start replicating the log entry
   */
  private void replicateNewConf() {
    final RaftConfigurationImpl conf = server.getRaftConf();
    final RaftConfigurationImpl newConf = RaftConfigurationImpl.newBuilder()
        .setConf(conf)
        .setLogEntryIndex(raftLog.getNextIndex())
        .build();
    // stop the LogAppender if the corresponding follower and listener is no longer in the conf
    updateSenders(newConf);
    long index = raftLog.append(server.getState().getCurrentTerm(), newConf);
    updateConfiguration(index, newConf);
    notifySenders();
  }

  private long[] getSorted(List<FollowerInfo> followerInfos, boolean includeSelf,
      ToLongFunction<FollowerInfo> getFollowerIndex, LongSupplier getLogIndex) {
    final int length = includeSelf ? followerInfos.size() + 1 : followerInfos.size();
    if (length == 0) {
      throw new IllegalArgumentException("followerInfos is empty and includeSelf == " + includeSelf);
    }

    final long[] indices = new long[length];
    for (int i = 0; i < followerInfos.size(); i++) {
      indices[i] = getFollowerIndex.applyAsLong(followerInfos.get(i));
    }

    if (includeSelf) {
      // note that we also need to wait for the local disk I/O
      indices[length - 1] = getLogIndex.getAsLong();
    }

    Arrays.sort(indices);
    return indices;
  }

  private void checkPeersForYieldingLeader() {
    final RaftConfigurationImpl conf = server.getRaftConf();
    final RaftPeer leader = conf.getPeer(server.getId());
    if (leader == null) {
      LOG.error("{} the leader {} is not in the conf {}", this, server.getId(), conf);
      return;
    }
    final int leaderPriority = leader.getPriority();

    final List<LogAppender> highestPriorityInfos = new ArrayList<>();
    int highestPriority = Integer.MIN_VALUE;
    for (LogAppender logAppender : senders) {
      final RaftPeer follower = conf.getPeer(logAppender.getFollowerId());
      if (follower == null) {
        continue;
      }
      final int followerPriority = follower.getPriority();
      if (followerPriority > leaderPriority && followerPriority >= highestPriority) {
        if (followerPriority > highestPriority) {
          highestPriority = followerPriority;
          highestPriorityInfos.clear();
        }
        highestPriorityInfos.add(logAppender);
      }
    }
    final TermIndex leaderLastEntry = getLastEntry();
    final LogAppender appender = chooseUpToDateFollower(highestPriorityInfos, leaderLastEntry);
    if (appender != null) {
      server.getTransferLeadership().start(appender);
    }
  }

  /**
   * See the thesis section 6.2: A leader in Raft steps down
   * if an election timeout elapses without a successful
   * round of heartbeats to a majority of its cluster.
   */

  public boolean checkLeadership() {
    if (!server.getInfo().isLeader()) {
      return false;
    }
    // The initial value of lastRpcResponseTime in FollowerInfo is set by
    // LeaderState::addSenders(), which is fake and used to trigger an
    // immediate round of AppendEntries request. Since candidates collect
    // votes from majority before becoming leader, without seeing higher term,
    // ideally, A leader is legal for election timeout if become leader soon.
    if (server.getRole().getRoleElapsedTimeMs() < server.getMaxTimeoutMs()) {
      return true;
    }

    final List<RaftPeerId> activePeers = getLogAppenders()
        .filter(sender -> sender.getFollower()
                                .getLastRpcResponseTime()
                                .elapsedTimeMs() <= server.getMaxTimeoutMs())
        .map(LogAppender::getFollowerId)
        .collect(Collectors.toList());

    final RaftConfigurationImpl conf = server.getRaftConf();

    if (conf.hasMajority(activePeers, server.getId())) {
      // leadership check passed
      return true;
    }

    LOG.warn(this + ": Lost leadership on term: " + currentTerm
        + ". Election timeout: " + server.getMaxTimeoutMs() + "ms"
        + ". In charge for: " + server.getRole().getRoleElapsedTimeMs() + "ms"
        + ". Conf: " + conf);
    getLogAppenders().map(LogAppender::getFollower).forEach(f -> LOG.warn("Follower {}", f));

    // step down as follower
    stepDown(currentTerm, StepDownReason.LOST_MAJORITY_HEARTBEATS);
    return false;
  }

  /**
   * Obtain the current readIndex for read only requests. See Raft paper section 6.4.
   * 1. Leader makes sure at least one log from current term is committed.
   * 2. Leader record last committed index as readIndex.
   * 3. Leader broadcast heartbeats to followers and waits for acknowledgements.
   * 4. If majority respond success, returns readIndex.
   * @return current readIndex.
   */
  CompletableFuture<Long> getReadIndex() {
    final long readIndex = server.getRaftLog().getLastCommittedIndex();

    // if group contains only one member, fast path
    if (server.getRaftConf().getCurrentPeers().size() == 1) {
      return CompletableFuture.completedFuture(readIndex);
    }

    // leader has not committed any entries in this term, reject
    if (server.getRaftLog().getTermIndex(readIndex).getTerm() != server.getState().getCurrentTerm()) {
      return JavaUtils.completeExceptionally(new ReadIndexException(
          "Failed to getReadIndex " + readIndex + " since the term is not yet committed.",
          new LeaderNotReadyException(server.getMemberId())));
    }

    final MemoizedSupplier<AppendEntriesListener> supplier = MemoizedSupplier.valueOf(
        () -> new AppendEntriesListener(readIndex));
    final AppendEntriesListener listener = readIndexHeartbeats.addAppendEntriesListener(
        readIndex, key -> supplier.get());

    // the readIndex is already acknowledged before
    if (listener == null) {
      return CompletableFuture.completedFuture(readIndex);
    }

    if (supplier.isInitialized()) {
      senders.forEach(LogAppender::triggerHeartbeat);
    }

    return listener.getFuture();
  }

  @Override
  public void onAppendEntriesReply(LogAppender appender, RaftProtos.AppendEntriesReplyProto reply) {
    readIndexHeartbeats.onAppendEntriesReply(appender, reply, this::hasMajority);
  }

  void replyPendingRequest(long logIndex, RaftClientReply reply) {
    pendingRequests.replyPendingRequest(logIndex, reply);
  }

  TransactionContext getTransactionContext(long index) {
    return pendingRequests.getTransactionContext(index);
  }

  long[] getFollowerNextIndices() {
    return getLogAppenders().mapToLong(s -> s.getFollower().getNextIndex()).toArray();
  }

  static Map<RaftPeerId, RaftPeer> newMap(Collection<RaftPeer> peers, String str) {
    Objects.requireNonNull(peers, () -> str + " == null");
    final Map<RaftPeerId, RaftPeer> map = new HashMap<>();
    for(RaftPeer p : peers) {
      map.put(p.getId(), p);
    }
    return Collections.unmodifiableMap(map);
  }

  private class ConfigurationStagingState {
    private final String name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    private final Map<RaftPeerId, RaftPeer> newPeers;
    private final Map<RaftPeerId, RaftPeer> newListeners;
    private final PeerConfiguration newConf;

    ConfigurationStagingState(Collection<RaftPeer> newPeers, Collection<RaftPeer> newListeners,
        PeerConfiguration newConf) {
      this.newPeers = newMap(newPeers, "peer");
      this.newListeners = newMap(newListeners, "listeners");
      this.newConf = newConf;
    }

    RaftConfigurationImpl generateOldNewConf(RaftConfigurationImpl current, long logIndex) {
      return RaftConfigurationImpl.newBuilder()
          .setConf(newConf)
          .setOldConf(current)
          .setLogEntryIndex(logIndex)
          .build();
    }

    Collection<RaftPeer> getNewPeers() {
      return newPeers.values();
    }

    Collection<RaftPeer> getNewListeners() {
      return newListeners.values();
    }

    boolean contains(RaftPeerId peerId) {
      return newPeers.containsKey(peerId) || newListeners.containsKey(peerId);
    }

    void fail(BootStrapProgress progress) {
      final String message = this + ": Fail to set configuration " + newConf + " due to " + progress;
      LOG.debug(message);
      stopAndRemoveSenders(s -> !isCaughtUp(s.getFollower()));

      stagingState = null;
      // send back failure response to client's request
      pendingRequests.failSetConfiguration(new ReconfigurationTimeoutException(message));
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * @return the RaftPeer (address and id) information of the followers.
   */
  Stream<RaftPeer> getFollowers() {
    return getLogAppenders()
        .map(sender -> sender.getFollower().getPeer())
        .filter(peer -> server.getRaftConf().containsInConf(peer.getId()));
  }

  Stream<LogAppender> getLogAppenders() {
    return StreamSupport.stream(senders.spliterator(), false);
  }

  Optional<LogAppender> getLogAppender(RaftPeerId id) {
    return getLogAppenders().filter(a -> a.getFollowerId().equals(id)).findAny();
  }

  private static boolean isCaughtUp(FollowerInfo follower) {
    return ((FollowerInfoImpl)follower).isCaughtUp();
  }

  @Override
  public void checkHealth(FollowerInfo follower) {
    final TimeDuration elapsedTime = follower.getLastRpcResponseTime().elapsedTime();
    if (elapsedTime.compareTo(server.properties().rpcSlownessTimeout()) > 0) {
      final RoleInfoProto leaderInfo = server.getInfo().getRoleInfoProto();
      server.getStateMachine().leaderEvent().notifyFollowerSlowness(leaderInfo);
      server.getStateMachine().leaderEvent().notifyFollowerSlowness(leaderInfo, follower.getPeer());
    }
    final RaftPeerId followerId = follower.getId();
    raftServerMetrics.recordFollowerHeartbeatElapsedTime(followerId, elapsedTime.toLong(TimeUnit.NANOSECONDS));
  }

  @Override
  public String toString() {
    return name;
  }
}
