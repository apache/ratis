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
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * States for leader only. It contains three different types of processors:
 * 1. RPC senders: each thread is appending log to a follower
 * 2. EventProcessor: a single thread updating the raft server's state based on
 *                    status of log appending response
 * 3. PendingRequestHandler: a handler sending back responses to clients when
 *                           corresponding log entries are committed
 */
public class LeaderState {
  private static final Logger LOG = RaftServerImpl.LOG;
  public static final String APPEND_PLACEHOLDER = LeaderState.class.getSimpleName() + ".placeholder";

  private enum BootStrapProgress {
    NOPROGRESS, PROGRESSING, CAUGHTUP
  }

  static class StateUpdateEvent {
    private enum Type {
      STEP_DOWN, UPDATE_COMMIT, CHECK_STAGING
    }

    final Type type;
    final long newTerm;
    final Runnable handler;

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
    public String toString() {
      return type + (newTerm >= 0? ":" + newTerm: "");
    }
  }

  private class EventQueue {
    private final BlockingQueue<StateUpdateEvent> queue = new ArrayBlockingQueue<>(4096);

    void submit(StateUpdateEvent event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        LOG.info("{}: Interrupted when submitting {} ", server.getId(), event);
      }
    }

    StateUpdateEvent poll() {
      final StateUpdateEvent e;
      try {
        e = queue.poll(server.getMaxTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch(InterruptedException ie) {
        String s = server.getId() + ": " + getClass().getSimpleName() + " thread is interrupted";
        if (!running) {
          LOG.info(s + " gracefully");
          return null;
        } else {
          throw new IllegalStateException(s + " UNEXPECTEDLY", ie);
        }
      }

      if (e != null) {
        // remove duplicated events from the head.
        for(; e.equals(queue.peek()); queue.poll());
      }
      return e;
    }
  }

  /**
   * Use {@link CopyOnWriteArrayList} to implement a thread-safe list.
   * Since each mutation induces a copy of the list, only bulk operations
   * (addAll and removeAll) are supported.
   */
  static class SenderList {
    private final List<LogAppender> senders;

    SenderList(LogAppender[] senders) {
      this.senders = new CopyOnWriteArrayList<>(senders);
    }

    int size() {
      return senders.size();
    }

    Stream<LogAppender> stream() {
      return senders.stream();
    }

    void forEach(Consumer<LogAppender> action) {
      senders.forEach(action);
    }

    boolean addAll(Collection<LogAppender> c) {
      return senders.addAll(c);
    }

    boolean removeAll(Collection<LogAppender> c) {
      return senders.removeAll(c);
    }
  }

  private final StateUpdateEvent UPDATE_COMMIT_EVENT =
      new StateUpdateEvent(StateUpdateEvent.Type.UPDATE_COMMIT, -1, this::updateCommit);
  private final StateUpdateEvent CHECK_STAGING_EVENT =
      new StateUpdateEvent(StateUpdateEvent.Type.CHECK_STAGING, -1, this::checkStaging);

  private final RaftServerImpl server;
  private final RaftLog raftLog;
  private final long currentTerm;
  private volatile ConfigurationStagingState stagingState;
  private List<List<FollowerInfo>> voterLists;

  /**
   * The list of threads appending entries to followers.
   * The list is protected by the RaftServer's lock.
   */
  private final SenderList senders;
  private final EventQueue eventQueue = new EventQueue();
  private final EventProcessor processor;
  private final PendingRequests pendingRequests;
  private final WatchRequests watchRequests;
  private volatile boolean running = true;

  private final int stagingCatchupGap;
  private final TimeDuration syncInterval;
  private final long placeHolderIndex;

  LeaderState(RaftServerImpl server, RaftProperties properties) {
    this.server = server;

    stagingCatchupGap = RaftServerConfigKeys.stagingCatchupGap(properties);
    syncInterval = RaftServerConfigKeys.Rpc.sleepTime(properties);

    final ServerState state = server.getState();
    this.raftLog = state.getLog();
    this.currentTerm = state.getCurrentTerm();
    processor = new EventProcessor();
    this.pendingRequests = new PendingRequests(server.getId());
    this.watchRequests = new WatchRequests(server);

    final RaftConfiguration conf = server.getRaftConf();
    Collection<RaftPeer> others = conf.getOtherPeers(state.getSelfId());
    final Timestamp t = new Timestamp().addTimeMs(-server.getMaxTimeoutMs());
    placeHolderIndex = raftLog.getNextIndex();

    senders = new SenderList(others.stream().map(
        p -> server.newLogAppender(this, p, t, placeHolderIndex, true))
        .toArray(LogAppender[]::new));

    voterLists = divideFollowers(conf);
  }

  LogEntryProto start() {
    // In the beginning of the new term, replicate a conf entry in order
    // to finally commit entries in the previous term.
    // Also this message can help identify the last committed index and the conf.
    final LogEntryProto placeHolder = ServerProtoUtils.toLogEntryProto(
        server.getRaftConf(), server.getState().getCurrentTerm(), raftLog.getNextIndex());
    CodeInjectionForTesting.execute(APPEND_PLACEHOLDER,
        server.getId().toString(), null);
    raftLog.append(placeHolder);

    processor.start();
    senders.forEach(LogAppender::startAppender);
    return placeHolder;
  }

  boolean isReady() {
    return server.getState().getLastAppliedIndex() >= placeHolderIndex;
  }

  void stop() {
    this.running = false;
    // do not interrupt event processor since it may be in the middle of logSync
    senders.forEach(LogAppender::stopAppender);
    final NotLeaderException nle = server.generateNotLeaderException();
    final Collection<CommitInfoProto> commitInfos = server.getCommitInfos();
    try {
      final Collection<TransactionContext> transactions = pendingRequests.sendNotLeaderResponses(nle, commitInfos);
      server.getStateMachine().notifyNotLeader(transactions);
      watchRequests.failWatches(nle);
    } catch (IOException e) {
      LOG.warn(server.getId() + ": Caught exception in sendNotLeaderResponses", e);
    }
  }

  void notifySenders() {
    senders.forEach(LogAppender::notifyAppend);
  }

  boolean inStagingState() {
    return stagingState != null;
  }

  long getCurrentTerm() {
    return currentTerm;
  }

  TimeDuration getSyncInterval() {
    return syncInterval;
  }

  /**
   * Start bootstrapping new peers
   */
  PendingRequest startSetConfiguration(SetConfigurationRequest request) {
    Preconditions.assertTrue(running && !inStagingState());

    RaftPeer[] peersInNewConf = request.getPeersInNewConf();
    Collection<RaftPeer> peersToBootStrap = RaftConfiguration
        .computeNewPeers(peersInNewConf, server.getRaftConf());

    // add the request to the pending queue
    final PendingRequest pending = pendingRequests.addConfRequest(request);

    ConfigurationStagingState stagingState = new ConfigurationStagingState(
        peersToBootStrap, new PeerConfiguration(Arrays.asList(peersInNewConf)));
    Collection<RaftPeer> newPeers = stagingState.getNewPeers();
    // set the staging state
    this.stagingState = stagingState;

    if (newPeers.isEmpty()) {
      applyOldNewConf();
    } else {
      // update the LeaderState's sender list
      addSenders(newPeers);
    }
    return pending;
  }

  PendingRequest addPendingRequest(RaftClientRequest request, TransactionContext entry) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: addPendingRequest at {}, entry=", server.getId(), request,
          ServerProtoUtils.toLogEntryString(entry.getLogEntry()));
    }
    return pendingRequests.add(request, entry);
  }

  CompletableFuture<Void> addWatchReqeust(RaftClientRequest request) {
    LOG.debug("{}: addWatchRequest {}", server.getId(), request);
    return watchRequests.add(request.getType().getWatch());
  }

  void commitIndexChanged() {
    getMajorityMin(FollowerInfo::getCommitIndex, raftLog::getLastCommittedIndex).ifPresent(m -> {
      // Normally, leader commit index is always ahead followers.
      // However, after a leader change, the new leader commit index may
      // be behind some followers in the beginning.
      watchRequests.update(ReplicationLevel.ALL_COMMITTED, m.min);
      watchRequests.update(ReplicationLevel.MAJORITY_COMMITTED, m.majority);
      watchRequests.update(ReplicationLevel.MAJORITY, m.max);
    });
  }

  private void applyOldNewConf() {
    final ServerState state = server.getState();
    final RaftConfiguration current = server.getRaftConf();
    final RaftConfiguration oldNewConf= stagingState.generateOldNewConf(current,
        state.getLog().getNextIndex());
    // apply the (old, new) configuration to log, and use it as the current conf
    long index = state.getLog().append(state.getCurrentTerm(), oldNewConf);
    updateConfiguration(index, oldNewConf);

    this.stagingState = null;
    notifySenders();
  }

  private void updateConfiguration(long logIndex, RaftConfiguration newConf) {
    voterLists = divideFollowers(newConf);
    server.getState().setRaftConf(logIndex, newConf);
  }

  void updateFollowerCommitInfos(CommitInfoCache cache, List<CommitInfoProto> protos) {
    senders.stream().map(LogAppender::getFollower)
        .map(f -> cache.update(f.getPeer(), f.getCommitIndex()))
        .forEach(protos::add);
  }

  AppendEntriesRequestProto newAppendEntriesRequestProto(RaftPeerId targetId,
      TermIndex previous, List<LogEntryProto> entries, boolean initializing,
      long callId) {
    return ServerProtoUtils.toAppendEntriesRequestProto(server.getId(), targetId,
        server.getGroupId(), currentTerm, entries, raftLog.getLastCommittedIndex(),
        initializing, previous, server.getCommitInfos(), callId);
  }

  /**
   * After receiving a setConfiguration request, the leader should update its
   * RpcSender list.
   */
  void addSenders(Collection<RaftPeer> newMembers) {
    final Timestamp t = new Timestamp().addTimeMs(-server.getMaxTimeoutMs());
    final long nextIndex = raftLog.getNextIndex();

    senders.addAll(newMembers.stream().map(peer -> {
      LogAppender sender = server.newLogAppender(this, peer, t, nextIndex, false);
      sender.startAppender();
      return sender;
    }).collect(Collectors.toList()));
  }

  void stopAndRemoveSenders(Predicate<LogAppender> predicate) {
    final List<LogAppender> toStop = senders.stream().filter(predicate).collect(Collectors.toList());
    toStop.forEach(LogAppender::stopAppender);
    senders.removeAll(toStop);
  }

  /**
   * Update the RpcSender list based on the current configuration
   */
  private void updateSenders(RaftConfiguration conf) {
    Preconditions.assertTrue(conf.isStable() && !inStagingState());
    stopAndRemoveSenders(s -> !conf.containsInConf(s.getFollower().getPeer().getId()));
  }

  void submitStepDownEvent() {
    submitStepDownEvent(getCurrentTerm());
  }

  void submitStepDownEvent(long term) {
    eventQueue.submit(new StateUpdateEvent(StateUpdateEvent.Type.STEP_DOWN, term, () -> stepDown(term)));
  }

  private void stepDown(long term) {
    try {
      server.changeToFollowerAndPersistMetadata(term);
    } catch(IOException e) {
      final String s = server.getId() + ": Failed to persist metadata for term " + term;
      LOG.warn(s, e);
      // the failure should happen while changing the state to follower
      // thus the in-memory state should have been updated
      if (running) {
        throw new IllegalStateException(s + " and running == true", e);
      }
    }
  }

  private void prepare() {
    synchronized (server) {
      if (running) {
        final RaftConfiguration conf = server.getRaftConf();
        if (conf.isTransitional() && server.getState().isConfCommitted()) {
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
    @Override
    public void run() {
      // apply an empty message; check if necessary to replicate (new) conf
      prepare();

      while (running) {
        final StateUpdateEvent event = eventQueue.poll();
        synchronized(server) {
          if (running) {
            if (event != null) {
              event.execute();
            } else if (inStagingState()) {
              checkStaging();
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
  private BootStrapProgress checkProgress(FollowerInfo follower,
      long committed) {
    Preconditions.assertTrue(!follower.isAttendingVote());
    final Timestamp progressTime = new Timestamp().addTimeMs(-server.getMaxTimeoutMs());
    final Timestamp timeoutTime = new Timestamp().addTimeMs(-3*server.getMaxTimeoutMs());
    if (follower.getLastRpcResponseTime().compareTo(timeoutTime) < 0) {
      LOG.debug("{} detects a follower {} timeout for bootstrapping," +
              " timeoutTime: {}", server.getId(), follower, timeoutTime);
      return BootStrapProgress.NOPROGRESS;
    } else if (follower.getMatchIndex() + stagingCatchupGap > committed
        && follower.getLastRpcResponseTime().compareTo(progressTime) > 0) {
      return BootStrapProgress.CAUGHTUP;
    } else {
      return BootStrapProgress.PROGRESSING;
    }
  }

  private Collection<BootStrapProgress> checkAllProgress(long committed) {
    Preconditions.assertTrue(inStagingState());
    return senders.stream()
        .filter(sender -> !sender.getFollower().isAttendingVote())
        .map(sender -> checkProgress(sender.getFollower(), committed))
        .collect(Collectors.toCollection(ArrayList::new));
  }

  void submitCheckStagingEvent() {
    eventQueue.submit(CHECK_STAGING_EVENT);
  }

  private void checkStaging() {
    if (!inStagingState()) {
      // it is possible that the bootstrapping is done. Then, fallback to UPDATE_COMMIT
      UPDATE_COMMIT_EVENT.execute();
    } else {
      final long committedIndex = server.getState().getLog()
          .getLastCommittedIndex();
      Collection<BootStrapProgress> reports = checkAllProgress(committedIndex);
      if (reports.contains(BootStrapProgress.NOPROGRESS)) {
        LOG.debug("{} fails the setConfiguration request", server.getId());
        stagingState.fail();
      } else if (!reports.contains(BootStrapProgress.PROGRESSING)) {
        // all caught up!
        applyOldNewConf();
        senders.forEach(s -> s.getFollower().startAttendVote());
      }
    }
  }

  boolean isBootStrappingPeer(RaftPeerId peerId) {
    return Optional.ofNullable(stagingState).map(s -> s.contains(peerId)).orElse(false);
  }

  void submitUpdateCommitEvent() {
    eventQueue.submit(UPDATE_COMMIT_EVENT);
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

    static long getMajority(long[] sorted) {
      return sorted[(sorted.length - 1) / 2];
    }

    static long getMax(long[] sorted) {
      return sorted[sorted.length - 1];
    }
  }

  private void updateCommit() {
    getMajorityMin(FollowerInfo::getMatchIndex, raftLog::getLatestFlushedIndex)
        .ifPresent(m -> updateCommit(m.majority, m.min));
  }

  private Optional<MinMajorityMax> getMajorityMin(ToLongFunction<FollowerInfo> followerIndex, LongSupplier logIndex) {
    final RaftPeerId selfId = server.getId();
    final RaftConfiguration conf = server.getRaftConf();

    final List<FollowerInfo> followers = voterLists.get(0);
    final boolean includeSelf = conf.containsInConf(selfId);
    if (followers.isEmpty() && !includeSelf) {
      return Optional.empty();
    }

    final long[] indicesInNewConf = getSorted(followers, includeSelf, followerIndex, logIndex);
    final MinMajorityMax newConf = MinMajorityMax.valueOf(indicesInNewConf);

    if (!conf.isTransitional()) {
      return Optional.of(newConf);
    } else { // configuration is in transitional state
      final List<FollowerInfo> oldFollowers = voterLists.get(1);
      final boolean includeSelfInOldConf = conf.containsInOldConf(selfId);
      if (oldFollowers.isEmpty() && !includeSelfInOldConf) {
        return Optional.empty();
      }

      final long[] indicesInOldConf = getSorted(oldFollowers, includeSelfInOldConf, followerIndex, logIndex);
      final MinMajorityMax oldConf = MinMajorityMax.valueOf(indicesInOldConf);
      return Optional.of(newConf.combine(oldConf));
    }
  }

  private void updateCommit(long majority, long min) {
    final long oldLastCommitted = raftLog.getLastCommittedIndex();
    if (majority > oldLastCommitted) {
      // copy the entries out from the raftlog, in order to prevent that
      // the log gets purged after the statemachine does a snapshot
      final TermIndex[] entriesToCommit = raftLog.getEntries(
          oldLastCommitted + 1, majority + 1);
      if (server.getState().updateStatemachine(majority, currentTerm)) {
        watchRequests.update(ReplicationLevel.MAJORITY, majority);
        logMetadata(majority);
        commitIndexChanged();
      }
      checkAndUpdateConfiguration(entriesToCommit);
    }

    watchRequests.update(ReplicationLevel.ALL, min);
  }

  private void logMetadata(long commitIndex) {
    raftLog.appendMetadata(currentTerm, commitIndex);
    notifySenders();
  }

  private boolean committedConf(TermIndex[] entries) {
    final long currentCommitted = raftLog.getLastCommittedIndex();
    for (TermIndex entry : entries) {
      if (entry.getIndex() <= currentCommitted && raftLog.isConfigEntry(entry)) {
        return true;
      }
    }
    return false;
  }

  private void checkAndUpdateConfiguration(TermIndex[] entriesToCheck) {
    final RaftConfiguration conf = server.getRaftConf();
    if (committedConf(entriesToCheck)) {
      if (conf.isTransitional()) {
        replicateNewConf();
      } else { // the (new) log entry has been committed
        pendingRequests.replySetConfiguration(server::getCommitInfos);
        // if the leader is not included in the current configuration, step down
        if (!conf.containsInConf(server.getId())) {
          LOG.info("{} is not included in the new configuration {}. Step down.",
              server.getId(), conf);
          try {
            // leave some time for all RPC senders to send out new conf entry
            Thread.sleep(server.getMinTimeoutMs());
          } catch (InterruptedException ignored) {
          }
          // the pending request handler will send NotLeaderException for
          // pending client requests when it stops
          server.shutdown(false);
        }
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
    final RaftConfiguration conf = server.getRaftConf();
    final RaftConfiguration newConf = RaftConfiguration.newBuilder()
        .setConf(conf)
        .setLogEntryIndex(raftLog.getNextIndex())
        .build();
    // stop the LogAppender if the corresponding follower is no longer in the conf
    updateSenders(newConf);
    long index = raftLog.append(server.getState().getCurrentTerm(), newConf);
    updateConfiguration(index, newConf);
    notifySenders();
  }

  private static long[] getSorted(List<FollowerInfo> followers, boolean includeSelf,
      ToLongFunction<FollowerInfo> getFollowerIndex, LongSupplier getLogIndex) {
    final int length = includeSelf ? followers.size() + 1 : followers.size();
    if (length == 0) {
      throw new IllegalArgumentException("followers.size() == "
          + followers.size() + " and includeSelf == " + includeSelf);
    }
    final long[] indices = new long[length];
    for (int i = 0; i < followers.size(); i++) {
      indices[i] = getFollowerIndex.applyAsLong(followers.get(i));
    }
    if (includeSelf) {
      // note that we also need to wait for the local disk I/O
      indices[length - 1] = getLogIndex.getAsLong();
    }

    Arrays.sort(indices);
    return indices;
  }

  private List<List<FollowerInfo>> divideFollowers(RaftConfiguration conf) {
    List<List<FollowerInfo>> lists = new ArrayList<>(2);
    List<FollowerInfo> listForNew = senders.stream()
        .filter(sender -> conf.containsInConf(sender.getFollower().getPeer().getId()))
        .map(LogAppender::getFollower)
        .collect(Collectors.toList());
    lists.add(listForNew);
    if (conf.isTransitional()) {
      List<FollowerInfo> listForOld = senders.stream()
          .filter(sender -> conf.containsInOldConf(sender.getFollower().getPeer().getId()))
          .map(LogAppender::getFollower)
          .collect(Collectors.toList());
      lists.add(listForOld);
    }
    return lists;
  }

  void replyPendingRequest(long logIndex, RaftClientReply reply) {
    pendingRequests.replyPendingRequest(logIndex, reply);
  }

  TransactionContext getTransactionContext(long index) {
    return pendingRequests.getTransactionContext(index);
  }

  long[] getFollowerNextIndices() {
    return senders.stream().mapToLong(s -> s.getFollower().getNextIndex()).toArray();
  }

  private class ConfigurationStagingState {
    private final Map<RaftPeerId, RaftPeer> newPeers;
    private final PeerConfiguration newConf;

    ConfigurationStagingState(Collection<RaftPeer> newPeers,
        PeerConfiguration newConf) {
      Map<RaftPeerId, RaftPeer> map = new HashMap<>();
      for (RaftPeer peer : newPeers) {
        map.put(peer.getId(), peer);
      }
      this.newPeers = Collections.unmodifiableMap(map);
      this.newConf = newConf;
    }

    RaftConfiguration generateOldNewConf(RaftConfiguration current,
        long logIndex) {
      return RaftConfiguration.newBuilder()
          .setConf(newConf)
          .setOldConf(current)
          .setLogEntryIndex(logIndex)
          .build();
    }

    Collection<RaftPeer> getNewPeers() {
      return newPeers.values();
    }

    boolean contains(RaftPeerId peerId) {
      return newPeers.containsKey(peerId);
    }

    void fail() {
      stopAndRemoveSenders(s -> !s.getFollower().isAttendingVote());

      LeaderState.this.stagingState = null;
      // send back failure response to client's request
      pendingRequests.failSetConfiguration(
          new ReconfigurationTimeoutException("Fail to set configuration "
              + newConf + ". Timeout when bootstrapping new peers."));
    }
  }

  /**
   * @return the RaftPeer (address and id) information of the followers.
   */
  List<RaftPeer> getFollowers() {
    return Collections.unmodifiableList(senders.stream()
        .map(sender -> sender.getFollower().getPeer())
        .collect(Collectors.toList()));
  }

  Stream<LogAppender> getLogAppenders() {
    return senders.stream();
  }
}
