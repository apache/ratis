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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.shaded.proto.RaftProtos.LeaderNoOp;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.ratis.server.impl.LeaderState.StateUpdateEventType.*;

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

  enum StateUpdateEventType {
    STEPDOWN, UPDATECOMMIT, STAGINGPROGRESS
  }

  enum BootStrapProgress {
    NOPROGRESS, PROGRESSING, CAUGHTUP
  }

  static class StateUpdateEvent {
    final StateUpdateEventType type;
    final long newTerm;

    StateUpdateEvent(StateUpdateEventType type, long newTerm) {
      this.type = type;
      this.newTerm = newTerm;
    }
  }

  static final StateUpdateEvent UPDATE_COMMIT_EVENT =
      new StateUpdateEvent(StateUpdateEventType.UPDATECOMMIT, -1);
  static final StateUpdateEvent STAGING_PROGRESS_EVENT =
      new StateUpdateEvent(StateUpdateEventType.STAGINGPROGRESS, -1);

  private final RaftServerImpl server;
  private final RaftLog raftLog;
  private final long currentTerm;
  private volatile ConfigurationStagingState stagingState;
  private List<List<FollowerInfo>> voterLists;

  /**
   * The list of threads appending entries to followers.
   * The list is protected by the RaftServer's lock.
   */
  private final List<LogAppender> senders;
  private final BlockingQueue<StateUpdateEvent> eventQ;
  private final EventProcessor processor;
  private final PendingRequests pendingRequests;
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
    eventQ = new ArrayBlockingQueue<>(4096);
    processor = new EventProcessor();
    pendingRequests = new PendingRequests(server);

    final RaftConfiguration conf = server.getRaftConf();
    Collection<RaftPeer> others = conf.getOtherPeers(state.getSelfId());
    final Timestamp t = new Timestamp().addTimeMs(-server.getMaxTimeoutMs());
    placeHolderIndex = raftLog.getNextIndex();
    senders = new ArrayList<>(others.size());

    for (RaftPeer p : others) {
      FollowerInfo f = new FollowerInfo(p, t, placeHolderIndex, true);
      senders.add(server.getFactory().newLogAppender(server, this, f));
    }
    voterLists = divideFollowers(conf);
  }

  void start() {
    // In the beginning of the new term, replicate an empty entry in order
    // to finally commit entries in the previous term.
    // Also this message can help identify the last committed index when
    // the leader peer is just started.
    final LogEntryProto placeHolder = LogEntryProto.newBuilder()
        .setTerm(server.getState().getCurrentTerm())
        .setIndex(raftLog.getNextIndex())
        .setNoOp(LeaderNoOp.newBuilder()).build();
    CodeInjectionForTesting.execute(APPEND_PLACEHOLDER,
        server.getId().toString(), null);
    raftLog.append(placeHolder);

    processor.start();
    startSenders();
  }

  boolean isReady() {
    return server.getState().getLastAppliedIndex() >= placeHolderIndex;
  }

  private void startSenders() {
    senders.forEach(Thread::start);
  }

  void stop() {
    this.running = false;
    // do not interrupt event processor since it may be in the middle of logSync
    for (LogAppender sender : senders) {
      sender.stopSender();
      sender.interrupt();
    }
    try {
      pendingRequests.sendNotLeaderResponses();
    } catch (IOException e) {
      LOG.warn("Caught exception in sendNotLeaderResponses", e);
    }
  }

  void notifySenders() {
    senders.forEach(LogAppender::notifyAppend);
  }

  boolean inStagingState() {
    return stagingState != null;
  }

  ConfigurationStagingState getStagingState() {
    return stagingState;
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

  PendingRequest addPendingRequest(long index, RaftClientRequest request,
      TransactionContext entry) {
    return pendingRequests.addPendingRequest(index, request, entry);
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

  /**
   * After receiving a setConfiguration request, the leader should update its
   * RpcSender list.
   */
  void addSenders(Collection<RaftPeer> newMembers) {
    final Timestamp t = new Timestamp().addTimeMs(-server.getMaxTimeoutMs());
    final long nextIndex = raftLog.getNextIndex();
    for (RaftPeer peer : newMembers) {
      FollowerInfo f = new FollowerInfo(peer, t, nextIndex, false);
      LogAppender sender = server.getFactory().newLogAppender(server, this, f);
      senders.add(sender);
      sender.start();
    }
  }

  /**
   * Update the RpcSender list based on the current configuration
   */
  private void updateSenders(RaftConfiguration conf) {
    Preconditions.assertTrue(conf.isStable() && !inStagingState());
    Iterator<LogAppender> iterator = senders.iterator();
    while (iterator.hasNext()) {
      LogAppender sender = iterator.next();
      if (!conf.containsInConf(sender.getFollower().getPeer().getId())) {
        iterator.remove();
        sender.stopSender();
        sender.interrupt();
      }
    }
  }

  void submitUpdateStateEvent(StateUpdateEvent event) {
    try {
      eventQ.put(event);
    } catch (InterruptedException e) {
      LOG.info("Interrupted when adding event {} into the queue", event);
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
        try {
          StateUpdateEvent event = eventQ.poll(server.getMaxTimeoutMs(),
              TimeUnit.MILLISECONDS);
          synchronized (server) {
            if (running) {
              handleEvent(event);
            }
          }
          // the updated configuration does not need to be sync'ed here
        } catch (InterruptedException e) {
          final String s = server.getId() + " " + getClass().getSimpleName()
              + " thread is interrupted ";
          if (!running) {
            LOG.info(s + " gracefully; server=" + server);
          } else {
            LOG.warn(s + " UNEXPECTEDLY; server=" + server, e);
            throw new RuntimeException(e);
          }
        } catch (IOException e) {
          LOG.warn("Failed to persist new votedFor/term.", e);
          // the failure should happen while changing the state to follower
          // thus the in-memory state should have been updated
          Preconditions.assertTrue(!running);
        }
      }
    }
  }

  private void handleEvent(StateUpdateEvent e) throws IOException {
    if (e == null) {
      if (inStagingState()) {
        checkNewPeers();
      }
    } else {
      if (e.type == STEPDOWN) {
        server.changeToFollower(e.newTerm, true);
      } else if (e.type == UPDATECOMMIT) {
        updateLastCommitted();
      } else if (e.type == STAGINGPROGRESS) {
        checkNewPeers();
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

  private void checkNewPeers() {
    if (!inStagingState()) {
      // it is possible that the bootstrapping is done and we still have
      // remaining STAGINGPROGRESS event to handle.
      updateLastCommitted();
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
        for (LogAppender sender : senders) {
          sender.getFollower().startAttendVote();
        }
      }
    }
  }

  boolean isBootStrappingPeer(RaftPeerId peerId) {
    return inStagingState() && getStagingState().contains(peerId);
  }

  private void updateLastCommitted() {
    final RaftPeerId selfId = server.getId();
    final RaftConfiguration conf = server.getRaftConf();
    long majorityInNewConf = computeLastCommitted(voterLists.get(0),
        conf.containsInConf(selfId));
    final long oldLastCommitted = raftLog.getLastCommittedIndex();
    final LogEntryProto[] entriesToCommit;
    if (!conf.isTransitional()) {
      // copy the entries that may get committed out of the raftlog, to prevent
      // the possible race that the log gets purged after the statemachine does
      // a snapshot
      entriesToCommit = raftLog.getEntries(oldLastCommitted + 1,
          Math.max(majorityInNewConf, oldLastCommitted) + 1);
      server.getState().updateStatemachine(majorityInNewConf, currentTerm);
    } else { // configuration is in transitional state
      long majorityInOldConf = computeLastCommitted(voterLists.get(1),
          conf.containsInOldConf(selfId));
      final long majority = Math.min(majorityInNewConf, majorityInOldConf);
      entriesToCommit = raftLog.getEntries(oldLastCommitted + 1,
          Math.max(majority, oldLastCommitted) + 1);
      server.getState().updateStatemachine(majority, currentTerm);
    }
    checkAndUpdateConfiguration(entriesToCommit);
  }

  private boolean committedConf(LogEntryProto[] entries) {
    final long currentCommitted = raftLog.getLastCommittedIndex();
    for (LogEntryProto entry : entries) {
      if (entry.getIndex() <= currentCommitted &&
          ProtoUtils.isConfigurationLogEntry(entry)) {
        return true;
      }
    }
    return false;
  }

  private void checkAndUpdateConfiguration(LogEntryProto[] entriesToCheck) {
    final RaftConfiguration conf = server.getRaftConf();
    if (committedConf(entriesToCheck)) {
      if (conf.isTransitional()) {
        replicateNewConf();
      } else { // the (new) log entry has been committed
        LOG.debug("{} sends success to setConfiguration request", server.getId());
        pendingRequests.replySetConfiguration();
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
          server.close();
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

  private long computeLastCommitted(List<FollowerInfo> followers,
      boolean includeSelf) {
    final int length = includeSelf ? followers.size() + 1 : followers.size();
    final long[] indices = new long[length];
    for (int i = 0; i < followers.size(); i++) {
      indices[i] = followers.get(i).getMatchIndex();
    }
    if (includeSelf) {
      // note that we also need to wait for the local disk I/O
      indices[length - 1] = raftLog.getLatestFlushedIndex();
    }

    Arrays.sort(indices);
    return indices[(indices.length - 1) / 2];
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

  PendingRequest returnNoConfChange(SetConfigurationRequest r) {
    PendingRequest pending = new PendingRequest(r);
    pending.setSuccessReply(null);
    return pending;
  }

  void replyPendingRequest(long logIndex, RaftClientReply reply) {
    pendingRequests.replyPendingRequest(logIndex, reply);
  }

  TransactionContext getTransactionContext(long index) {
    return pendingRequests.getTransactionContext(index);
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
      Iterator<LogAppender> iterator = senders.iterator();
      while (iterator.hasNext()) {
        LogAppender sender = iterator.next();
        if (!sender.getFollower().isAttendingVote()) {
          iterator.remove();
          sender.stopSender();
          sender.interrupt();
        }
      }
      LeaderState.this.stagingState = null;
      // send back failure response to client's request
      pendingRequests.failSetConfiguration(
          new ReconfigurationTimeoutException("Fail to set configuration "
              + newConf + ". Timeout when bootstrapping new peers."));
    }
  }
}
