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
import org.apache.hadoop.raft.protocol.RaftClientRequest;
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.raft.server.protocol.AppendEntriesRequest;
import org.apache.hadoop.raft.server.protocol.Entry;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerReply;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.raft.protocol.Message.EMPTY_MESSAGE;
import static org.apache.hadoop.raft.server.LeaderState.StateUpdateEventType.STAGINGPROGRESS;
import static org.apache.hadoop.raft.server.LeaderState.StateUpdateEventType.STEPDOWN;
import static org.apache.hadoop.raft.server.LeaderState.StateUpdateEventType.UPDATECOMMIT;
import static org.apache.hadoop.raft.server.RaftConstants.RPC_TIMEOUT_MAX_MS;

/**
 * States for leader only. It contains three different types of processors:
 * 1. RPC senders: each thread is appending log to a follower
 * 2. EventProcessor: a single thread updating the raft server's state based on
 *                    status of log appending response
 * 3. PendingRequestHandler: a handler sending back responses to clients when
 *                           corresponding log entries are committed
 */
class LeaderState {
  private static final Logger LOG = RaftServer.LOG;

  /**
   * @return the time in milliseconds that the leader should send a heartbeat.
   */
  private static long getHeartbeatRemainingTime(long lastTime) {
    return lastTime + RaftConstants.RPC_TIMEOUT_MIN_MS / 2 - Time.monotonicNow();
  }

  enum StateUpdateEventType {
    STEPDOWN, UPDATECOMMIT, STAGINGPROGRESS
  }

  private static class StateUpdateEvent {
    final StateUpdateEventType type;
    final long newTerm;

    StateUpdateEvent(StateUpdateEventType type, long newTerm) {
      this.type = type;
      this.newTerm = newTerm;
    }
  }

  private static final StateUpdateEvent UPDATE_COMMIT_EVENT =
      new StateUpdateEvent(StateUpdateEventType.UPDATECOMMIT, -1);
  private static final StateUpdateEvent STAGING_PROGRESS_EVENT =
      new StateUpdateEvent(StateUpdateEventType.STAGINGPROGRESS, -1);

  private final RaftServer server;
  private final RaftLog raftLog;
  private final long currentTerm;
  private volatile ConfigurationStagingState stagingState;
  private List<List<FollowerInfo>> voterLists;

  /**
   * The list of threads appending entries to followers.
   * The list is protected by the RaftServer's lock.
   */
  private final List<RpcSender> senders;
  private final BlockingQueue<StateUpdateEvent> eventQ;
  private final EventProcessor processor;
  private final PendingRequestsHandler pendingRequests;
  private volatile boolean running = true;

  LeaderState(RaftServer server) {
    this.server = server;

    final ServerState state = server.getState();
    this.raftLog = state.getLog();
    this.currentTerm = state.getCurrentTerm();
    eventQ = new LinkedBlockingQueue<>();
    processor = new EventProcessor();
    pendingRequests = new PendingRequestsHandler(server);

    final RaftConfiguration conf = server.getRaftConf();
    Collection<RaftPeer> others = conf.getOtherPeers(state.getSelfId());
    final long t = Time.monotonicNow() - RPC_TIMEOUT_MAX_MS;
    final long nextIndex = raftLog.getNextIndex();
    senders = new ArrayList<>(others.size());
    for (RaftPeer p : others) {
      FollowerInfo f = new FollowerInfo(p, t, nextIndex, true);
      senders.add(new RpcSender(f));
    }
    voterLists = divideFollowers(conf);

    // In the beginning of the new term, replicate an empty entry in order
    // to finally commit entries in the previous term
    raftLog.apply(server.getState().getCurrentTerm(), EMPTY_MESSAGE);
  }

  void start() {
    processor.start();
    startSenders();
    pendingRequests.start();
  }

  private void startSenders() {
    senders.forEach(Thread::start);
  }

  void stop() {
    this.running = false;
    // do not interrupt event processor since it may be in the middle of logSync
    for (RpcSender sender : senders) {
      sender.stopSender();
      sender.interrupt();
    }
    pendingRequests.stop();
  }

  void notifySenders() {
    senders.forEach(RpcSender::notifyAppend);
  }

  boolean inStagingState() {
    return stagingState != null;
  }

  ConfigurationStagingState getStagingState() {
    return stagingState;
  }

  /**
   * Start bootstrapping new peers
   */
  void startSetConfiguration(SetConfigurationRequest request) {
    Preconditions.checkState(running && !inStagingState());

    RaftPeer[] peersInNewConf = request.getPeersInNewConf();
    Collection<RaftPeer> peersToBootStrap = RaftConfiguration
        .computeNewPeers(peersInNewConf, server.getRaftConf());

    // add the request to the pending queue
    pendingRequests.addConfRequest(request);

    ConfigurationStagingState stagingState = new ConfigurationStagingState(
        peersToBootStrap, new SimpleConfiguration(peersInNewConf));
    Collection<RaftPeer> newPeers = stagingState.getNewPeers();
    // set the staging state
    this.stagingState = stagingState;

    if (newPeers.isEmpty()) {
      applyOldNewConf();
    } else {
      // update the LeaderState's sender list
      addSenders(newPeers);
    }
  }

  void addPendingRequest(long index, RaftClientRequest request) {
    pendingRequests.put(index, request);
  }

  private void applyOldNewConf() {
    final ServerState state = server.getState();
    final RaftConfiguration current = server.getRaftConf();
    final RaftConfiguration oldNewConf= stagingState.generateOldNewConf(current,
        state.getLog().getNextIndex());
    // apply the (old, new) configuration to log, and use it as the current conf
    state.getLog().apply(state.getCurrentTerm(), current, oldNewConf);
    updateConfiguration(oldNewConf);

    this.stagingState = null;
    notifySenders();
  }

  private void updateConfiguration(RaftConfiguration newConf) {
    voterLists = divideFollowers(newConf);
    server.getState().setRaftConf(newConf);
  }

  /**
   * After receiving a setConfiguration request, the leader should update its
   * RpcSender list.
   */
  void addSenders(Collection<RaftPeer> newMembers) {
    final long t = Time.monotonicNow() - RPC_TIMEOUT_MAX_MS;
    final long nextIndex = raftLog.getNextIndex();
    for (RaftPeer peer : newMembers) {
      FollowerInfo f = new FollowerInfo(peer, t, nextIndex, false);
      RpcSender sender = new RpcSender(f);
      senders.add(sender);
      sender.start();
    }
  }

  /**
   * Update the RpcSender list based on the current configuration
   */
  private void updateSenders() {
    final RaftConfiguration conf = server.getRaftConf();
    Preconditions.checkState(conf.inStableState() && !inStagingState());
    Iterator<RpcSender> iterator = senders.iterator();
    while (iterator.hasNext()) {
      RpcSender sender = iterator.next();
      if (!conf.containsInConf(sender.follower.peer.getId())) {
        iterator.remove();
        sender.stopSender();
        sender.interrupt();
      }
    }
  }

  void submitUpdateStateEvent(StateUpdateEvent e) {
    eventQ.offer(e);
  }

  private void prepare() {
    synchronized (server) {
      if (running) {
        final RaftConfiguration conf = server.getRaftConf();
        if (conf.inTransitionState() && server.getState().isConfCommitted()) {
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
          StateUpdateEvent event = eventQ.poll(
              RaftConstants.ELECTION_TIMEOUT_MAX_MS, TimeUnit.MILLISECONDS);
          synchronized (server) {
            if (running) {
              handleEvent(event);
            }
          }
          raftLog.logSync();
        } catch (InterruptedException e) {
          if (!running) {
            LOG.info("The LeaderState gets is stopped");
          } else {
            LOG.warn("The leader election thread of peer {} is interrupted. "
                + "Currently role: {}.", server.getId(), server.getRole());
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  private void handleEvent(StateUpdateEvent e) {
    if (e == null) {
      if (inStagingState()) {
        checkNewPeers();
      }
    } else {
      if (e.type == STEPDOWN) {
        server.changeToFollower(e.newTerm);
      } else if (e.type == UPDATECOMMIT) {
        updateLastCommitted();
      } else if (e.type == STAGINGPROGRESS) {
        checkNewPeers();
      }
    }
  }

  /**
   * A simple implementation for catchup checking.
   * TODO: later we can add more advanced checking algorithm
   */
  private boolean isCaughtup(FollowerInfo follower, long committed,
      long expectedLastRpcTime) {
    Preconditions.checkArgument(!follower.attendVote);
    return follower.matchIndex.get() + RaftConstants.STAGING_CATCHUP_GAP >
        committed && follower.lastRpcTime.get() > expectedLastRpcTime;
  }

  private boolean allCaughtup(long committed) {
    Preconditions.checkState(inStagingState());
    final long expectedLastRpcTime = Time.monotonicNow() - RPC_TIMEOUT_MAX_MS;
    for (RpcSender sender : senders) {
      if (!sender.follower.attendVote) { // follower in bootstrapping stage
        if (!isCaughtup(sender.follower, committed, expectedLastRpcTime)) {
          return false;
        }
      }
    }
    return true;
  }

  private void checkNewPeers() {
    if (!inStagingState()) {
      // it is possible that the bootstrapping is done and we still have
      // remaining STAGINGPROGRESS event to handle.
      updateLastCommitted();
    } else {
      if (allCaughtup(server.getState().getLog().getLastCommittedIndex())) {
        applyOldNewConf();
        for (RpcSender sender : senders) {
          sender.follower.startAttendVote();
        }
      }
    }
  }

  boolean isBootStrappingPeer(String peerId) {
    return inStagingState() && getStagingState().contains(peerId);
  }

  private void updateLastCommitted() {
    final String selfId = server.getId();
    final RaftConfiguration conf = server.getRaftConf();
    long majorityInNewConf = computeLastCommitted(voterLists.get(0),
        conf.containsInConf(selfId));
    final long oldLastCommitted = raftLog.getLastCommittedIndex();
    if (!conf.inTransitionState()) {
      raftLog.updateLastCommitted(majorityInNewConf, currentTerm);
    } else { // configuration is in transitional state
      long majorityInOldConf = computeLastCommitted(voterLists.get(1),
          conf.containsInOldConf(selfId));
      final long majority = Math.min(majorityInNewConf, majorityInOldConf);
      raftLog.updateLastCommitted(majority, currentTerm);
    }
    checkAndUpdateConfiguration(oldLastCommitted);
  }

  private void checkAndUpdateConfiguration(long oldLastCommitted) {
    final RaftConfiguration conf = server.getRaftConf();
    if (raftLog.committedConfEntry(oldLastCommitted)) {
      if (conf.inTransitionState()) {
        replicateNewConf();
      } else { // the (new) log entry has been committed
        pendingRequests.finishSetConfiguration(true);
        // if the leader is not included in the current configuration, step down
        if (!conf.containsInConf(server.getId())) {
          LOG.info("{} is not included in the new configuration {}. Step down.",
              server.getId(), conf);
          try {
            // leave some time for all RPC senders to send out new conf entry
            Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MIN_MS);
          } catch (InterruptedException ignored) {
          }
          // the pending request handler will send NotLeaderException for
          // pending client requests when it stops
          server.kill();
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
    RaftConfiguration newConf = conf.generateNewConf(raftLog.getNextIndex());
    raftLog.apply(server.getState().getCurrentTerm(), conf, newConf);
    updateConfiguration(newConf);
    updateSenders();
    notifySenders();
  }

  private long computeLastCommitted(List<FollowerInfo> followers,
      boolean includeSelf) {
    final int length = includeSelf ? followers.size() + 1 : followers.size();
    final long[] indices = new long[length];
    for (int i = 0; i < followers.size(); i++) {
      indices[i] = followers.get(i).matchIndex.get();
    }
    if (includeSelf) {
      indices[length - 1] = raftLog.getNextIndex() - 1;
    }

    Arrays.sort(indices);
    return indices[(indices.length - 1) / 2];
  }

  private List<List<FollowerInfo>> divideFollowers(RaftConfiguration conf) {
    List<List<FollowerInfo>> lists = new ArrayList<>(2);
    List<FollowerInfo> listForNew = new ArrayList<>();
    for (RpcSender sender : senders) {
      if (conf.containsInConf(sender.follower.peer.getId())) {
        listForNew.add(sender.follower);
      }
    }
    lists.add(listForNew);
    if (conf.inTransitionState()) {
      List<FollowerInfo> listForOld = new ArrayList<>();
      for (RpcSender sender : senders) {
        if (conf.containsInOldConf(sender.follower.peer.getId())) {
          listForOld.add(sender.follower);
        }
      }
      lists.add(listForOld);
    }
    return lists;
  }

  void returnNoConfChange(SetConfigurationRequest r) {
    pendingRequests.addConfRequest(r);
    pendingRequests.finishSetConfiguration(true);
  }

  private class FollowerInfo {
    private final RaftPeer peer;
    private final AtomicLong lastRpcTime;
    private long nextIndex;
    private final AtomicLong matchIndex;
    private volatile boolean attendVote;

    FollowerInfo(RaftPeer peer, long lastRpcTime, long nextIndex,
        boolean attendVote) {
      this.peer = peer;
      this.lastRpcTime = new AtomicLong(lastRpcTime);
      this.nextIndex = nextIndex;
      this.matchIndex = new AtomicLong(0);
      this.attendVote = attendVote;
    }

    void updateMatchIndex(final long matchIndex) {
      this.matchIndex.set(matchIndex);
    }

    void updateNextIndex(long i) {
      nextIndex = i;
    }

    void decreaseNextIndex() {
      nextIndex--;
    }

    @Override
    public String toString() {
      return peer.getId() + "(next=" + nextIndex + ", match=" + matchIndex + ")";
    }

    void startAttendVote() {
      attendVote = true;
    }
  }

  class RpcSender extends Daemon {
    private final FollowerInfo follower;
    private volatile boolean sending = true;

    public RpcSender(FollowerInfo f) {
      this.follower = f;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + server.getId()
          + " -> " + follower.peer.getId() + ")";
    }

    @Override
    public void run() {
      try {
        checkAndSendAppendEntries();
      } catch (InterruptedException | InterruptedIOException e) {
        LOG.info(this + " was interrupted: " + e);
        LOG.trace("TRACE", e);
      }
    }

    private boolean isSenderRunning() {
      return running && sending;
    }

    void stopSender() {
      this.sending = false;
    }

    /** Send an appendEntries RPC; retry indefinitely. */
    private RaftServerReply sendAppendEntriesWithRetries()
        throws InterruptedException, InterruptedIOException {
      Entry[] entries = null;
      int retry = 0;
      while (isSenderRunning()) {
        try {
          if (entries == null || entries.length == 0) {
            entries = raftLog.getEntries(follower.nextIndex);
          }
          final TermIndex previous = raftLog.get(follower.nextIndex - 1);
          if (entries != null || previous != null) {
            LOG.trace("follower {}, log {}", follower, raftLog);
          }
          AppendEntriesRequest request = server.createAppendEntriesRequest(
              follower.peer.getId(), previous, entries);
          final RaftServerReply r = server.sendAppendEntries(request);
          follower.lastRpcTime.set(Time.monotonicNow());
          if (r.isSuccess()) {
            if (!follower.attendVote) {
              // if there is progress, submit STAGING_PROGRESS_EVENT.
              // even if already caught up, still submit an event to trigger the
              // progress check
              submitUpdateStateEvent(STAGING_PROGRESS_EVENT);
            } else if (entries != null && entries.length > 0) {
              final long mi = entries[entries.length - 1].getIndex();
              follower.updateMatchIndex(mi);
              follower.updateNextIndex(mi + 1);
              submitUpdateStateEvent(UPDATE_COMMIT_EVENT);
            }
          }
          return r;
        } catch (InterruptedIOException iioe) {
          throw iioe;
        } catch (IOException ioe) {
          LOG.warn(this + ": failed to send appendEntries; retry " + retry++,
              ioe);
        }
        if (isSenderRunning()) {
          Thread.sleep(RaftConstants.RPC_SLEEP_TIME_MS);
        }
      }
      return null;
    }

    /** Check and send appendEntries RPC */
    private void checkAndSendAppendEntries() throws InterruptedException,
        InterruptedIOException {
      while (isSenderRunning()) {
        if (shouldSend()) {
          final RaftServerReply r = sendAppendEntriesWithRetries();
          if (r == null) {
            break;
          }

          // check if should step down
          checkResponseTerm(r.getTerm());

          if (!r.isSuccess()) {
            // TODO implement the optimization in Section 5.3. (this + snapshot)
            // is useful for bootstrapping new peers. Also need to handle other
            // failure cases (e.g., a new peer has not been correctly formatted)
            follower.decreaseNextIndex();
          }
        }
        if (isSenderRunning()) {
          synchronized (this) {
            wait(getHeartbeatRemainingTime(follower.lastRpcTime.get()));
          }
        }
      }
    }

    synchronized void notifyAppend() {
      this.notify();
    }

    /** Should the leader send appendEntries RPC to this follower? */
    private boolean shouldSend() {
      return raftLog.get(follower.nextIndex) != null ||
          getHeartbeatRemainingTime(follower.lastRpcTime.get()) <= 0;
    }

    private void checkResponseTerm(long responseTerm) {
      synchronized (server) {
        if (isSenderRunning() && follower.attendVote &&
            responseTerm > currentTerm) {
          submitUpdateStateEvent(
              new StateUpdateEvent(StateUpdateEventType.STEPDOWN, responseTerm));
        }
      }
    }
  }

  static class ConfigurationStagingState {
    private final Map<String, RaftPeer> newPeers;
    private final SimpleConfiguration newConf;

    ConfigurationStagingState(Collection<RaftPeer> newPeers,
        SimpleConfiguration newConf) {
      Map<String, RaftPeer> map = new HashMap<>();
      for (RaftPeer peer : newPeers) {
        map.put(peer.getId(), peer);
      }
      this.newPeers = Collections.unmodifiableMap(map);
      this.newConf = newConf;
    }

    RaftConfiguration generateOldNewConf(RaftConfiguration current,
        long logIndex) {
      return current.generateOldNewConf(newConf, logIndex);
    }

    Collection<RaftPeer> getNewPeers() {
      return newPeers.values();
    }

    boolean contains(String peerId) {
      return newPeers.containsKey(peerId);
    }
  }
}
