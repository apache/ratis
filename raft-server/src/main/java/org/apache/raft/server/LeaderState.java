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
package org.apache.raft.server;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotResult;
import org.apache.raft.proto.RaftProtos.SnapshotChunkProto;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.protocol.ReconfigurationTimeoutException;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.protocol.*;
import org.apache.raft.server.protocol.AppendEntriesReply.AppendResult;
import org.apache.raft.server.storage.RaftLog;
import org.apache.raft.server.storage.RaftStorageDirectory.SnapshotPathAndTermIndex;
import org.apache.raft.util.MD5FileUtil;
import org.apache.raft.util.ProtoUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.raft.protocol.Message.EMPTY_MESSAGE;
import static org.apache.raft.server.LeaderState.StateUpdateEventType.*;
import static org.apache.raft.server.RaftServerConstants.*;

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
    return lastTime + RaftServerConstants.RPC_TIMEOUT_MIN_MS / 2 - Time.monotonicNow();
  }

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
  private final PendingRequests pendingRequests;
  private volatile boolean running = true;

  LeaderState(RaftServer server) {
    this.server = server;

    final ServerState state = server.getState();
    this.raftLog = state.getLog();
    this.currentTerm = state.getCurrentTerm();
    eventQ = new ArrayBlockingQueue<>(4096);
    processor = new EventProcessor();
    pendingRequests = new PendingRequests(server);

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
  }

  void start() {
    // In the beginning of the new term, replicate an empty entry in order
    // to finally commit entries in the previous term.
    // Also this message can help identify the last committed index when
    // the leader peer is just started.
    raftLog.append(server.getState().getCurrentTerm(), EMPTY_MESSAGE);

    processor.start();
    startSenders();
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
    pendingRequests.sendResponses(raftLog.getLastCommittedIndex());
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
  PendingRequest startSetConfiguration(SetConfigurationRequest request) {
    Preconditions.checkState(running && !inStagingState());

    RaftPeer[] peersInNewConf = request.getPeersInNewConf();
    Collection<RaftPeer> peersToBootStrap = RaftConfiguration
        .computeNewPeers(peersInNewConf, server.getRaftConf());

    // add the request to the pending queue
    final PendingRequest pending = pendingRequests.addConfRequest(request);

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
    return pending;
  }

  PendingRequest addPendingRequest(long index, RaftClientRequest request) {
    return pendingRequests.addPendingRequest(index, request);
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
              ELECTION_TIMEOUT_MAX_MS, TimeUnit.MILLISECONDS);
          synchronized (server) {
            if (running) {
              handleEvent(event);
            }
          }
          // the updated configuration does not need to be sync'ed here
        } catch (InterruptedException e) {
          if (!running) {
            LOG.info("The LeaderState gets is stopped");
          } else {
            LOG.warn("The leader election thread of peer {} is interrupted. "
                + "Currently role: {}.", server.getId(), server.getRole());
            throw new RuntimeException(e);
          }
        } catch (IOException e) {
          LOG.warn("Failed to persist new votedFor/term.", e);
          // the failure should happen while changing the state to follower
          // thus the in-memory state should have been updated
          Preconditions.checkState(!running);
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
   * 1. If the latest rpc time of the remote peer is before 2 * max_timeout,
   *    the peer made no progress for that long. We should fail the whole
   *    setConfiguration request.
   * 2. If the peer's matching index is just behind for a small gap, and the
   *    peer was updated recently (within max_timeout), declare the peer as
   *    caught-up.
   * 3. Otherwise the peer is making progressing. Keep waiting.
   */
  private BootStrapProgress checkProgress(FollowerInfo follower,
      long committed) {
    Preconditions.checkArgument(!follower.attendVote);
    final long progressTime = Time.monotonicNow() - RPC_TIMEOUT_MAX_MS;
    final long timeoutTime = Time.monotonicNow() - STAGING_NOPROGRESS_TIMEOUT;
    if (follower.lastRpcTime.get() < timeoutTime) {
      LOG.debug("{} detects a follower {} timeout for bootstrapping",
          server.getId(), follower);
      return BootStrapProgress.NOPROGRESS;
    } else if (follower.matchIndex.get() + STAGING_CATCHUP_GAP >
        committed && follower.lastRpcTime.get() > progressTime) {
      return BootStrapProgress.CAUGHTUP;
    } else {
      return BootStrapProgress.PROGRESSING;
    }
  }

  private Collection<BootStrapProgress> checkAllProgress(long committed) {
    Preconditions.checkState(inStagingState());
    return senders.stream()
        .filter(sender -> !sender.follower.attendVote)
        .map(sender -> checkProgress(sender.follower, committed))
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
    final LogEntryProto[] entriesToCommit;
    if (!conf.inTransitionState()) {
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

  private boolean committedConf(LogEntryProto[] entreis) {
    final long currentCommitted = raftLog.getLastCommittedIndex();
    for (LogEntryProto entry : entreis) {
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
      if (conf.inTransitionState()) {
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
            Thread.sleep(ELECTION_TIMEOUT_MIN_MS);
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
    long index = raftLog.append(server.getState().getCurrentTerm(), newConf);
    updateConfiguration(index, newConf);
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
      // note that we also need to wait for the local disk I/O
      indices[length - 1] = raftLog.getLatestFlushedIndex();
    }

    Arrays.sort(indices);
    return indices[(indices.length - 1) / 2];
  }

  private List<List<FollowerInfo>> divideFollowers(RaftConfiguration conf) {
    List<List<FollowerInfo>> lists = new ArrayList<>(2);
    List<FollowerInfo> listForNew = senders.stream()
        .filter(sender -> conf.containsInConf(sender.follower.peer.getId()))
        .map(sender -> sender.follower)
        .collect(Collectors.toList());
    lists.add(listForNew);
    if (conf.inTransitionState()) {
      List<FollowerInfo> listForOld = senders.stream()
          .filter(sender -> conf.containsInOldConf(sender.follower.peer.getId()))
          .map(sender -> sender.follower)
          .collect(Collectors.toList());
      lists.add(listForOld);
    }
    return lists;
  }

  PendingRequest returnNoConfChange(SetConfigurationRequest r) {
    PendingRequest pending = new PendingRequest(r);
    pending.setReply(new RaftClientReply(r, true, null));
    return pending;
  }

  void replyPendingRequest(long logIndex, Exception e) {
    pendingRequests.replyPendingRequest(logIndex, e);
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

    void decreaseNextIndex(long targetIndex) {
      if (nextIndex > 0) {
        nextIndex = Math.min(nextIndex - 1, targetIndex);
      }
    }

    @Override
    public String toString() {
      return peer.getId() + "(next=" + nextIndex + ", match=" + matchIndex
          + ", attendVote=" + attendVote + ", lastRpcTime="
          + lastRpcTime.get() + ")";
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
    private AppendEntriesReply sendAppendEntriesWithRetries()
        throws InterruptedException, InterruptedIOException {
      LogEntryProto[] entries = null;
      int retry = 0;
      while (isSenderRunning()) {
        try {
          final long endIndex = raftLog.getNextIndex();
          if (entries == null || entries.length == 0) {
            entries = raftLog.getEntries(follower.nextIndex, endIndex);
          }
          TermIndex previous = ServerProtoUtils.toTermIndex(
              raftLog.get(follower.nextIndex - 1));
          if (previous == null) {
            // if previous is null, nextIndex must be equal to the log start
            // index (otherwise we will install snapshot).
            Preconditions.checkState(
                follower.nextIndex == raftLog.getStartIndex(),
                "follower's next index %s, local log start index %s",
                follower.nextIndex, raftLog.getStartIndex());
            SnapshotPathAndTermIndex si = server.getState().getLatestSnapshot();
            previous = si == null ? null : si.getTermIndex();
          }
          if (entries != null || previous != null) {
            LOG.trace("follower {}, log {}", follower, raftLog);
          }

          final AppendEntriesRequest request = server.createAppendEntriesRequest(
              follower.peer.getId(), previous, entries, !follower.attendVote);
          final AppendEntriesReply r = (AppendEntriesReply) server
              .getServerRpc().sendServerRequest(request);

          follower.lastRpcTime.set(Time.monotonicNow());
          if (r.isSuccess()) {
            if (entries != null && entries.length > 0) {
              final long mi = entries[entries.length - 1].getIndex();
              follower.updateMatchIndex(mi);
              follower.updateNextIndex(mi + 1);

              StateUpdateEvent e = follower.attendVote ?
                  UPDATE_COMMIT_EVENT : STAGING_PROGRESS_EVENT;
              submitUpdateStateEvent(e);
            }
          }
          return r;
        } catch (InterruptedIOException iioe) {
          throw iioe;
        } catch (IOException ioe) {
          LOG.debug(this + ": failed to send appendEntries; retry " + retry++,
              ioe);
        }
        if (isSenderRunning()) {
          Thread.sleep(RPC_SLEEP_TIME_MS);
        }
      }
      return null;
    }

    private SnapshotChunkProto readChunk(FileInputStream in, byte[] buf,
        int length, long offset, int chunkIndex) throws IOException {
      SnapshotChunkProto.Builder builder = SnapshotChunkProto.newBuilder()
          .setOffset(offset).setChunkIndex(chunkIndex);
      IOUtils.readFully(in, buf, 0, length);
      builder.setData(ByteString.copyFrom(buf, 0, length));
      return builder.build();
    }

    // TODO inefficient using RPC. need to change to zero-copy transfer
    private InstallSnapshotReply installSnapshot(SnapshotPathAndTermIndex si)
        throws InterruptedException, InterruptedIOException {
      File snapshotFile = si.path.toFile();
      final long totalSize = snapshotFile.length();
      final int bufLength = (int) Math.min(SNAPSHOT_CHUNK_MAX_SIZE, totalSize);
      final byte[] buf = new byte[bufLength];
      long offset = 0;
      int chunkIndex = 0;
      try (FileInputStream in = new FileInputStream(snapshotFile)) {
        MD5Hash fileDigest = MD5FileUtil.readStoredMd5ForFile(snapshotFile);
        InstallSnapshotReply reply = null;
        while (offset < totalSize) {
          int targetLength = (int) Math.min(totalSize - offset,
              SNAPSHOT_CHUNK_MAX_SIZE);
          SnapshotChunkProto chunk = readChunk(in, buf, targetLength, offset,
              chunkIndex);
          InstallSnapshotRequest request = server.createInstallSnapshotRequest(
              follower.peer.getId(), si, chunk, totalSize, fileDigest);

          reply = (InstallSnapshotReply) server.getServerRpc()
              .sendServerRequest(request);
          follower.lastRpcTime.set(Time.monotonicNow());

          if (!reply.isSuccess()) {
            return reply;
          }

          offset += targetLength;
          chunkIndex++;
        }
        if (reply != null) {
          follower.updateMatchIndex(si.endIndex);
          follower.updateNextIndex(si.endIndex + 1);
          LOG.info("{}: install snapshot-{} successfully on follower {}",
              server.getId(), si.endIndex, follower.peer);
        }
        return reply;
      } catch (InterruptedIOException iioe) {
          throw iioe;
      } catch (IOException ioe) {
        LOG.warn(this + ": failed to install Snapshot at offset " + offset, ioe);
      }
      return null;
    }

    /** Check and send appendEntries RPC */
    private void checkAndSendAppendEntries() throws InterruptedException,
        InterruptedIOException {
      while (isSenderRunning()) {
        if (shouldSend()) {
          final long logStartIndex = raftLog.getStartIndex();
          SnapshotPathAndTermIndex si = server.getState().getLatestSnapshot();
          // we should install snapshot if the follower needs to catch up and:
          // 1. there is no local log entry but there is snapshot
          // 2. or the follower's next index is smaller than the log start index
          boolean toInstallSnapshot = false;
          if (follower.nextIndex < raftLog.getNextIndex()) {
            toInstallSnapshot = (follower.nextIndex < logStartIndex) ||
                (logStartIndex == INVALID_LOG_INDEX && si != null);
          }

          if (toInstallSnapshot) {
            LOG.info("{}: follower {}'s next index is {}," +
                " log's start index is {}, need to install snapshot",
                server.getId(), follower.peer, follower.nextIndex, logStartIndex);
            Preconditions.checkState(si != null);

            final InstallSnapshotReply r = installSnapshot(si);
            if (r != null && r.getResult() == InstallSnapshotResult.NOT_LEADER) {
              checkResponseTerm(r.getTerm());
            } // otherwise if r is null, retry the snapshot installation
          } else {
            final AppendEntriesReply r = sendAppendEntriesWithRetries();
            if (r == null) {
              break;
            }

            // check if should step down
            if (r.getResult() == AppendResult.NOT_LEADER) {
              checkResponseTerm(r.getTerm());
            } else if (r.getResult() == AppendResult.INCONSISTENCY) {
              follower.decreaseNextIndex(r.getNextIndex());
            }
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
      return follower.nextIndex < raftLog.getNextIndex() ||
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

  private class ConfigurationStagingState {
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

    void fail() {
      Iterator<RpcSender> iterator = senders.iterator();
      while (iterator.hasNext()) {
        RpcSender sender = iterator.next();
        if (!sender.follower.attendVote) {
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
