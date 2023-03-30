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
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.LeaderElection.Phase;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.memory.MemoryRaftLog;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.server.storage.*;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.apache.ratis.server.RaftServer.Division.LOG;

/**
 * Common states of a raft peer. Protected by RaftServer's lock.
 */
class ServerState implements Closeable {
  private final RaftGroupMemberId memberId;
  private final RaftServerImpl server;
  /** Raft log */
  private final MemoizedSupplier<RaftLog> log;
  /** Raft configuration */
  private final ConfigurationManager configurationManager;
  /** The thread that applies committed log entries to the state machine */
  private final MemoizedSupplier<StateMachineUpdater> stateMachineUpdater;
  /** local storage for log and snapshot */
  private RaftStorageImpl storage;
  private final SnapshotManager snapshotManager;
  private volatile Timestamp lastNoLeaderTime;
  private final TimeDuration noLeaderTimeout;

  private final ReadRequests readRequests;

  /**
   * Latest term server has seen.
   * Initialized to 0 on first boot, increases monotonically.
   */
  private final AtomicLong currentTerm = new AtomicLong();
  /**
   * The server ID of the leader for this term. Null means either there is
   * no leader for this term yet or this server does not know who it is yet.
   */
  private final AtomicReference<RaftPeerId> leaderId = new AtomicReference<>();
  /**
   * Candidate that this peer granted vote for in current term (or null if none)
   */
  private volatile RaftPeerId votedFor;

  /**
   * Latest installed snapshot for this server. This maybe different than StateMachine's latest
   * snapshot. Once we successfully install a snapshot, the SM may not pick it up immediately.
   * Further, this will not get updated when SM does snapshots itself.
   */
  private final AtomicReference<TermIndex> latestInstalledSnapshot = new AtomicReference<>();

  ServerState(RaftPeerId id, RaftGroup group, RaftProperties prop,
              RaftServerImpl server, StateMachine stateMachine)
      throws IOException {
    this.memberId = RaftGroupMemberId.valueOf(id, group.getGroupId());
    this.server = server;
    Collection<RaftPeer> followerPeers = group.getPeers().stream()
        .filter(peer -> peer.getStartupRole() == RaftPeerRole.FOLLOWER)
        .collect(Collectors.toList());
    Collection<RaftPeer> listenerPeers = group.getPeers().stream()
        .filter(peer -> peer.getStartupRole() == RaftPeerRole.LISTENER)
        .collect(Collectors.toList());
    final RaftConfigurationImpl initialConf = RaftConfigurationImpl.newBuilder()
        .setConf(followerPeers, listenerPeers)
        .build();
    configurationManager = new ConfigurationManager(initialConf);
    LOG.info("{}: {}", getMemberId(), configurationManager);

    boolean storageFound = false;
    List<File> directories = RaftServerConfigKeys.storageDir(prop);
    while (!directories.isEmpty()) {
      // use full uuid string to create a subdirectory
      File dir = chooseStorageDir(directories, group.getGroupId().getUuid().toString());
      try {
        storage = (RaftStorageImpl) RaftStorage.newBuilder()
            .setDirectory(dir)
            .setOption(RaftStorage.StartupOption.RECOVER)
            .setLogCorruptionPolicy(RaftServerConfigKeys.Log.corruptionPolicy(prop))
            .setStorageFreeSpaceMin(RaftServerConfigKeys.storageFreeSpaceMin(prop))
            .build();
        storageFound = true;
        break;
      } catch (IOException e) {
        if (e.getCause() instanceof OverlappingFileLockException) {
          throw e;
        }
        LOG.warn("Failed to init RaftStorage under {} for {}: {}",
            dir.getParent(), group.getGroupId().getUuid().toString(), e);
        directories.removeIf(d -> d.getAbsolutePath().equals(dir.getParent()));
      }
    }

    if (!storageFound) {
      throw new IOException("No healthy directories found for RaftStorage among: " +
          RaftServerConfigKeys.storageDir(prop));
    }

    snapshotManager = new SnapshotManager(id, storage.getStorageDir(), stateMachine.getStateMachineStorage());

    // On start the leader is null, start the clock now
    this.lastNoLeaderTime = Timestamp.currentTime();
    this.noLeaderTimeout = RaftServerConfigKeys.Notification.noLeaderTimeout(prop);

    final LongSupplier getSnapshotIndexFromStateMachine = () -> Optional.ofNullable(stateMachine.getLatestSnapshot())
        .map(SnapshotInfo::getIndex)
        .filter(i -> i >= 0)
        .orElse(RaftLog.INVALID_LOG_INDEX);
    this.log = JavaUtils.memoize(() -> initRaftLog(getSnapshotIndexFromStateMachine, prop));
    this.readRequests = new ReadRequests(prop, stateMachine);
    this.stateMachineUpdater = JavaUtils.memoize(() -> new StateMachineUpdater(
        stateMachine, server, this, getLog().getSnapshotIndex(), prop,
        this.readRequests.getAppliedIndexConsumer()));
  }

  void initialize(StateMachine stateMachine) throws IOException {
    storage.initialize();
    // read configuration from the storage
    Optional.ofNullable(storage.readRaftConfiguration()).ifPresent(this::setRaftConf);

    stateMachine.initialize(server.getRaftServer(), getMemberId().getGroupId(), storage);

    // we cannot apply log entries to the state machine in this step, since we
    // do not know whether the local log entries have been committed.
    final RaftStorageMetadata metadata = log.get().loadMetadata();
    currentTerm.set(metadata.getTerm());
    votedFor = metadata.getVotedFor();
  }

  RaftGroupMemberId getMemberId() {
    return memberId;
  }

  static File chooseStorageDir(List<File> volumes, String targetSubDir) throws IOException {
    final Map<File, Integer> numberOfStorageDirPerVolume = new HashMap<>();
    final File[] empty = {};
    final List<File> resultList = new ArrayList<>();
    volumes.stream().flatMap(volume -> {
      final File[] dirs = Optional.ofNullable(volume.listFiles()).orElse(empty);
      numberOfStorageDirPerVolume.put(volume, dirs.length);
      return Arrays.stream(dirs);
    }).filter(dir -> targetSubDir.equals(dir.getName()))
        .forEach(resultList::add);

    if (resultList.size() > 1) {
      throw new IOException("More than one directories found for " + targetSubDir + ": " + resultList);
    }
    if (resultList.size() == 1) {
      return resultList.get(0);
    }
    return numberOfStorageDirPerVolume.entrySet().stream()
        .min(Map.Entry.comparingByValue())
        .map(Map.Entry::getKey)
        .map(v -> new File(v, targetSubDir))
        .orElseThrow(() -> new IOException("No storage directory found."));
  }

  void writeRaftConfiguration(LogEntryProto conf) {
    storage.writeRaftConfiguration(conf);
  }

  void start() {
    stateMachineUpdater.get().start();
  }

  private RaftLog initRaftLog(LongSupplier getSnapshotIndexFromStateMachine, RaftProperties prop) {
    try {
      return initRaftLog(getMemberId(), server, storage, this::setRaftConf, getSnapshotIndexFromStateMachine, prop);
    } catch (IOException e) {
      throw new IllegalStateException(getMemberId() + ": Failed to initRaftLog.", e);
    }
  }

  private static RaftLog initRaftLog(RaftGroupMemberId memberId, RaftServerImpl server, RaftStorage storage,
      Consumer<LogEntryProto> logConsumer, LongSupplier getSnapshotIndexFromStateMachine,
      RaftProperties prop) throws IOException {
    final RaftLog log;
    if (RaftServerConfigKeys.Log.useMemory(prop)) {
      log = new MemoryRaftLog(memberId, getSnapshotIndexFromStateMachine, prop);
    } else {
      log = new SegmentedRaftLog(memberId, server,
          server.getStateMachine(),
          server::notifyTruncatedLogEntry,
          server::submitUpdateCommitEvent,
          storage, getSnapshotIndexFromStateMachine, prop);
    }
    log.open(log.getSnapshotIndex(), logConsumer);
    return log;
  }

  RaftConfigurationImpl getRaftConf() {
    return configurationManager.getCurrent();
  }

  long getCurrentTerm() {
    return currentTerm.get();
  }

  boolean updateCurrentTerm(long newTerm) {
    final long current = currentTerm.getAndUpdate(curTerm -> Math.max(curTerm, newTerm));
    if (newTerm > current) {
      votedFor = null;
      setLeader(null, "updateCurrentTerm");
      return true;
    }
    return false;
  }

  RaftPeerId getLeaderId() {
    return leaderId.get();
  }

  boolean hasLeader() {
    return leaderId != null;
  }

  /**
   * Become a candidate and start leader election
   */
  LeaderElection.ConfAndTerm initElection(Phase phase) throws IOException {
    setLeader(null, phase);
    final long term;
    if (phase == Phase.PRE_VOTE) {
      term = getCurrentTerm();
    } else if (phase == Phase.ELECTION) {
      term = currentTerm.incrementAndGet();
      votedFor = getMemberId().getPeerId();
      persistMetadata();
    } else {
      throw new IllegalArgumentException("Unexpected phase " + phase);
    }
    return new LeaderElection.ConfAndTerm(getRaftConf(), term);
  }

  void persistMetadata() throws IOException {
    getLog().persistMetadata(RaftStorageMetadata.valueOf(currentTerm.get(), votedFor));
  }

  RaftPeerId getVotedFor() {
    return votedFor;
  }

  /**
   * Vote for a candidate and update the local state.
   */
  void grantVote(RaftPeerId candidateId) {
    votedFor = candidateId;
    setLeader(null, "grantVote");
  }

  void setLeader(RaftPeerId newLeaderId, Object op) {
    final RaftPeerId oldLeaderId = leaderId.getAndSet(newLeaderId);
    if (!Objects.equals(oldLeaderId, newLeaderId)) {
      String suffix;
      if (newLeaderId == null) {
        // reset the time stamp when a null leader is assigned
        lastNoLeaderTime = Timestamp.currentTime();
        suffix = "";
      } else {
        Timestamp previous = lastNoLeaderTime;
        lastNoLeaderTime = null;
        suffix = ", leader elected after " + previous.elapsedTimeMs() + "ms";
        server.getStateMachine().event().notifyLeaderChanged(getMemberId(), newLeaderId);
      }
      LOG.info("{}: change Leader from {} to {} at term {} for {}{}",
          getMemberId(), oldLeaderId, newLeaderId, getCurrentTerm(), op, suffix);
      if (newLeaderId != null) {
        server.onGroupLeaderElected();
      }
    }
  }

  boolean shouldNotifyExtendedNoLeader() {
    return Optional.ofNullable(lastNoLeaderTime)
        .map(Timestamp::elapsedTime)
        .filter(t -> t.compareTo(noLeaderTimeout) > 0)
        .isPresent();
  }

  long getLastLeaderElapsedTimeMs() {
    return Optional.ofNullable(lastNoLeaderTime).map(Timestamp::elapsedTimeMs).orElse(0L);
  }

  void becomeLeader() {
    setLeader(getMemberId().getPeerId(), "becomeLeader");
  }

  StateMachineUpdater getStateMachineUpdater() {
    if (!stateMachineUpdater.isInitialized()) {
      throw new IllegalStateException(getMemberId() + ": stateMachineUpdater is uninitialized.");
    }
    return stateMachineUpdater.get();
  }

  RaftLog getLog() {
    if (!log.isInitialized()) {
      throw new IllegalStateException(getMemberId() + ": log is uninitialized.");
    }
    return log.get();
  }

  TermIndex getLastEntry() {
    TermIndex lastEntry = getLog().getLastEntryTermIndex();
    if (lastEntry == null) {
      // lastEntry may need to be derived from snapshot
      SnapshotInfo snapshot = getLatestSnapshot();
      if (snapshot != null) {
        lastEntry = snapshot.getTermIndex();
      }
    }

    return lastEntry;
  }

  void appendLog(TransactionContext operation) throws StateMachineException {
    getLog().append(currentTerm.get(), operation);
    Objects.requireNonNull(operation.getLogEntry());
  }

  /**
   * Check if accept the leader selfId and term from the incoming AppendEntries rpc.
   * If accept, update the current state.
   * @return true if the check passes
   */
  boolean recognizeLeader(RaftPeerId peerLeaderId, long leaderTerm) {
    final long current = currentTerm.get();
    if (leaderTerm < current) {
      return false;
    }
    final RaftPeerId curLeaderId = getLeaderId();
    if (leaderTerm > current || curLeaderId == null) {
      // If the request indicates a term that is greater than the current term
      // or no leader has been set for the current term, make sure to update
      // leader and term later
      return true;
    }
    return curLeaderId.equals(peerLeaderId);
  }

  static int compareLog(TermIndex lastEntry, TermIndex candidateLastEntry) {
    if (lastEntry == null) {
      // If the lastEntry of candidate is null, the proto will transfer an empty TermIndexProto,
      // then term and index of candidateLastEntry will both be 0.
      // Besides, candidateLastEntry comes from proto now, it never be null.
      // But we still check candidateLastEntry == null here,
      // to avoid candidateLastEntry did not come from proto in future.
      if (candidateLastEntry == null ||
          (candidateLastEntry.getTerm() == 0 && candidateLastEntry.getIndex() == 0)) {
        return 0;
      }
      return -1;
    } else if (candidateLastEntry == null) {
      return 1;
    }

    return lastEntry.compareTo(candidateLastEntry);
  }

  @Override
  public String toString() {
    return getMemberId() + ":t" + currentTerm + ", leader=" + getLeaderId()
        + ", voted=" + votedFor + ", raftlog=" + log + ", conf=" + getRaftConf();
  }

  boolean isConfCommitted() {
    return getLog().getLastCommittedIndex() >= getRaftConf().getLogEntryIndex();
  }

  void setRaftConf(LogEntryProto entry) {
    if (entry.hasConfigurationEntry()) {
      setRaftConf(LogProtoUtils.toRaftConfiguration(entry));
    }
  }

  void setRaftConf(RaftConfiguration conf) {
    configurationManager.addConfiguration(conf);
    server.getServerRpc().addRaftPeers(conf.getAllPeers());
    final Collection<RaftPeer> listeners = conf.getAllPeers(RaftPeerRole.LISTENER);
    if (!listeners.isEmpty()) {
      server.getServerRpc().addRaftPeers(listeners);
    }
    LOG.info("{}: set configuration {}", getMemberId(), conf);
    LOG.trace("{}: {}", getMemberId(), configurationManager);
  }

  void updateConfiguration(List<LogEntryProto> entries) {
    if (entries != null && !entries.isEmpty()) {
      configurationManager.removeConfigurations(entries.get(0).getIndex());
      entries.stream().forEach(this::setRaftConf);
    }
  }

  boolean updateCommitIndex(long majorityIndex, long curTerm, boolean isLeader) {
    if (getLog().updateCommitIndex(majorityIndex, curTerm, isLeader)) {
      getStateMachineUpdater().notifyUpdater();
      return true;
    }
    return false;
  }

  void notifyStateMachineUpdater() {
    getStateMachineUpdater().notifyUpdater();
  }

  void reloadStateMachine(TermIndex snapshotTermIndex) {
    getStateMachineUpdater().reloadStateMachine();

    getLog().onSnapshotInstalled(snapshotTermIndex.getIndex());
    latestInstalledSnapshot.set(snapshotTermIndex);
  }

  @Override
  public void close() throws IOException {
    try {
      getStateMachineUpdater().stopAndJoin();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("{}: Interrupted when joining stateMachineUpdater", getMemberId(), e);
    }
    LOG.info("{}: closes. applyIndex: {}", getMemberId(), getLastAppliedIndex());

    getLog().close();
    storage.close();
  }

  RaftStorage getStorage() {
    return storage;
  }

  void installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    // TODO: verify that we need to install the snapshot
    StateMachine sm = server.getStateMachine();
    sm.pause(); // pause the SM to prepare for install snapshot
    snapshotManager.installSnapshot(request, sm);
  }

  private SnapshotInfo getLatestSnapshot() {
    return server.getStateMachine().getLatestSnapshot();
  }

  long getLatestInstalledSnapshotIndex() {
    final TermIndex ti = latestInstalledSnapshot.get();
    return ti != null? ti.getIndex(): RaftLog.INVALID_LOG_INDEX;
  }

  /**
   * The last index included in either the latestSnapshot or the latestInstalledSnapshot
   * @return the last snapshot index
   */
  long getSnapshotIndex() {
    final SnapshotInfo s = getLatestSnapshot();
    final long latestSnapshotIndex = s != null ? s.getIndex() : RaftLog.INVALID_LOG_INDEX;
    return Math.max(latestSnapshotIndex, getLatestInstalledSnapshotIndex());
  }

  long getNextIndex() {
    final long logNextIndex = getLog().getNextIndex();
    final long snapshotNextIndex = getLog().getSnapshotIndex() + 1;
    return Math.max(logNextIndex, snapshotNextIndex);
  }

  long getLastAppliedIndex() {
    return getStateMachineUpdater().getStateMachineLastAppliedIndex();
  }

  boolean containsTermIndex(TermIndex ti) {
    Objects.requireNonNull(ti, "ti == null");

    if (Optional.ofNullable(latestInstalledSnapshot.get()).filter(ti::equals).isPresent()) {
      return true;
    }
    if (Optional.ofNullable(getLatestSnapshot()).map(SnapshotInfo::getTermIndex).filter(ti::equals).isPresent()) {
      return true;
    }
    return getLog().contains(ti);
  }

  ReadRequests getReadRequests() {
    return readRequests;
  }
}
