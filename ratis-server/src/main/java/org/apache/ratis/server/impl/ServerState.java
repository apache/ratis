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
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.*;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.Timestamp;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.ratis.server.impl.RaftServerImpl.LOG;

/**
 * Common states of a raft peer. Protected by RaftServer's lock.
 */
public class ServerState implements Closeable {
  private final RaftPeerId selfId;
  private final RaftServerImpl server;
  /** Raft log */
  private final RaftLog log;
  /** Raft configuration */
  private final ConfigurationManager configurationManager;
  /** The thread that applies committed log entries to the state machine */
  private final StateMachineUpdater stateMachineUpdater;
  /** local storage for log and snapshot */
  private final RaftStorage storage;
  private final SnapshotManager snapshotManager;
  private volatile Timestamp lastNoLeaderTime;
  private final long leaderElectionTimeoutMs;

  /**
   * Latest term server has seen. initialized to 0 on first boot, increases
   * monotonically.
   */
  private volatile long currentTerm;
  /**
   * The server ID of the leader for this term. Null means either there is
   * no leader for this term yet or this server does not know who it is yet.
   */
  private volatile RaftPeerId leaderId;
  /**
   * Candidate that this peer granted vote for in current term (or null if none)
   */
  private volatile RaftPeerId votedFor;

  /**
   * Latest installed snapshot for this server. This maybe different than StateMachine's latest
   * snapshot. Once we successfully install a snapshot, the SM may not pick it up immediately.
   * Further, this will not get updated when SM does snapshots itself.
   */
  private volatile TermIndex latestInstalledSnapshot;

  ServerState(RaftPeerId id, RaftGroup group, RaftProperties prop,
              RaftServerImpl server, StateMachine stateMachine)
      throws IOException {
    this.selfId = id;
    this.server = server;
    RaftConfiguration initialConf = RaftConfiguration.newBuilder()
        .setConf(group.getPeers()).build();
    configurationManager = new ConfigurationManager(initialConf);
    LOG.info("{}: {}", id, configurationManager);

    // use full uuid string to create a subdirectory
    final File dir = chooseStorageDir(RaftServerConfigKeys.storageDirs(prop),
        group.getGroupId().getUuid().toString());
    storage = new RaftStorage(dir, RaftServerConstants.StartupOption.REGULAR);
    snapshotManager = new SnapshotManager(storage, id);

    long lastApplied = initStatemachine(stateMachine, group.getGroupId());

    // On start the leader is null, start the clock now
    leaderId = null;
    this.lastNoLeaderTime = new Timestamp();
    this.leaderElectionTimeoutMs =
        RaftServerConfigKeys.leaderElectionTimeout(prop).toInt(TimeUnit.MILLISECONDS);

    // we cannot apply log entries to the state machine in this step, since we
    // do not know whether the local log entries have been committed.
    log = initLog(id, prop, lastApplied, this::setRaftConf);

    RaftLog.Metadata metadata = log.loadMetadata();
    currentTerm = metadata.getTerm();
    votedFor = metadata.getVotedFor();

    stateMachineUpdater = new StateMachineUpdater(stateMachine, server, log,
         lastApplied, prop);
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
        .min(Comparator.comparing(Map.Entry::getValue))
        .map(Map.Entry::getKey)
        .map(v -> new File(v, targetSubDir))
        .orElseThrow(() -> new IOException("No storage directory found."));
  }

  private long initStatemachine(StateMachine sm, RaftGroupId groupId)
      throws IOException {
    sm.initialize(server.getProxy(), groupId, storage);
    storage.setStateMachineStorage(sm.getStateMachineStorage());
    SnapshotInfo snapshot = sm.getLatestSnapshot();

    if (snapshot == null || snapshot.getTermIndex().getIndex() < 0) {
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    // get the raft configuration from raft metafile
    RaftConfiguration raftConf = storage.readRaftConfiguration();
    if (raftConf != null) {
      setRaftConf(raftConf.getLogEntryIndex(), raftConf);
    }
    return snapshot.getIndex();
  }

  void writeRaftConfiguration(LogEntryProto conf) {
    storage.writeRaftConfiguration(conf);
  }

  void start() {
    stateMachineUpdater.start();
  }

  /**
   * note we do not apply log entries to the state machine here since we do not
   * know whether they have been committed.
   */
  private RaftLog initLog(RaftPeerId id, RaftProperties prop,
      long lastIndexInSnapshot, Consumer<LogEntryProto> logConsumer)
      throws IOException {
    final RaftLog log;
    if (RaftServerConfigKeys.Log.useMemory(prop)) {
      final int maxBufferSize =
          RaftServerConfigKeys.Log.Appender.bufferCapacity(prop).getSizeInt();
      log = new MemoryRaftLog(id, maxBufferSize);
    } else {
      log = new SegmentedRaftLog(id, server, this.storage,
          lastIndexInSnapshot, prop);
    }
    log.open(lastIndexInSnapshot, logConsumer);
    return log;
  }

  public RaftConfiguration getRaftConf() {
    return configurationManager.getCurrent();
  }

  public RaftPeerId getSelfId() {
    return this.selfId;
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  boolean updateCurrentTerm(long newTerm) {
    if (newTerm > currentTerm) {
      currentTerm = newTerm;
      votedFor = null;
      setLeader(null, "updateCurrentTerm");
      return true;
    }
    return false;
  }

  RaftPeerId getLeaderId() {
    return leaderId;
  }

  boolean hasLeader() {
    return leaderId != null;
  }

  /**
   * Become a candidate and start leader election
   */
  long initElection() {
    votedFor = selfId;
    setLeader(null, "initElection");
    return ++currentTerm;
  }

  void persistMetadata() throws IOException {
    this.log.writeMetadata(currentTerm, votedFor);
  }

  /**
   * Vote for a candidate and update the local state.
   */
  void grantVote(RaftPeerId candidateId) {
    votedFor = candidateId;
    setLeader(null, "grantVote");
  }

  void setLeader(RaftPeerId newLeaderId, String op) {
    if (!Objects.equals(leaderId, newLeaderId)) {
      String suffix;
      if (newLeaderId == null) {
        // reset the time stamp when a null leader is assigned
        lastNoLeaderTime = new Timestamp();
        suffix = "";
      } else {
        Timestamp previous = lastNoLeaderTime;
        lastNoLeaderTime = null;
        suffix = ", leader elected after " + previous.elapsedTimeMs() + "ms";
      }
      LOG.info("{}: change Leader from {} to {} at term {} for {}{}",
          selfId, leaderId, newLeaderId, getCurrentTerm(), op, suffix);
      leaderId = newLeaderId;
    }
  }

  boolean checkForExtendedNoLeader() {
    return getLastLeaderElapsedTimeMs() > leaderElectionTimeoutMs;
  }

  long getLastLeaderElapsedTimeMs() {
    final Timestamp t = lastNoLeaderTime;
    return t == null ? 0 : t.elapsedTimeMs();
  }

  void becomeLeader() {
    setLeader(selfId, "becomeLeader");
  }

  public RaftLog getLog() {
    return log;
  }

  long applyLog(TransactionContext operation) throws StateMachineException {
    return log.append(currentTerm, operation);
  }

  /**
   * Check if accept the leader selfId and term from the incoming AppendEntries rpc.
   * If accept, update the current state.
   * @return true if the check passes
   */
  boolean recognizeLeader(RaftPeerId leaderId, long leaderTerm) {
    if (leaderTerm < currentTerm) {
      return false;
    } else if (leaderTerm > currentTerm || this.leaderId == null) {
      // If the request indicates a term that is greater than the current term
      // or no leader has been set for the current term, make sure to update
      // leader and term later
      return true;
    }
    return this.leaderId.equals(leaderId);
  }

  /**
   * Check if the candidate's term is acceptable
   */
  boolean recognizeCandidate(RaftPeerId candidateId, long candidateTerm) {
    if (!getRaftConf().containsInConf(candidateId)) {
      return false;
    }
    if (candidateTerm > currentTerm) {
      return true;
    } else if (candidateTerm == currentTerm) {
      // has not voted yet or this is a retry
      return votedFor == null || votedFor.equals(candidateId);
    }
    return false;
  }

  boolean isLogUpToDate(TermIndex candidateLastEntry) {
    TermIndex local = log.getLastEntryTermIndex();
    // need to take into account snapshot
    SnapshotInfo snapshot = server.getStateMachine().getLatestSnapshot();
     if (local == null && snapshot == null) {
      return true;
    } else if (candidateLastEntry == null) {
      return false;
    }
    if (local == null || (snapshot != null && snapshot.getIndex() > local.getIndex())) {
      local = snapshot.getTermIndex();
    }
    return local.compareTo(candidateLastEntry) <= 0;
  }

  @Override
  public String toString() {
    return selfId + ":t" + currentTerm + ", leader=" + leaderId
        + ", voted=" + votedFor + ", raftlog=" + log + ", conf=" + getRaftConf();
  }

  boolean isConfCommitted() {
    return getLog().getLastCommittedIndex() >=
        getRaftConf().getLogEntryIndex();
  }

  void setRaftConf(LogEntryProto entry) {
    if (entry.hasConfigurationEntry()) {
      setRaftConf(entry.getIndex(), ServerProtoUtils.toRaftConfiguration(entry));
    }
  }

  void setRaftConf(long logIndex, RaftConfiguration conf) {
    configurationManager.addConfiguration(logIndex, conf);
    server.getServerRpc().addPeers(conf.getPeers());
    LOG.info("{}: set configuration {} at {}", getSelfId(), conf, logIndex);
    LOG.trace("{}: {}", getSelfId(), configurationManager);
  }

  void updateConfiguration(LogEntryProto[] entries) {
    if (entries != null && entries.length > 0) {
      configurationManager.removeConfigurations(entries[0].getIndex());
      Arrays.stream(entries).forEach(this::setRaftConf);
    }
  }

  boolean updateStatemachine(long majorityIndex, long currentTerm) {
    if (log.updateLastCommitted(majorityIndex, currentTerm)) {
      stateMachineUpdater.notifyUpdater();
      return true;
    }
    return false;
  }

  void reloadStateMachine(long lastIndexInSnapshot, long currentTerm) {
    log.updateLastCommitted(lastIndexInSnapshot, currentTerm);
    stateMachineUpdater.reloadStateMachine();
  }

  @Override
  public void close() throws IOException {
    try {
      stateMachineUpdater.stopAndJoin();
    } catch (InterruptedException e) {
      LOG.warn(getSelfId() +
          ": Interrupted when joining stateMachineUpdater", e);
    }
    LOG.info("{} closes. The last applied log index is {}",
        getSelfId(), getLastAppliedIndex());

    log.close();
    storage.close();
  }

  public RaftStorage getStorage() {
    return storage;
  }

  void installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    // TODO: verify that we need to install the snapshot
    StateMachine sm = server.getStateMachine();
    sm.pause(); // pause the SM to prepare for install snapshot
    snapshotManager.installSnapshot(sm, request);
    log.syncWithSnapshot(request.getTermIndex().getIndex());
    this.latestInstalledSnapshot = ServerProtoUtils.toTermIndex(
        request.getTermIndex());
  }

  SnapshotInfo getLatestSnapshot() {
    return server.getStateMachine().getStateMachineStorage().getLatestSnapshot();
  }

  public TermIndex getLatestInstalledSnapshot() {
    return latestInstalledSnapshot;
  }

  public long getLastAppliedIndex() {
    return stateMachineUpdater.getLastAppliedIndex();
  }
}
