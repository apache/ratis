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
import org.apache.ratis.shaded.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.ProtoUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

import static org.apache.ratis.server.impl.RaftServerImpl.LOG;
import static org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto.LogEntryBodyCase.CONFIGURATIONENTRY;

/**
 * Common states of a raft peer. Protected by RaftServer's lock.
 */
public class ServerState implements Closeable {
  private final RaftPeerId selfId;
  private final RaftPeer peer;
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

  /**
   * Latest term server has seen. initialized to 0 on first boot, increases
   * monotonically.
   */
  private long currentTerm;
  /**
   * The server ID of the leader for this term. Null means either there is
   * no leader for this term yet or this server does not know who it is yet.
   */
  private RaftPeerId leaderId;
  /**
   * Candidate that this peer granted vote for in current term (or null if none)
   */
  private RaftPeerId votedFor;

  /**
   * Latest installed snapshot for this server. This maybe different than StateMachine's latest
   * snapshot. Once we successfully install a snapshot, the SM may not pick it up immediately.
   * Further, this will not get updated when SM does snapshots itself.
   */
  private TermIndex latestInstalledSnapshot;

  ServerState(RaftPeer peer, RaftGroup group, RaftProperties prop,
              RaftServerImpl server, StateMachine stateMachine)
      throws IOException {
    this.selfId = peer.getId();
    this.peer = peer;
    this.server = server;
    RaftConfiguration initialConf = RaftConfiguration.newBuilder()
        .setConf(group.getPeers()).build();
    configurationManager = new ConfigurationManager(initialConf);

    final File dir = RaftServerConfigKeys.storageDir(prop);
    storage = new RaftStorage(new File(dir, group.getGroupId().toString()),
        RaftServerConstants.StartupOption.REGULAR);
    snapshotManager = new SnapshotManager(storage, peer.getId());

    long lastApplied = initStatemachine(stateMachine, prop);

    leaderId = null;
    // we cannot apply log entries to the state machine in this step, since we
    // do not know whether the local log entries have been committed.
    log = initLog(peer.getId(), prop, lastApplied, entry -> {
      if (entry.getLogEntryBodyCase() == CONFIGURATIONENTRY) {
        configurationManager.addConfiguration(entry.getIndex(),
            ServerProtoUtils.toRaftConfiguration(entry.getIndex(),
                entry.getConfigurationEntry()));
      }
    });

    RaftLog.Metadata metadata = log.loadMetadata();
    currentTerm = metadata.getTerm();
    votedFor = metadata.getVotedFor();

    stateMachineUpdater = new StateMachineUpdater(stateMachine, server, log,
         lastApplied, prop);
  }

  private long initStatemachine(StateMachine sm, RaftProperties properties)
      throws IOException {
    sm.initialize(selfId, properties, storage);
    storage.setStateMachineStorage(sm.getStateMachineStorage());
    SnapshotInfo snapshot = sm.getLatestSnapshot();

    if (snapshot == null || snapshot.getTermIndex().getIndex() < 0) {
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    // get the raft configuration from the snapshot
    RaftConfiguration raftConf = sm.getRaftConfiguration();
    if (raftConf != null) {
      configurationManager.addConfiguration(raftConf.getLogEntryIndex(),
          raftConf);
    }
    return snapshot.getIndex();
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

  CommitInfoProto updateCommitInfo(CommitInfoCache cache) {
    return cache.update(peer, log.getLastCommittedIndex());
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
      LOG.info("{}: change Leader from {} to {} at term {} for {}",
          selfId, leaderId, newLeaderId, getCurrentTerm(), op);
      leaderId = newLeaderId;
    }
  }

  void becomeLeader() {
    setLeader(selfId, "becomeLeader");
  }

  public RaftLog getLog() {
    return log;
  }

  long applyLog(TransactionContext operation, ClientId clientId, long callId)
      throws StateMachineException {
    return log.append(currentTerm, operation, clientId, callId);
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
  boolean recognizeCandidate(RaftPeerId candidateId,
      long candidateTerm) {
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

  public void setRaftConf(long logIndex, RaftConfiguration conf) {
    configurationManager.addConfiguration(logIndex, conf);
    LOG.info("{}: successfully update the configuration {}",
        getSelfId(), conf);
  }

  void updateConfiguration(LogEntryProto[] entries) {
    if (entries != null && entries.length > 0) {
      configurationManager.removeConfigurations(entries[0].getIndex());
      for (LogEntryProto entry : entries) {
        if (ProtoUtils.isConfigurationLogEntry(entry)) {
          final RaftConfiguration conf = ServerProtoUtils.toRaftConfiguration(
              entry.getIndex(), entry.getConfigurationEntry());
          configurationManager.addConfiguration(entry.getIndex(), conf);
          server.getServerRpc().addPeers(conf.getPeers());
        }
      }
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
    stateMachineUpdater.stop();
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