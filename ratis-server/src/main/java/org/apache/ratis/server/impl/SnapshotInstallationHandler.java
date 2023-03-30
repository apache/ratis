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
import org.apache.ratis.proto.RaftProtos.FollowerInfoProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.ServerRpcProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ratis.server.raftlog.RaftLog.INVALID_LOG_INDEX;

class SnapshotInstallationHandler {
  static final Logger LOG = LoggerFactory.getLogger(SnapshotInstallationHandler.class);

  static final TermIndex INVALID_TERM_INDEX = TermIndex.valueOf(0, INVALID_LOG_INDEX);

  private final RaftServerImpl server;
  private final ServerState state;

  private final boolean installSnapshotEnabled;
  private final AtomicLong inProgressInstallSnapshotIndex = new AtomicLong(INVALID_LOG_INDEX);
  private final AtomicReference<TermIndex> installedSnapshotTermIndex =
    new AtomicReference<>(INVALID_TERM_INDEX);
  private final AtomicBoolean isSnapshotNull = new AtomicBoolean();
  private final AtomicLong installedIndex = new AtomicLong(INVALID_LOG_INDEX);

  SnapshotInstallationHandler(RaftServerImpl server, RaftProperties properties) {
    this.server = server;
    this.state = server.getState();
    this.installSnapshotEnabled = RaftServerConfigKeys.Log.Appender.installSnapshotEnabled(properties);
  }

  RaftGroupMemberId getMemberId() {
    return state.getMemberId();
  }

  long getInstalledIndex() {
    return installedIndex.getAndSet(INVALID_LOG_INDEX);
  }

  long getInProgressInstallSnapshotIndex() {
    return inProgressInstallSnapshotIndex.get();
  }

  InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("{}: receive installSnapshot: {}", getMemberId(),
          ServerStringUtils.toInstallSnapshotRequestString(request));
    }
    final InstallSnapshotReplyProto reply;
    try {
      reply = installSnapshotImpl(request);
    } catch (Exception e) {
      LOG.error("{}: installSnapshot failed", getMemberId(), e);
      throw e;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("{}: reply installSnapshot: {}", getMemberId(),
          ServerStringUtils.toInstallSnapshotReplyString(reply));
    }
    return reply;
  }

  private InstallSnapshotReplyProto installSnapshotImpl(InstallSnapshotRequestProto request) throws IOException {
    final RaftRpcRequestProto r = request.getServerRequest();
    final RaftPeerId leaderId = RaftPeerId.valueOf(r.getRequestorId());
    final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(r.getRaftGroupId());
    CodeInjectionForTesting.execute(RaftServerImpl.INSTALL_SNAPSHOT, server.getId(), leaderId, request);

    server.assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
    server.assertGroup(leaderId, leaderGroupId);

    InstallSnapshotReplyProto reply = null;
    // Check if install snapshot from Leader is enabled
    if (installSnapshotEnabled) {
      // Leader has sent InstallSnapshot request with SnapshotInfo. Install the snapshot.
      if (request.hasSnapshotChunk()) {
        reply = checkAndInstallSnapshot(request, leaderId);
      }
    } else {
      // Leader has only sent a notification to install snapshot. Inform State Machine to install snapshot.
      if (request.hasNotification()) {
        reply = notifyStateMachineToInstallSnapshot(request, leaderId);
      }
    }

    if (reply != null) {
      if (request.hasLastRaftConfigurationLogEntryProto()) {
        // Set the configuration included in the snapshot
        final LogEntryProto proto = request.getLastRaftConfigurationLogEntryProto();
        if (!state.getRaftConf().equals(LogProtoUtils.toRaftConfiguration(proto))) {
          LOG.info("{}: set new configuration {} from snapshot", getMemberId(), proto);
          state.setRaftConf(proto);
          state.writeRaftConfiguration(proto);
          server.getStateMachine().event().notifyConfigurationChanged(
              proto.getTerm(), proto.getIndex(), proto.getConfigurationEntry());
        }
      }
      return reply;
    }

    // There is a mismatch between configurations on leader and follower.
    final InstallSnapshotReplyProto failedReply = ServerProtoUtils.toInstallSnapshotReplyProto(
        leaderId, getMemberId(), InstallSnapshotResult.CONF_MISMATCH);
    LOG.error("{}: Configuration Mismatch ({}): Leader {} has it set to {} but follower {} has it set to {}",
        getMemberId(), RaftServerConfigKeys.Log.Appender.INSTALL_SNAPSHOT_ENABLED_KEY,
        leaderId, request.hasSnapshotChunk(), server.getId(), installSnapshotEnabled);
    return failedReply;
  }

  private InstallSnapshotReplyProto checkAndInstallSnapshot(InstallSnapshotRequestProto request,
      RaftPeerId leaderId) throws IOException {
    final long currentTerm;
    final long leaderTerm = request.getLeaderTerm();
    final InstallSnapshotRequestProto.SnapshotChunkProto snapshotChunkRequest = request.getSnapshotChunk();
    final TermIndex lastIncluded = TermIndex.valueOf(snapshotChunkRequest.getTermIndex());
    final long lastIncludedIndex = lastIncluded.getIndex();
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final InstallSnapshotReplyProto reply = ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
            currentTerm, snapshotChunkRequest.getRequestIndex(), InstallSnapshotResult.NOT_LEADER);
        LOG.warn("{}: Failed to recognize leader for installSnapshot chunk.", getMemberId());
        return reply;
      }
      server.changeToFollowerAndPersistMetadata(leaderTerm, true, "installSnapshot");
      state.setLeader(leaderId, "installSnapshot");

      server.updateLastRpcTime(FollowerState.UpdateType.INSTALL_SNAPSHOT_START);
      try {
        // Check and append the snapshot chunk. We simply put this in lock
        // considering a follower peer requiring a snapshot installation does not
        // have a lot of requests
        Preconditions.assertTrue(state.getLog().getNextIndex() <= lastIncludedIndex,
            "%s log's next id is %s, last included index in snapshot is %s",
            getMemberId(), state.getLog().getNextIndex(), lastIncludedIndex);

        //TODO: We should only update State with installed snapshot once the request is done.
        state.installSnapshot(request);

        // update the committed index
        // re-load the state machine if this is the last chunk
        if (snapshotChunkRequest.getDone()) {
          state.reloadStateMachine(lastIncluded);
        }
      } finally {
        server.updateLastRpcTime(FollowerState.UpdateType.INSTALL_SNAPSHOT_COMPLETE);
      }
    }
    if (snapshotChunkRequest.getDone()) {
      LOG.info("{}: successfully install the entire snapshot-{}", getMemberId(), lastIncludedIndex);
    }
    return ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
        currentTerm, snapshotChunkRequest.getRequestIndex(), InstallSnapshotResult.SUCCESS);
  }

  private InstallSnapshotReplyProto notifyStateMachineToInstallSnapshot(
      InstallSnapshotRequestProto request, RaftPeerId leaderId) throws IOException {
    final long currentTerm;
    final long leaderTerm = request.getLeaderTerm();
    final TermIndex firstAvailableLogTermIndex = TermIndex.valueOf(
        request.getNotification().getFirstAvailableTermIndex());
    final long firstAvailableLogIndex = firstAvailableLogTermIndex.getIndex();
    synchronized (this) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        final InstallSnapshotReplyProto reply = ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
            currentTerm, InstallSnapshotResult.NOT_LEADER, -1);
        LOG.warn("{}: Failed to recognize leader for installSnapshot notification.", getMemberId());
        return reply;
      }
      server.changeToFollowerAndPersistMetadata(leaderTerm, true, "installSnapshot");
      state.setLeader(leaderId, "installSnapshot");
      server.updateLastRpcTime(FollowerState.UpdateType.INSTALL_SNAPSHOT_NOTIFICATION);

      if (inProgressInstallSnapshotIndex.compareAndSet(INVALID_LOG_INDEX, firstAvailableLogIndex)) {
        LOG.info("{}: Received notification to install snapshot at index {}", getMemberId(), firstAvailableLogIndex);
        // Check if snapshot index is already at par or ahead of the first
        // available log index of the Leader.
        final long snapshotIndex = state.getLog().getSnapshotIndex();
        if (snapshotIndex + 1 >= firstAvailableLogIndex && firstAvailableLogIndex > INVALID_LOG_INDEX) {
          // State Machine has already installed the snapshot. Return the
          // latest snapshot index to the Leader.

          inProgressInstallSnapshotIndex.compareAndSet(firstAvailableLogIndex, INVALID_LOG_INDEX);
          LOG.info("{}: InstallSnapshot notification result: {}, current snapshot index: {}", getMemberId(),
              InstallSnapshotResult.ALREADY_INSTALLED, snapshotIndex);
          return ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(), currentTerm,
              InstallSnapshotResult.ALREADY_INSTALLED, snapshotIndex);
        }

        final RaftPeerProto leaderProto;
        if (!request.hasLastRaftConfigurationLogEntryProto()) {
          leaderProto = null;
        } else {
          leaderProto = request.getLastRaftConfigurationLogEntryProto().getConfigurationEntry().getPeersList()
              .stream()
              .filter(p -> RaftPeerId.valueOf(p.getId()).equals(leaderId))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("Leader " + leaderId
                  + " not found from the last configuration LogEntryProto, request = " + request));
        }

        // For the cases where RaftConf is empty on newly started peer with empty peer list,
        // we retrieve leader info from installSnapShotRequestProto.
        final RoleInfoProto proto = leaderProto == null || server.getRaftConf().getPeer(state.getLeaderId()) != null?
            server.getRoleInfoProto(): getRoleInfoProto(ProtoUtils.toRaftPeer(leaderProto));
        // This is the first installSnapshot notify request for this term and
        // index. Notify the state machine to install the snapshot.
        LOG.info("{}: notifyInstallSnapshot: nextIndex is {} but the leader's first available index is {}.",
            getMemberId(), state.getLog().getNextIndex(), firstAvailableLogIndex);
        // If notifyInstallSnapshotFromLeader future is done asynchronously, the main thread will go through the
        // downside part. As the time consumed by user-defined statemachine is uncontrollable(e.g. the RocksDB
        // checkpoint could be constantly increasing, the transmission will always exceed one boundary), we expect that
        // once snapshot installed, follower could work ASAP. For the rest of time, server can respond snapshot
        // installation in progress.

        // There is another appendLog thread appending raft entries, which returns inconsistency entries with
        // nextIndex and commitIndex to the leader when install snapshot in progress. The nextIndex on follower side
        // is updated when state.reloadStateMachine. We shall keep this index upgraded synchronously with main thread,
        // otherwise leader could get this follower's latest nextIndex from appendEntries instead of after
        // acknowledging the SNAPSHOT_INSTALLED.
        server.getStateMachine().followerEvent().notifyInstallSnapshotFromLeader(proto, firstAvailableLogTermIndex)
            .whenComplete((reply, exception) -> {
              if (exception != null) {
                LOG.error("{}: Failed to notify StateMachine to InstallSnapshot. Exception: {}",
                    getMemberId(), exception.getMessage());
                inProgressInstallSnapshotIndex.compareAndSet(firstAvailableLogIndex, INVALID_LOG_INDEX);
                return;
              }

              if (reply != null) {
                LOG.info("{}: StateMachine successfully installed snapshot index {}. Reloading the StateMachine.",
                    getMemberId(), reply.getIndex());
                installedSnapshotTermIndex.set(reply);
              } else {
                isSnapshotNull.set(true);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("{}: StateMachine could not install snapshot as it is not available", this);
                }
              }
            });

        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: StateMachine is processing Snapshot Installation Request.", getMemberId());
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: StateMachine is already installing a snapshot.", getMemberId());
        }
      }

      final long inProgressInstallSnapshotIndexValue = getInProgressInstallSnapshotIndex();
      Preconditions.assertTrue(inProgressInstallSnapshotIndexValue <= firstAvailableLogIndex
              && inProgressInstallSnapshotIndexValue > INVALID_LOG_INDEX,
          "inProgressInstallSnapshotRequest: %s is not eligible, firstAvailableLogIndex: %s",
          getInProgressInstallSnapshotIndex(), firstAvailableLogIndex);

      // If the snapshot is null or unavailable, return SNAPSHOT_UNAVAILABLE.
      if (isSnapshotNull.compareAndSet(true, false)) {
        LOG.info("{}: InstallSnapshot notification result: {}", getMemberId(),
            InstallSnapshotResult.SNAPSHOT_UNAVAILABLE);
        inProgressInstallSnapshotIndex.set(INVALID_LOG_INDEX);
        server.getStateMachine().event().notifySnapshotInstalled(
            InstallSnapshotResult.SNAPSHOT_UNAVAILABLE, INVALID_LOG_INDEX, server.getPeer());
        return ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
            currentTerm, InstallSnapshotResult.SNAPSHOT_UNAVAILABLE, -1);
      }

      // If a snapshot has been installed, return SNAPSHOT_INSTALLED with the installed snapshot index and reset
      // installedSnapshotIndex to (0,-1).
      final TermIndex latestInstalledSnapshotTermIndex = this.installedSnapshotTermIndex
          .getAndSet(INVALID_TERM_INDEX);
      if (latestInstalledSnapshotTermIndex.getIndex() > INVALID_LOG_INDEX) {
        server.getStateMachine().pause();
        state.reloadStateMachine(latestInstalledSnapshotTermIndex);
        LOG.info("{}: InstallSnapshot notification result: {}, at index: {}", getMemberId(),
            InstallSnapshotResult.SNAPSHOT_INSTALLED, latestInstalledSnapshotTermIndex);
        inProgressInstallSnapshotIndex.set(INVALID_LOG_INDEX);
        final long latestInstalledIndex = latestInstalledSnapshotTermIndex.getIndex();
        server.getStateMachine().event().notifySnapshotInstalled(
            InstallSnapshotResult.SNAPSHOT_INSTALLED, latestInstalledIndex, server.getPeer());
        installedIndex.set(latestInstalledIndex);
        return ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
            currentTerm, InstallSnapshotResult.SNAPSHOT_INSTALLED, latestInstalledSnapshotTermIndex.getIndex());
      }

      // Otherwise, Snapshot installation is in progress.
      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: InstallSnapshot notification result: {}", getMemberId(),
            InstallSnapshotResult.IN_PROGRESS);
      }
      return ServerProtoUtils.toInstallSnapshotReplyProto(leaderId, getMemberId(),
          currentTerm, InstallSnapshotResult.IN_PROGRESS, -1);
    }
  }

  private RoleInfoProto getRoleInfoProto(RaftPeer leader) {
    final RoleInfo role = server.getRole();
    final Optional<FollowerState> fs = role.getFollowerState();
    final ServerRpcProto leaderInfo = ServerProtoUtils.toServerRpcProto(leader,
        fs.map(FollowerState::getLastRpcTime).map(Timestamp::elapsedTimeMs).orElse(0L));
    final FollowerInfoProto.Builder followerInfo = FollowerInfoProto.newBuilder()
        .setLeaderInfo(leaderInfo)
        .setOutstandingOp(fs.map(FollowerState::getOutstandingOp).orElse(0));
    return RoleInfoProto.newBuilder()
        .setSelf(server.getPeer().getRaftPeerProto())
        .setRole(role.getCurrentRole())
        .setRoleElapsedTimeMs(role.getRoleElapsedTimeMs())
        .setFollowerInfo(followerInfo)
        .build();
  }
}