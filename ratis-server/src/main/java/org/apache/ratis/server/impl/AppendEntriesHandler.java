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
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.ServerImplUtils.NavigableIndices;
import org.apache.ratis.server.protocol.RaftServerProtocol.Op;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.trace.TraceServer;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.ratis.server.impl.ServerImplUtils.assertEntries;
import static org.apache.ratis.server.impl.ServerImplUtils.assertGroup;
import static org.apache.ratis.server.impl.ServerImplUtils.effectiveCommitIndex;
import static org.apache.ratis.server.impl.ServerProtoUtils.toAppendEntriesReplyProto;
import static org.apache.ratis.server.raftlog.LogProtoUtils.toLogEntryTermIndexString;
import static org.apache.ratis.server.util.ServerStringUtils.toAppendEntriesReplyString;
import static org.apache.ratis.server.util.ServerStringUtils.toAppendEntriesRequestString;

/** Handle appendEntries requests for a {@link RaftServerImpl}. */
class AppendEntriesHandler {
  static final Logger LOG = LoggerFactory.getLogger(AppendEntriesHandler.class);

  private final RaftServerImpl server;
  private final ServerState state;
  private final StateMachine stateMachine;
  private final NavigableIndices appendLogTermIndices;

  AppendEntriesHandler(RaftServerImpl server, RaftProperties properties) {
    this.server = server;
    this.state = server.getState();
    this.stateMachine = server.getStateMachine();
    this.appendLogTermIndices = RaftServerConfigKeys.Log.appendEntriesComposeEnabled(properties) ?
        new NavigableIndices() : null;
  }

  RaftGroupMemberId getMemberId() {
    return state.getMemberId();
  }

  static void logAppendEntries(boolean isHeartbeat, Supplier<String> message) {
    if (isHeartbeat) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("HEARTBEAT: {}", message.get());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(message.get());
      }
    }
  }

  CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(AppendEntriesRequestProto r) throws IOException {
    final RaftRpcRequestProto request = r.getServerRequest();
    final TermIndex previous = r.hasPreviousLog()? TermIndex.valueOf(r.getPreviousLog()) : null;
    try {
      final RaftPeerId leaderId = RaftPeerId.valueOf(request.getRequestorId());
      final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(request.getRaftGroupId());
      CodeInjectionForTesting.execute(RaftServerImpl.APPEND_ENTRIES, server.getId(), leaderId, previous, r);

      server.assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      assertGroup(getMemberId(), leaderId, leaderGroupId);
      assertEntries(r, previous, state);
      return TraceServer.traceAppendEntriesAsync(() -> appendEntriesAsync(leaderId, request.getCallId(), previous, r),
          r, getMemberId().toString());
    } catch(Exception t) {
      LOG.error("{}: Failed appendEntries* {}", getMemberId(),
          toAppendEntriesRequestString(r, stateMachine::toStateMachineLogEntryString), t);
      throw IOUtils.asIOException(t);
    }
  }

  private CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(RaftPeerId leaderId, long callId,
      TermIndex previous, AppendEntriesRequestProto proto) throws IOException {
    final List<LogEntryProto> entries = proto.getEntriesList();
    final boolean isHeartbeat = entries.isEmpty();
    logAppendEntries(isHeartbeat, () -> getMemberId() + ": appendEntries* "
        + toAppendEntriesRequestString(proto, stateMachine::toStateMachineLogEntryString));

    final long leaderTerm = proto.getLeaderTerm();
    final long currentTerm;
    final long followerCommit = state.getLog().getLastCommittedIndex();
    final long appliedIndex = state.getLastAppliedIndex();
    final Optional<FollowerState> followerState;
    final Timekeeper.Context timer = server.getRaftServerMetrics().getFollowerAppendEntryTimer(isHeartbeat).time();
    final CompletableFuture<Void> future;
    synchronized (server) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      server.assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      currentTerm = state.getCurrentTerm();
      final boolean recognized = state.recognizeLeader(Op.APPEND_ENTRIES, leaderId, leaderTerm);
      if (!recognized) {
        return CompletableFuture.completedFuture(toAppendEntriesReplyProto(
            leaderId, getMemberId(), currentTerm, followerCommit, state.getNextIndex(),
            AppendResult.NOT_LEADER, callId, RaftLog.INVALID_LOG_INDEX, isHeartbeat, appliedIndex));
      }
      try {
        future = server.changeToFollowerAndPersistMetadata(leaderTerm, true, Op.APPEND_ENTRIES);
      } catch (IOException e) {
        return JavaUtils.completeExceptionally(e);
      }
      state.setLeader(leaderId, Op.APPEND_ENTRIES);

      server.startFollowerState(proto.getInitializing());
      followerState = server.updateLastRpcTime(FollowerState.UpdateType.APPEND_START);

      // Check that the append entries are not inconsistent. There are 3
      // scenarios which can result in inconsistency:
      //      1. There is a snapshot installation in progress
      //      2. There is an overlap between the snapshot index and the entries
      //      3. There is a gap between the local log and the entries
      // In any of these scenarios, we should return an INCONSISTENCY reply
      // back to leader so that the leader can update this follower's next index.
      final long inconsistencyReplyNextIndex = checkInconsistentAppendEntries(previous, entries);
      if (inconsistencyReplyNextIndex > RaftLog.INVALID_LOG_INDEX) {
        final AppendEntriesReplyProto reply = toAppendEntriesReplyProto(
            leaderId, getMemberId(), currentTerm, followerCommit, inconsistencyReplyNextIndex,
            AppendResult.INCONSISTENCY, callId, RaftLog.INVALID_LOG_INDEX, isHeartbeat, appliedIndex);
        LOG.info("{}: appendEntries* reply {}", getMemberId(), toAppendEntriesReplyString(reply));
        followerState.ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.APPEND_COMPLETE));
        return future.thenApply(dummy -> reply);
      }

      state.updateConfiguration(entries);
    }
    future.join();
    final CompletableFuture<Void> appendFuture = entries.isEmpty()? CompletableFuture.completedFuture(null)
        : appendLogTermIndices != null ? appendLogTermIndices.append(entries, this::appendLog)
        : JavaUtils.allOf(state.getLog().append(entries));

    server.updateCommitInfoCache(proto.getCommitInfosList());

    CodeInjectionForTesting.execute(RaftServerImpl.LOG_SYNC, server.getId(), null);
    if (!isHeartbeat) {
      final long installedIndex = server.getSnapshotInstallationHandler().getInstalledIndex();
      if (installedIndex >= RaftLog.LEAST_VALID_LOG_INDEX) {
        LOG.info("{}: Follower has completed install the snapshot {}.", server, installedIndex);
        stateMachine.event().notifySnapshotInstalled(InstallSnapshotResult.SUCCESS, installedIndex, server.getPeer());
      }
    }

    final long commitIndex = effectiveCommitIndex(proto.getLeaderCommit(), previous, entries.size());
    final long matchIndex = isHeartbeat? RaftLog.INVALID_LOG_INDEX: entries.get(entries.size() - 1).getIndex();
    return appendFuture.whenCompleteAsync((r, t) -> {
      if  (t != null) {
        LOG.warn("{}: appendEntries* failed: {}", getMemberId(), toLogEntryTermIndexString(entries), t);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("{}: appendEntries* succeeded: {}", getMemberId(), toLogEntryTermIndexString(entries));
      }
      followerState.ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.APPEND_COMPLETE));
      timer.stop();
    }, server.getServerExecutor()).thenApply(v -> {
      // Re-check the term after the asynchronous append completes: this server may have
      // granted a higher-term vote while the append was in flight. In that case this server
      // no longer recognizes the requestor as leader, so it must not report SUCCESS, which
      // the old leader could otherwise count toward committing an entry that the new leader
      // may not have. Reply NOT_LEADER with the current term so the old leader steps down.
      final long replyTerm = state.getCurrentTerm();
      if (replyTerm != leaderTerm) {
        final AppendEntriesReplyProto reply = toAppendEntriesReplyProto(leaderId, getMemberId(),
            replyTerm, followerCommit, state.getNextIndex(),
            AppendResult.NOT_LEADER, callId, RaftLog.INVALID_LOG_INDEX, isHeartbeat, appliedIndex);
        LOG.info("{}: appendEntries* reply {} since term changed from {} to {}",
            getMemberId(), toAppendEntriesReplyString(reply), leaderTerm, replyTerm);
        return reply;
      }
      final boolean updated = state.updateCommitIndex(commitIndex, currentTerm, false);
      if (updated) {
        server.updateCommitInfoCache();
      }
      final long nextIndex = isHeartbeat? state.getNextIndex(): matchIndex + 1;
      final AppendEntriesReplyProto reply = toAppendEntriesReplyProto(leaderId, getMemberId(),
          currentTerm, updated? commitIndex : state.getLog().getLastCommittedIndex(),
          nextIndex, AppendResult.SUCCESS, callId, matchIndex, isHeartbeat, appliedIndex);
      logAppendEntries(isHeartbeat, () -> getMemberId()
          + ": appendEntries* reply " + toAppendEntriesReplyString(reply));
      return reply;
    });
  }

  private CompletableFuture<Void> appendLog(List<LogEntryProto> entries) {
    return CompletableFuture.completedFuture(null)
        .thenComposeAsync(dummy -> {
          CodeInjectionForTesting.execute(RaftServerImpl.APPEND_ENTRIES_BEFORE_APPEND_LOG,
              server.getId(), null, entries);
          return JavaUtils.allOf(state.getLog().append(entries));
        }, server.getServerExecutor());
  }

  private long checkInconsistentAppendEntries(TermIndex previous, List<LogEntryProto> entries) {
    // Check if a snapshot installation through state machine is in progress.
    final long installSnapshot = server.getSnapshotInstallationHandler().getInProgressInstallSnapshotIndex();
    if (installSnapshot != RaftLog.INVALID_LOG_INDEX) {
      LOG.info("{}: Failed appendEntries as snapshot ({}) installation is in progress", getMemberId(), installSnapshot);
      return state.getNextIndex();
    }

    // Check that the first log entry is greater than the snapshot index in the latest snapshot and follower's last
    // committed index. If not, reply to the leader the new next index.
    if (entries != null && !entries.isEmpty()) {
      final long firstEntryIndex = entries.get(0).getIndex();
      final long snapshotIndex = state.getSnapshotIndex();
      final long commitIndex =  state.getLog().getLastCommittedIndex();
      final long nextIndex = Math.max(snapshotIndex, commitIndex);
      if (nextIndex > RaftLog.INVALID_LOG_INDEX && nextIndex >= firstEntryIndex) {
        LOG.info("{}: Failed appendEntries as the first entry (index {})" +
                " already exists (snapshotIndex: {}, commitIndex: {})",
            getMemberId(), firstEntryIndex, snapshotIndex, commitIndex);
        return nextIndex + 1;
      }
    }

    // Check if "previous" is contained in current state.
    if (previous != null
        && !(appendLogTermIndices != null && appendLogTermIndices.contains(previous))
        && !state.containsTermIndex(previous)) {
      final long replyNextIndex = Math.min(state.getNextIndex(), previous.getIndex());
      LOG.info("{}: Failed appendEntries, previous log entry {} not found", getMemberId(), previous);
      return replyNextIndex;
    }

    return RaftLog.INVALID_LOG_INDEX;
  }
}
