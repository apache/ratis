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

import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionReplyProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.LeaderElection.Phase;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ratis.server.impl.ServerImplUtils.assertGroup;
import static org.apache.ratis.server.impl.ServerProtoUtils.toRequestVoteReplyProto;
import static org.apache.ratis.server.impl.ServerProtoUtils.toStartLeaderElectionReplyProto;
import static org.apache.ratis.server.util.ServerStringUtils.toRequestVoteReplyString;

/** Handle requestVote and startLeaderElection requests for a {@link RaftServerImpl}. */
class ElectionRequestHandler {
  static final Logger LOG = LoggerFactory.getLogger(ElectionRequestHandler.class);

  private final RaftServerImpl server;
  private final ServerState state;

  ElectionRequestHandler(RaftServerImpl server) {
    this.server = server;
    this.state = server.getState();
  }

  RaftGroupMemberId getMemberId() {
    return state.getMemberId();
  }

  /**
   * The remote peer should shut down if all the following are true.
   * 1. this is the current leader
   * 2. current conf is stable and has been committed
   * 3. candidate is not in the current conf
   * 4. candidate last entry index < conf index (the candidate was removed)
   */
  private boolean shouldSendShutdown(RaftPeerId candidateId, TermIndex candidateLastEntry) {
    return server.getInfo().isLeader()
        && server.getRaftConf().isStable()
        && state.isConfCommitted()
        && !server.getRaftConf().containsInConf(candidateId)
        && candidateLastEntry.getIndex() < server.getRaftConf().getLogEntryIndex()
        && server.getRole().getLeaderState().map(ls -> !ls.isBootStrappingPeer(candidateId)).orElse(false);
  }

  RequestVoteReplyProto requestVote(RequestVoteRequestProto r) throws IOException {
    final RaftRpcRequestProto request = r.getServerRequest();
    return requestVote(r.getPreVote() ? Phase.PRE_VOTE : Phase.ELECTION,
        RaftPeerId.valueOf(request.getRequestorId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        r.getCandidateTerm(),
        TermIndex.valueOf(r.getCandidateLastEntry()),
        request.getCallId());
  }

  private RequestVoteReplyProto requestVote(Phase phase,
      RaftPeerId candidateId, RaftGroupId candidateGroupId,
      long candidateTerm, TermIndex candidateLastEntry, long callId) throws IOException {
    CodeInjectionForTesting.execute(RaftServerImpl.REQUEST_VOTE, server.getId(),
        candidateId, candidateTerm, candidateLastEntry);
    LOG.info("{}: receive requestVote({}, {}, {}, {}, {})",
        getMemberId(), phase, candidateId, candidateGroupId, candidateTerm, candidateLastEntry);
    server.assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), candidateId, candidateGroupId);

    boolean shouldShutdown = false;
    final RequestVoteReplyProto reply;
    CompletableFuture<Void> future = null;
    synchronized (server) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      server.assertLifeCycleState(LifeCycle.States.RUNNING);

      final VoteContext context = new VoteContext(server, phase, candidateId);
      final RaftPeer candidate = context.recognizeCandidate(candidateTerm);
      final boolean voteGranted = context.decideVote(candidate, candidateLastEntry);
      if (candidate != null && phase == Phase.ELECTION) {
        // change server state in the ELECTION phase
        final AtomicBoolean termUpdated = new AtomicBoolean();
        future = server.changeToFollower(candidateTerm, true, false, "candidate:" + candidateId, termUpdated);
        if (voteGranted) {
          state.grantVote(candidate.getId());
        }
        if (termUpdated.get() || voteGranted) {
          state.persistMetadata(); // sync metafile
        }
      }
      if (voteGranted) {
        server.getRole().getFollowerState()
            .ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.REQUEST_VOTE));
      } else if(shouldSendShutdown(candidateId, candidateLastEntry)) {
        shouldShutdown = true;
      }
      reply = toRequestVoteReplyProto(candidateId, getMemberId(),
          voteGranted, state.getCurrentTerm(), shouldShutdown, state.getLastEntry(), callId);
      if (LOG.isInfoEnabled()) {
        LOG.info("{} replies to {} vote request: {}. Peer's state: {}",
            getMemberId(), phase, toRequestVoteReplyString(reply), state);
      }
    }
    if (future != null) {
      future.join();
    }
    return reply;
  }

  StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
    final RaftRpcRequestProto r = request.getServerRequest();
    final RaftPeerId leaderId = RaftPeerId.valueOf(r.getRequestorId());
    final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(r.getRaftGroupId());
    CodeInjectionForTesting.execute(RaftServerImpl.START_LEADER_ELECTION, server.getId(), leaderId, request);

    if (!request.hasLeaderLastEntry()) {
      // It should have a leaderLastEntry since there is a placeHolder entry.
      LOG.warn("{}: leaderLastEntry is missing in {}", getMemberId(), request);
      return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
    }

    final TermIndex leaderLastEntry = TermIndex.valueOf(request.getLeaderLastEntry());
    LOG.debug("{}: receive startLeaderElection from {} with lastEntry {}", getMemberId(), leaderId, leaderLastEntry);

    server.assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), leaderId, leaderGroupId);

    synchronized (server) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      server.assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      final boolean recognized = state.recognizeLeader("startLeaderElection", leaderId, leaderLastEntry.getTerm());
      if (!recognized) {
        return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      if (!server.getInfo().isFollower()) {
        LOG.warn("{} refused StartLeaderElectionRequest from {}, because role is:{}",
            getMemberId(), leaderId, server.getRole().getCurrentRole());
        return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      if (ServerState.compareLog(state.getLastEntry(), leaderLastEntry) < 0) {
        LOG.warn("{} refused StartLeaderElectionRequest from {}, because lastEntry:{} less than leaderEntry:{}",
            getMemberId(), leaderId, leaderLastEntry, state.getLastEntry());
        return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      server.changeToCandidate(true);
      return toStartLeaderElectionReplyProto(leaderId, getMemberId(), true);
    }
  }
}
