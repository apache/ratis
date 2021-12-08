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

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.impl.LeaderElection.Phase;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VoteContext {
  static final Logger LOG = LoggerFactory.getLogger(VoteContext.class);

  private final RaftServerImpl impl;
  private final RaftConfigurationImpl conf;
  private final Phase phase;
  private final RaftPeerId candidateId;

  VoteContext(RaftServerImpl impl, Phase phase, RaftPeerId candidateId) {
    this.impl = impl;
    this.conf = impl.getRaftConf();
    this.phase = phase;
    this.candidateId = candidateId;
  }

  private boolean reject(String reason) {
    return log(false, reason);
  }

  private boolean log(boolean accept, String reason) {
    LOG.info("{}-{}: {} {} from {}: {}",
        impl.getMemberId(), impl.getInfo().getCurrentRole(), accept? "accept": "reject", phase, candidateId, reason);
    return accept;
  }

  /** Check if the candidate is in the current conf. */
  private RaftPeer checkConf() {
    if (!conf.containsInConf(candidateId)) {
      reject(candidateId + " is not in current conf " + conf.getCurrentPeers());
      return null;
    }
    return conf.getPeer(candidateId);
  }

  enum CheckTermResult {
    FAILED, CHECK_LEADER, SKIP_CHECK_LEADER
  }

  /** Check the candidate term. */
  private CheckTermResult checkTerm(long candidateTerm) {
    if (phase == Phase.PRE_VOTE) {
      return CheckTermResult.CHECK_LEADER;
    }
    // check term
    final ServerState state = impl.getState();
    final long currentTerm = state.getCurrentTerm();
    if (currentTerm > candidateTerm) {
      reject("current term " + currentTerm + " > candidate's term " + candidateTerm);
      return CheckTermResult.FAILED;
    } else if (currentTerm == candidateTerm) {
      // check if this server has already voted
      final RaftPeerId votedFor = state.getVotedFor();
      if (votedFor != null && !votedFor.equals(candidateId)) {
        reject("already has voted for " + votedFor + " at current term " + currentTerm);
        return CheckTermResult.FAILED;
      }
      return CheckTermResult.CHECK_LEADER;
    } else {
      return CheckTermResult.SKIP_CHECK_LEADER; //currentTerm < candidateTerm
    }
  }

  /** Check if there is already a leader. */
  private boolean checkLeader() {
    // check if this server is the leader
    final DivisionInfo info = impl.getInfo();
    if (info.isLeader()) {
      if (impl.getRole().getLeaderState().map(LeaderStateImpl::checkLeadership).orElse(false)) {
        return reject("this server is the leader and still has leadership");
      }
    }

    // check if this server is a follower and has a valid leader
    if (info.isFollower()) {
      final RaftPeerId leader = impl.getState().getLeaderId();
      if (leader != null
          && impl.getRole().getFollowerState().map(FollowerState::isCurrentLeaderValid).orElse(false)) {
        return reject("this server is a follower and still has a valid leader " + leader);
      }
    }
    return true;
  }

  RaftPeer recognizeCandidate(long candidateTerm) {
    final RaftPeer candidate = checkConf();
    if (candidate == null) {
      return null;
    }
    final CheckTermResult r = checkTerm(candidateTerm);
    if (r == CheckTermResult.FAILED) {
      return null;
    }
    if (r == CheckTermResult.CHECK_LEADER && !checkLeader()) {
      return null;
    }
    return candidate;
  }

  /**
   * A server should vote for candidate if:
   * 1. log lags behind candidate, or
   * 2. log equals candidate's, and priority less or equal candidate's
   *
   * See Section 5.4.1 Election restriction
   */
  boolean decideVote(RaftPeer candidate, TermIndex candidateLastEntry) {
    if (candidate == null) {
      return false;
    }
    // Check last log entry
    final TermIndex lastEntry = impl.getState().getLastEntry();
    final int compare = ServerState.compareLog(lastEntry, candidateLastEntry);
    if (compare < 0) {
      return log(true, "our last entry " + lastEntry + " < candidate's last entry " + candidateLastEntry);
    } else if (compare > 0) {
      return reject("our last entry " + lastEntry + " > candidate's last entry " + candidateLastEntry);
    }

    // Check priority
    final RaftPeer peer = conf.getPeer(impl.getId());
    if (peer == null) {
      return reject("our server " + impl.getId() + " is not in the conf " + conf);
    }
    final int priority = peer.getPriority();
    if (priority <= candidate.getPriority()) {
      return log(true, "our priority " + priority + " <= candidate's priority " + candidate.getPriority());
    } else {
      return reject("our priority " + priority + " > candidate's priority " + candidate.getPriority());
    }
  }
}
