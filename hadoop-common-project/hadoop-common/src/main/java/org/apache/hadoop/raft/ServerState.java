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
package org.apache.hadoop.raft;

import org.apache.hadoop.raft.RaftServer.Candidate;
import org.apache.hadoop.raft.RaftServer.Follower;
import org.apache.hadoop.raft.RaftServer.Leader;
import org.apache.hadoop.raft.RaftServer.Role;

import com.google.common.base.Preconditions;

class ServerState {
  private Role role;
  private long currentTerm = 0;
  private String leaderId = null;

  synchronized boolean isFollower() {
    return role instanceof Follower;
  }

  synchronized boolean isCandidate() {
    return role instanceof Candidate;
  }

  synchronized boolean isLeader() {
    return role instanceof Leader;
  }

  synchronized Role getRole() {
    return role;
  }

  synchronized <R extends Role> R changeRole(R newRole) {
    Preconditions.checkState(role != newRole);
    role = newRole;
    return newRole;
  }

  synchronized long getCurrentTerm() {
    return currentTerm;
  }

  synchronized String getLeaderId() {
    return leaderId;
  }

  synchronized boolean recognizeLeader(String leaderId, long leaderTerm) {
    if (leaderTerm < currentTerm) {
      return false;
    } else if (leaderTerm > currentTerm) {
      this.leaderId = leaderId;
      this.currentTerm = leaderTerm;
      return true;
    }

    if (this.leaderId == null || !isLeader()) {
      this.leaderId = leaderId;
    }
    return this.leaderId == leaderId;
  }

  synchronized boolean recognizeCandidate(String candidateId, long candidateTerm) {
    if (candidateTerm < currentTerm) {
      return false;
    } else if (candidateTerm == currentTerm && !isFollower()) {
      return false;
    }
    return true;
  }

  synchronized long initElection(String id) {
    Preconditions.checkState(isFollower() || isCandidate());
    leaderId = id;
    return ++currentTerm;
  }

  synchronized boolean vote(String candidateId, long candidateTerm) {
    Preconditions.checkState(isFollower());

    boolean voteGranted = true;
    if (currentTerm > candidateTerm) {
      voteGranted = false;
    } else if (currentTerm < candidateTerm) {
      currentTerm = candidateTerm;
    } else {
      if (leaderId != null && leaderId != candidateId) {
        voteGranted = false;
      }
    }

    if (voteGranted) {
      leaderId = candidateId;
    }
    return voteGranted;
  }
}