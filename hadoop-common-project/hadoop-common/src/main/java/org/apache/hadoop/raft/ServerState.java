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
  private String votedFor = null;

  synchronized long getTerm() {
    return currentTerm;
  }

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

  synchronized <T extends Role> T setRole(T newRole) {
    role = newRole;
    return newRole;
  }

  synchronized long initElection(String id) {
    Preconditions.checkState(isFollower() || isCandidate());
    votedFor = id;
    return ++currentTerm;
  }

  synchronized boolean vote(String candidateId, long term) {
    Preconditions.checkState(isFollower());

    boolean voteGranted = true;
    if (currentTerm > term) {
      voteGranted = false;
    } else if (currentTerm < term) {
      currentTerm = term;
    } else {
      if (votedFor != null && votedFor != candidateId) {
        voteGranted = false;
      }
    }

    if (voteGranted) {
      votedFor = candidateId;
    }
    return voteGranted;
  }
}