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
package org.apache.hadoop.raft.server;

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.server.protocol.TermIndex;

/**
 * Common states of a raft peer. Protected by RaftServer's lock.
 */
public class ServerState {
  private final String selfId;
  /** Raft log */
  private final RaftLog log;

  /**
   * Latest term server has seen. initialized to 0 on first boot, increases
   * monotonically.
   */
  private long currentTerm;
  /**
   * The server ID of the leader for this term. Null means either there is
   * no leader for this term yet or this server does not know who it is yet.
   */
  private String leaderId;
  /**
   * Candidate that this peer granted vote for in current term (or null if none)
   */
  private String votedFor;

  /** Index of highest log entry applied to state machine */
  private long lastApplied;

  ServerState(String id) {
    this.selfId = id;
    // TODO load log/currentTerm/votedFor/leaderId from persistent storage
    log = new RaftLog();
    currentTerm = 0;
    votedFor = null;
    leaderId = null;
  }

  public String getSelfId() {
    return this.selfId;
  }

  long getCurrentTerm() {
    return currentTerm;
  }

  String getLeaderId() {
    return leaderId;
  }

  /**
   * Become a candidate and start leader election
   */
  long initElection() {
    votedFor = selfId;
    leaderId = null;
    return ++currentTerm;
  }

  /**
   * Vote for a candidate and update the local state.
   */
  void grantVote(String candidateId, long candidateTerm) {
    currentTerm = candidateTerm;
    votedFor = candidateId;
    leaderId = null;
  }

  void setLeader(String leaderId, long leaderTerm) {
    currentTerm = leaderTerm;
    this.leaderId = leaderId;
  }

  RaftLog getLog() {
    return log;
  }

  /**
   * Check if accept the leader selfId and term from the incoming AppendEntries rpc.
   * If accept, update the current state.
   * @return true if the check passes
   */
  boolean recognizeLeader(String leaderId, long leaderTerm) {
    if (leaderTerm < currentTerm) {
      return false;
    } else if (leaderTerm > currentTerm || this.leaderId == null) {
      // If the request indicates a term that is greater than the current term
      // or no leader has been set for the current term,
      // update leader and term.
      setLeader(leaderId, leaderTerm);
      return true;
    }
    Preconditions.checkArgument(this.leaderId.equals(leaderId),
        "selfId:%s, this.leaderId:%s, received leaderId:%s",
        selfId, this.leaderId, leaderId);
    return true;
  }

  /**
   * Check if the candidate's term is acceptable
   */
  boolean recognizeCandidate(String candidateId,
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
    TermIndex lastEntry = log.getLastEntry();
    return lastEntry == null ||
        (candidateLastEntry != null &&
            lastEntry.compareTo(candidateLastEntry) <= 0);
  }

  @Override
  public String toString() {
    return selfId + ", current term: " + currentTerm
        + ", leaderId: " + leaderId + ", votedFor: " + votedFor
        + ", raft log: " + log;
  }
}