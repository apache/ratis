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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.server.protocol.RaftLogEntry;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.raft.server.storage.MemoryRaftLog;
import org.apache.hadoop.raft.server.storage.RaftLog;

import java.util.List;

/**
 * Common states of a raft peer. Protected by RaftServer's lock.
 */
public class ServerState {
  private final String selfId;
  /** Raft log */
  private final RaftLog log;
  /** Raft configuration */
  private RaftConfiguration raftConf;

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

  ServerState(String id, RaftConfiguration conf) {
    this.selfId = id;
    this.raftConf = conf;
    // TODO load log/currentTerm/votedFor/leaderId from persistent storage
    log = new MemoryRaftLog(id);
    currentTerm = 0;
    votedFor = null;
    leaderId = null;
    loadConfFromLog();
  }

  private ServerState(String id, RaftConfiguration conf, MemoryRaftLog newLog) {
    this.selfId = id;
    this.raftConf = conf;
    currentTerm = 0;
    votedFor = null;
    leaderId = null;
    log = newLog;
  }

  private void loadConfFromLog() {
    // TODO apply raftlog and update its RaftConfiguration
  }

  public RaftConfiguration getRaftConf() {
    return raftConf;
  }

  public void setRaftConf(RaftConfiguration conf) {
    this.raftConf = conf;
    RaftServer.LOG.info("{}: successfully update the configuration {}",
        getSelfId(), conf);
  }

  public String getSelfId() {
    return this.selfId;
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public static ServerState buildServerState(ServerState oldState,
      List<RaftLogEntry> logEntries) {
    MemoryRaftLog newLog = new MemoryRaftLog(oldState.getSelfId(), logEntries);
    return new ServerState(oldState.selfId, oldState.raftConf,
        newLog);
  }

  void setCurrentTerm(long term) {
    currentTerm = term;
  }

  String getLeaderId() {
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
    leaderId = null;
    return ++currentTerm;
  }

  void resetLeaderAndVotedFor() {
    votedFor = null;
    leaderId = null;
  }

  /**
   * Vote for a candidate and update the local state.
   */
  void grantVote(String candidateId) {
    votedFor = candidateId;
    leaderId = null;
  }

  void setLeader(String leaderId) {
    this.leaderId = leaderId;
  }

  void becomeLeader() {
    leaderId = selfId;
  }

  public RaftLog getLog() {
    return log;
  }

  long applyLog(Message message) {
    return log.append(currentTerm, message);
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
      // or no leader has been set for the current term, make sure to update
      // leader and term later
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
    return selfId + ": term=" + currentTerm + ", leader=" + leaderId
        + ", voted=" + votedFor + ", raftlog: " + log + ", conf: " + raftConf;
  }

  boolean isConfCommitted() {
    return getLog().getLastCommitted().getIndex() >= raftConf.getLogEntryIndex();
  }
}