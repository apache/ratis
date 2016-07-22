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
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.raft.server.protocol.pb.ProtoUtils;
import org.apache.hadoop.raft.server.storage.MemoryRaftLog;
import org.apache.hadoop.raft.server.storage.RaftLog;
import org.apache.hadoop.raft.server.storage.SegmentedRaftLog;
import org.apache.hadoop.raft.util.RaftUtils;

import java.io.IOException;

import static org.apache.hadoop.raft.server.RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_DEFAULT;
import static org.apache.hadoop.raft.server.RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY;

/**
 * Common states of a raft peer. Protected by RaftServer's lock.
 */
public class ServerState {
  private final String selfId;
  /** Raft log */
  private final RaftLog log;
  /** Raft configuration */
  private final ConfigurationManager configurationManager;

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

  ServerState(String id, RaftConfiguration conf, RaftProperties prop)
      throws IOException {
    this.selfId = id;
    configurationManager = new ConfigurationManager(conf);
    leaderId = null;

    log = initLog(id, prop);
    RaftLog.Metadata metadata = log.loadMetadata();
    currentTerm = metadata.getTerm();
    votedFor = metadata.getVotedFor();
  }

  private RaftLog initLog(String id, RaftProperties prop)
      throws IOException {
    if (prop.getBoolean(RAFT_SERVER_USE_MEMORY_LOG_KEY,
        RAFT_SERVER_USE_MEMORY_LOG_DEFAULT)) {
      return new MemoryRaftLog(id);
    } else {
      return new SegmentedRaftLog(id, prop, configurationManager);
    }
  }

  public RaftConfiguration getRaftConf() {
    return configurationManager.getCurrent();
  }

  public String getSelfId() {
    return this.selfId;
  }

  public long getCurrentTerm() {
    return currentTerm;
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

  void persistMetadata() throws IOException {
    this.log.writeMetadata(currentTerm, votedFor);
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
    LogEntryProto lastEntry = log.getLastEntry();
    if (lastEntry == null) {
      return true;
    } else if (candidateLastEntry == null) {
      return false;
    }
    return ProtoUtils.toTermIndex(lastEntry).compareTo(candidateLastEntry) <= 0;
  }

  @Override
  public String toString() {
    return selfId + ": term=" + currentTerm + ", leader=" + leaderId
        + ", voted=" + votedFor + ", raftlog: " + log + ", conf: "
        + getRaftConf();
  }

  boolean isConfCommitted() {
    return getLog().getLastCommitted().getIndex() >=
        getRaftConf().getLogEntryIndex();
  }

  public void setRaftConf(long logIndex, RaftConfiguration conf) {
    configurationManager.addConfiguration(logIndex, conf);
    RaftServer.LOG.info("{}: successfully update the configuration {}",
        getSelfId(), conf);
  }

  void updateConfiguration(LogEntryProto[] entries) {
    if (entries != null && entries.length > 0) {
      configurationManager.removeConfigurations(entries[0].getIndex());
      for (LogEntryProto entry : entries) {
        if (RaftUtils.isConfigurationLogEntry(entry)) {
          configurationManager.addConfiguration(entry.getIndex(),
              RaftUtils.convertProtoToConf(entry.getIndex(),
                  entry.getConfigurationEntry()));
        }
      }
    }
  }
}