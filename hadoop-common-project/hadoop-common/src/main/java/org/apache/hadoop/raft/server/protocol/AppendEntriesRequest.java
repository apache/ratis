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
package org.apache.hadoop.raft.server.protocol;

import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;

public class AppendEntriesRequest extends RaftServerRequest {
  private final long leaderTerm;
  private final TermIndex previousLog;
  private final LogEntryProto[] entries;
  private final long leaderCommit;

  /**
   * Set to true if the receiver of the request should still be in INITIALIZING
   * state. When the first time a peer receives a request with this field set
   * to false, the peer knows it joins the vote and starts heartbeat monitor.
   */
  private final boolean initializing;

  public AppendEntriesRequest(String from, String to, long leaderTerm,
      TermIndex previousLog, LogEntryProto[] entries, long leaderCommit,
      boolean initializing) {
    super(from, to);
    this.leaderTerm = leaderTerm;
    this.previousLog = previousLog;
    this.entries = entries;
    this.leaderCommit = leaderCommit;
    this.initializing = initializing;
  }

  public String getLeaderId() {
    return getRequestorId(); //requestor is the leader
  }

  public long getLeaderTerm() {
    return leaderTerm;
  }

  public TermIndex getPreviousLog() {
    return previousLog;
  }

  public LogEntryProto[] getEntries() {
    return entries;
  }

  public long getLeaderCommit() {
    return leaderCommit;
  }

  public boolean isInitializing() {
    return initializing;
  }

  @Override
  public String toString() {
    return super.toString() + ", leaderTerm: " + getLeaderTerm()
        + ", previous: " + getPreviousLog()
        + ", leaderCommit: " + getLeaderCommit()
        + ", initializing: " + isInitializing();
  }
}
