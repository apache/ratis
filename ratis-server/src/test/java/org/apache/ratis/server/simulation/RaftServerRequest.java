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
package org.apache.ratis.server.simulation;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftRpcMessage;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.util.ProtoUtils;

class RaftServerRequest implements RaftRpcMessage {
  private final AppendEntriesRequestProto appendEntries;
  private final RequestVoteRequestProto requestVote;
  private final InstallSnapshotRequestProto installSnapshot;
  private final StartLeaderElectionRequestProto startLeaderElection;

  RaftServerRequest(AppendEntriesRequestProto a) {
    appendEntries = a;
    requestVote = null;
    installSnapshot = null;
    startLeaderElection = null;
  }

  RaftServerRequest(RequestVoteRequestProto r) {
    appendEntries = null;
    requestVote = r;
    installSnapshot = null;
    startLeaderElection = null;
  }

  RaftServerRequest(InstallSnapshotRequestProto i) {
    appendEntries = null;
    requestVote = null;
    installSnapshot = i;
    startLeaderElection = null;
  }

  RaftServerRequest(StartLeaderElectionRequestProto i) {
    appendEntries = null;
    requestVote = null;
    installSnapshot = null;
    startLeaderElection = i;
  }

  boolean isAppendEntries() {
    return appendEntries != null;
  }

  boolean isRequestVote() {
    return requestVote != null;
  }

  boolean isInstallSnapshot() {
    return installSnapshot != null;
  }

  boolean isStartLeaderElection() {
    return startLeaderElection != null;
  }

  AppendEntriesRequestProto getAppendEntries() {
    return appendEntries;
  }

  RequestVoteRequestProto getRequestVote() {
    return requestVote;
  }

  InstallSnapshotRequestProto getInstallSnapshot() {
    return installSnapshot;
  }

  StartLeaderElectionRequestProto getStartLeaderElection() {
    return startLeaderElection;
  }

  @Override
  public boolean isRequest() {
    return true;
  }

  @Override
  public String getRequestorId() {
    if (isAppendEntries()) {
      return appendEntries.getServerRequest().getRequestorId().toStringUtf8();
    } else if (isRequestVote()) {
      return requestVote.getServerRequest().getRequestorId().toStringUtf8();
    } else if (isInstallSnapshot()) {
      return installSnapshot.getServerRequest().getRequestorId().toStringUtf8();
    } else {
      return startLeaderElection.getServerRequest().getRequestorId().toStringUtf8();
    }
  }

  @Override
  public String getReplierId() {
    if (isAppendEntries()) {
      return appendEntries.getServerRequest().getReplyId().toStringUtf8();
    } else if (isRequestVote()) {
      return requestVote.getServerRequest().getReplyId().toStringUtf8();
    } else if (isInstallSnapshot()) {
      return installSnapshot.getServerRequest().getReplyId().toStringUtf8();
    } else {
      return startLeaderElection.getServerRequest().getReplyId().toStringUtf8();
    }
  }

  @Override
  public RaftGroupId getRaftGroupId() {
    if (isAppendEntries()) {
      return ProtoUtils.toRaftGroupId(appendEntries.getServerRequest().getRaftGroupId());
    } else if (isRequestVote()) {
      return ProtoUtils.toRaftGroupId(requestVote.getServerRequest().getRaftGroupId());
    } else if (isInstallSnapshot()) {
      return ProtoUtils.toRaftGroupId(installSnapshot.getServerRequest().getRaftGroupId());
    } else {
      return ProtoUtils.toRaftGroupId(startLeaderElection.getServerRequest().getRaftGroupId());
    }
  }
}
