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

import org.apache.ratis.protocol.RaftRpcMessage;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteReplyProto;

import com.google.common.base.Preconditions;

public class RaftServerReply extends RaftRpcMessage {
  private final AppendEntriesReplyProto appendEntries;
  private final RequestVoteReplyProto requestVote;
  private final InstallSnapshotReplyProto installSnapshot;

  RaftServerReply(AppendEntriesReplyProto a) {
    appendEntries = Preconditions.checkNotNull(a);
    requestVote = null;
    installSnapshot = null;
  }

  RaftServerReply(RequestVoteReplyProto r) {
    appendEntries = null;
    requestVote = Preconditions.checkNotNull(r);
    installSnapshot = null;
  }

  RaftServerReply(InstallSnapshotReplyProto i) {
    appendEntries = null;
    requestVote = null;
    installSnapshot = Preconditions.checkNotNull(i);
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

  AppendEntriesReplyProto getAppendEntries() {
    return appendEntries;
  }

  RequestVoteReplyProto getRequestVote() {
    return requestVote;
  }

  InstallSnapshotReplyProto getInstallSnapshot() {
    return installSnapshot;
  }

  @Override
  public boolean isRequest() {
    return false;
  }

  @Override
  public String getRequestorId() {
    if (isAppendEntries()) {
      return appendEntries.getServerReply().getRequestorId();
    } else if (isRequestVote()) {
      return requestVote.getServerReply().getRequestorId();
    } else {
      return installSnapshot.getServerReply().getRequestorId();
    }
  }

  @Override
  public String getReplierId() {
    if (isAppendEntries()) {
      return appendEntries.getServerReply().getReplyId();
    } else if (isRequestVote()) {
      return requestVote.getServerReply().getReplyId();
    } else {
      return installSnapshot.getServerReply().getReplyId();
    }
  }
}
