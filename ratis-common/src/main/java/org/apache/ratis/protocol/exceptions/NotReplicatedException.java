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
package org.apache.ratis.protocol.exceptions;

import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;

import java.util.Collection;

public class NotReplicatedException extends RaftException {
  private final long callId;
  private final ReplicationLevel requiredReplication;
  private final long logIndex;
  /** This is only populated on client-side since RaftClientReply already has commitInfos */
  private Collection<CommitInfoProto> commitInfos;

  public NotReplicatedException(long callId, ReplicationLevel requiredReplication, long logIndex) {
    super("Request with call Id " + callId + " and log index " + logIndex
        + " is not yet replicated to " + requiredReplication);
    this.callId = callId;
    this.requiredReplication = requiredReplication;
    this.logIndex = logIndex;
  }

  public NotReplicatedException(long callId, ReplicationLevel requiredReplication, long logIndex,
                                Collection<CommitInfoProto> commitInfos) {
    this(callId, requiredReplication, logIndex);
    this.commitInfos = commitInfos;
  }

  public long getCallId() {
    return callId;
  }

  public ReplicationLevel getRequiredReplication() {
    return requiredReplication;
  }

  public long getLogIndex() {
    return logIndex;
  }

  public Collection<CommitInfoProto> getCommitInfos() {
    return commitInfos;
  }
}
