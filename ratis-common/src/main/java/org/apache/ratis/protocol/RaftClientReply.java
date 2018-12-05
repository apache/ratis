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
package org.apache.ratis.protocol;

import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.ReflectionUtils;

import java.util.Collection;
import java.util.Collections;

/**
 * Reply from server to client
 */
public class RaftClientReply extends RaftClientMessage {
  private final boolean success;
  private final long callId;

  /**
   * We mainly track two types of exceptions here:
   * 1. NotLeaderException if the server is not leader
   * 2. StateMachineException if the server's state machine returns an exception
   */
  private final RaftException exception;
  private final Message message;

  /**
   * This field is the log index of the transaction
   * if (1) the request is {@link RaftClientRequestProto.TypeCase#WRITE} and (2) the reply is success.
   * Otherwise, this field is not used.
   */
  private final long logIndex;
  /** The commit information when the reply is created. */
  private final Collection<CommitInfoProto> commitInfos;

  public RaftClientReply(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId,
      long callId, boolean success, Message message, RaftException exception,
      long logIndex, Collection<CommitInfoProto> commitInfos) {
    super(clientId, serverId, groupId);
    this.success = success;
    this.callId = callId;
    this.message = message;
    this.exception = exception;
    this.logIndex = logIndex;
    this.commitInfos = commitInfos != null? commitInfos: Collections.emptyList();

    if (exception != null) {
      Preconditions.assertTrue(!success,
          () -> "Inconsistent parameters: success && exception != null: " + this);
      Preconditions.assertTrue(ReflectionUtils.isInstance(exception,
          NotLeaderException.class, NotReplicatedException.class, StateMachineException.class,
          RaftRetryFailureException.class), () -> "Unexpected exception class: " + this);
    }
  }

  public RaftClientReply(RaftClientRequest request, RaftException exception, Collection<CommitInfoProto> commitInfos) {
    this(request.getClientId(), request.getServerId(), request.getRaftGroupId(),
        request.getCallId(), false, null, exception, 0L, commitInfos);
  }

  public RaftClientReply(RaftClientRequest request, Collection<CommitInfoProto> commitInfos) {
    this(request, (Message) null, commitInfos);
  }

  public RaftClientReply(RaftClientRequest request, Message message, Collection<CommitInfoProto> commitInfos) {
    this(request.getClientId(), request.getServerId(), request.getRaftGroupId(),
        request.getCallId(), true, message, null, 0L, commitInfos);
  }

  public RaftClientReply(RaftClientRequest request, NotReplicatedException nre,
      Collection<CommitInfoProto> commitInfos) {
    this(request.getClientId(), request.getServerId(), request.getRaftGroupId(),
        request.getCallId(), false, request.getMessage(), nre, nre.getLogIndex(), commitInfos);
  }

  public RaftClientReply(RaftClientReply reply, NotReplicatedException nre) {
    this(reply.getClientId(), reply.getServerId(), reply.getRaftGroupId(),
        reply.getCallId(), false, reply.getMessage(), nre, reply.getLogIndex(), reply.getCommitInfos());
  }

  /**
   * Get the commit information for the entire group.
   * The commit information may be unavailable for exception reply.
   *
   * @return the commit information if it is available; otherwise, return null.
   */
  public Collection<CommitInfoProto> getCommitInfos() {
    return commitInfos;
  }

  @Override
  public final boolean isRequest() {
    return false;
  }

  public long getCallId() {
    return callId;
  }

  public long getLogIndex() {
    return logIndex;
  }

  @Override
  public String toString() {
    return super.toString() + ", cid=" + getCallId() + ", "
        + (isSuccess()? "SUCCESS":  "FAILED " + exception)
        + ", logIndex=" + getLogIndex() + ", commits" + ProtoUtils.toString(commitInfos);
  }

  public boolean isSuccess() {
    return success;
  }

  public Message getMessage() {
    return message;
  }

  /** If this reply has {@link NotLeaderException}, return it; otherwise return null. */
  public NotLeaderException getNotLeaderException() {
    return JavaUtils.cast(exception, NotLeaderException.class);
  }

  /** If this reply has {@link NotReplicatedException}, return it; otherwise return null. */
  public NotReplicatedException getNotReplicatedException() {
    return JavaUtils.cast(exception, NotReplicatedException.class);
  }

  /** If this reply has {@link StateMachineException}, return it; otherwise return null. */
  public StateMachineException getStateMachineException() {
    return JavaUtils.cast(exception, StateMachineException.class);
  }

  /** If this reply has {@link RaftRetryFailureException}, return it; otherwise return null. */
  public RaftRetryFailureException getRetryFailureException() {
    return JavaUtils.cast(exception, RaftRetryFailureException.class);
  }
}
