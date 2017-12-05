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

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReflectionUtils;

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

  public RaftClientReply(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId, boolean success, Message message,
      RaftException exception) {
    super(clientId, serverId, groupId);
    this.success = success;
    this.callId = callId;
    this.message = message;
    this.exception = exception;

    if (exception != null) {
      Preconditions.assertTrue(!success,
          () -> "Inconsistent parameters: success && exception != null: " + this);
      Preconditions.assertTrue(
          ReflectionUtils.isInstance(exception, NotLeaderException.class, StateMachineException.class),
          () -> "Unexpected exception class: " + this);
    }
  }

  public RaftClientReply(RaftClientRequest request,
      RaftException exception) {
    this(request.getClientId(), request.getServerId(), request.getRaftGroupId(),
        request.getCallId(), false, null, exception);
  }

  public RaftClientReply(RaftClientRequest request, Message message) {
    this(request.getClientId(), request.getServerId(), request.getRaftGroupId(),
        request.getCallId(), true, message, null);
  }

  @Override
  public final boolean isRequest() {
    return false;
  }

  public long getCallId() {
    return callId;
  }

  @Override
  public String toString() {
    return super.toString() + ", cid=" + getCallId()
        + ", success? " + isSuccess() + ", exception=" + exception;
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

  /** If this reply has {@link StateMachineException}, return it; otherwise return null. */
  public StateMachineException getStateMachineException() {
    return JavaUtils.cast(exception, StateMachineException.class);
  }
}
