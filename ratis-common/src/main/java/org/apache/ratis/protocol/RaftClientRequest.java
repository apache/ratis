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

import org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.util.Preconditions;

import static org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto.Type.READ;
import static org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto.Type.STALE_READ;
import static org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto.Type.WRITE;

/**
 * Request from client to server
 */
public class RaftClientRequest extends RaftClientMessage {
  private final long callId;
  private final long seqNum;

  private final RaftClientRequestProto.Type type;
  private final Message message;

  private final long minIndex;

  public RaftClientRequest(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId, Message message) {
    this(clientId, serverId, groupId, callId, 0L, WRITE, message, 0L);
  }

  public RaftClientRequest(ClientId clientId, RaftPeerId serverId,
       RaftGroupId groupId, long callId, long seqNum, Message message) {
    this(clientId, serverId, groupId, callId, seqNum, WRITE, message, 0L);
  }

  public RaftClientRequest(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId,
      long callId, long seqNum, RaftClientRequestProto.Type type, Message message, long minIndex) {
    super(clientId, serverId, groupId);
    this.callId = callId;
    this.seqNum = seqNum;
    this.type = type;
    this.message = message;
    this.minIndex = minIndex;

    Preconditions.assertTrue(minIndex >= 0, "minIndex < 0");
  }

  @Override
  public final boolean isRequest() {
    return true;
  }

  public long getCallId() {
    return callId;
  }

  public long getSeqNum() {
    return seqNum;
  }

  public Message getMessage() {
    return message;
  }

  public RaftClientRequestProto.Type getType() {
    return type;
  }

  public boolean isReadOnly() {
    return getType() != WRITE;
  }

  public boolean isStaleRead() {
    return getType() == STALE_READ;
  }

  /** @return the minimum required commit index for processing the request. */
  public long getMinIndex() {
    return minIndex;
  }

  @Override
  public String toString() {
    return super.toString() + ", cid=" + callId + ", seq=" + seqNum + " "
        + (!isReadOnly()? "RW": isStaleRead()? "StaleRead(" + getMinIndex() + ")": "RO")
        + ", " + getMessage();
  }
}
