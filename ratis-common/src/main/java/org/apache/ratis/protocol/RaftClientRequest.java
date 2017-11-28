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

/**
 * Request from client to server
 */
public class RaftClientRequest extends RaftClientMessage {
  private final long callId;
  private final long seqNum;

  private final Message message;
  private final boolean readOnly;

  public RaftClientRequest(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId, Message message) {
    this(clientId, serverId, groupId, callId, 0L, message, false);
  }

  public RaftClientRequest(ClientId clientId, RaftPeerId serverId,
       RaftGroupId groupId, long callId, long seqNum, Message message) {
    this(clientId, serverId, groupId, callId, seqNum, message, false);
  }

  public RaftClientRequest(ClientId clientId, RaftPeerId serverId,
       RaftGroupId groupId, long callId, long seqNum, Message message, boolean readOnly) {
    super(clientId, serverId, groupId);
    this.callId = callId;
    this.seqNum = seqNum;
    this.message = message;
    this.readOnly = readOnly;
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

  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public String toString() {
    return super.toString() + ", cid=" + callId + ", seq=" + seqNum + " "
        + (isReadOnly()? "RO": "RW") + ", " + getMessage();
  }
}
