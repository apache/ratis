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
package org.apache.ratis.protocol;

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

public abstract class RaftClientMessage implements RaftRpcMessage {
  private final ClientId clientId;
  private final RaftPeerId serverId;
  private final RaftGroupId groupId;
  private final long callId;

  RaftClientMessage(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId) {
    this.clientId = Preconditions.assertNotNull(clientId, "clientId");
    this.serverId = Preconditions.assertNotNull(serverId, "serverId");
    this.groupId = Preconditions.assertNotNull(groupId, "groupId");
    this.callId = callId;
  }

  @Override
  public String getRequestorId() {
    return clientId.toString();
  }

  @Override
  public String getReplierId() {
    return serverId.toString();
  }

  public ClientId getClientId() {
    return clientId;
  }

  public RaftPeerId getServerId() {
    return serverId;
  }

  @Override
  public RaftGroupId getRaftGroupId() {
    return groupId;
  }

  public long getCallId() {
    return callId;
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":" + clientId + "->" + serverId
        + (groupId != null? "@" + groupId: "") + ", cid=" + getCallId();
  }
}
