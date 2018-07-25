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

import org.apache.ratis.shaded.proto.RaftProtos;
import org.apache.ratis.shaded.proto.RaftProtos.CommitInfoProto;

import java.util.Collection;

/**
 * The response of server information request. Sent from server to client.
 */
public class ServerInformationReply extends RaftClientReply {
  private final RaftGroup group;
  private final RaftProtos.RaftPeerRole role;
  private final long roleElapsedTime;
  private final boolean isRaftStorageHealthy;

  public ServerInformationReply(
      RaftClientRequest request, RaftProtos.RaftPeerRole role, long roleElapsedTime,
      boolean isRaftStorageHealthy, Collection<CommitInfoProto> commitInfos, RaftGroup group) {
    super(request, commitInfos);
    this.role = role;
    this.roleElapsedTime = roleElapsedTime;
    this.isRaftStorageHealthy = isRaftStorageHealthy;
    this.group = group;
  }

  public ServerInformationReply(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId,
      long callId, boolean success, RaftProtos.RaftPeerRole role, long roleElapsedTime,
      boolean isRaftStorageHealthy, Collection<CommitInfoProto> commitInfos, RaftGroup group) {
    super(clientId, serverId, groupId, callId, success, null, null, commitInfos);
    this.role = role;
    this.roleElapsedTime = roleElapsedTime;
    this.isRaftStorageHealthy = isRaftStorageHealthy;
    this.group = group;
  }

  public RaftGroup getGroup() {
    return group;
  }


  public RaftProtos.RaftPeerRole getRole() {
    return role;
  }

  public long getRoleElapsedTime() {
    return roleElapsedTime;
  }

  public boolean isRaftStorageHealthy() {
    return isRaftStorageHealthy;
  }
}
