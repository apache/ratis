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

import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;

import java.util.Collection;
import java.util.Optional;

/**
 * The response of server information request. Sent from server to client.
 */
public class GroupInfoReply extends RaftClientReply {

  private final RaftGroup group;
  private final RoleInfoProto roleInfoProto;
  private final boolean isRaftStorageHealthy;
  private final RaftConfigurationProto conf;

  public GroupInfoReply(RaftClientRequest request, Collection<CommitInfoProto> commitInfos,
      RaftGroup group, RoleInfoProto roleInfoProto, boolean isRaftStorageHealthy,
      RaftConfigurationProto conf) {
    this(request.getClientId(), request.getServerId(), request.getRaftGroupId(),
        request.getCallId(), commitInfos,
        group, roleInfoProto, isRaftStorageHealthy, conf);
  }

  @SuppressWarnings("parameternumber")
  public GroupInfoReply(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId,
      Collection<CommitInfoProto> commitInfos,
      RaftGroup group, RoleInfoProto roleInfoProto, boolean isRaftStorageHealthy,
      RaftConfigurationProto conf) {
    super(clientId, serverId, groupId, callId, true, null, null, 0L, commitInfos);
    this.group = group;
    this.roleInfoProto = roleInfoProto;
    this.isRaftStorageHealthy = isRaftStorageHealthy;
    this.conf = conf;
  }

  public RaftGroup getGroup() {
    return group;
  }

  public RoleInfoProto getRoleInfoProto() {
    return roleInfoProto;
  }

  public boolean isRaftStorageHealthy() {
    return isRaftStorageHealthy;
  }

  public Optional<RaftConfigurationProto> getConf() {
    return Optional.ofNullable(conf);
  }
}
