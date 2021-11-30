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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.util.Preconditions;

import java.io.IOException;
import java.util.Objects;

class GroupManagementImpl implements GroupManagementApi {
  private final RaftPeerId server;
  private final RaftClientImpl client;

  GroupManagementImpl(RaftPeerId server, RaftClientImpl client) {
    this.server = Objects.requireNonNull(server, "server == null");
    this.client = Objects.requireNonNull(client, "client == null");
  }

  @Override
  public RaftClientReply add(RaftGroup newGroup) throws IOException {
    Objects.requireNonNull(newGroup, "newGroup == null");

    final long callId = CallId.getAndIncrement();
    client.getClientRpc().addRaftPeers(newGroup.getPeers());
    return client.io().sendRequestWithRetry(
        () -> GroupManagementRequest.newAdd(client.getId(), server, callId, newGroup));
  }

  @Override
  public RaftClientReply remove(RaftGroupId groupId, boolean deleteDirectory, boolean renameDirectory)
      throws IOException {
    Objects.requireNonNull(groupId, "groupId == null");

    final long callId = CallId.getAndIncrement();
    return client.io().sendRequestWithRetry(
        () -> GroupManagementRequest.newRemove(client.getId(), server, callId, groupId,
            deleteDirectory, renameDirectory));
  }

  @Override
  public GroupListReply list() throws IOException {
    final long callId = CallId.getAndIncrement();
    final RaftClientReply reply = client.io().sendRequestWithRetry(
        () -> new GroupListRequest(client.getId(), server, client.getGroupId(), callId));
    Preconditions.assertTrue(reply instanceof GroupListReply, () -> "Unexpected reply: " + reply);
    return (GroupListReply)reply;
  }

  @Override
  public GroupInfoReply info(RaftGroupId groupId) throws IOException {
    final RaftGroupId gid = groupId != null? groupId: client.getGroupId();
    final long callId = CallId.getAndIncrement();
    final RaftClientReply reply = client.io().sendRequestWithRetry(
        () -> new GroupInfoRequest(client.getId(), server, gid, callId));
    Preconditions.assertTrue(reply instanceof GroupInfoReply, () -> "Unexpected reply: " + reply);
    return (GroupInfoReply)reply;
  }
}
