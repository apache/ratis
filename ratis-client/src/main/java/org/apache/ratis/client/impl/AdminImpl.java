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

import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.rpc.CallId;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

class AdminImpl implements AdminApi {
  private final RaftClientImpl client;

  AdminImpl(RaftClientImpl client) {
    this.client = Objects.requireNonNull(client, "client == null");
  }

  @Override
  public RaftClientReply setConfiguration(List<RaftPeer> peersInNewConf) throws IOException {
    Objects.requireNonNull(peersInNewConf, "peersInNewConf == null");

    final long callId = CallId.getAndIncrement();
    // also refresh the rpc proxies for these peers
    client.getClientRpc().addRaftPeers(peersInNewConf);
    return client.io().sendRequestWithRetry(() -> new SetConfigurationRequest(
        client.getId(), client.getLeaderId(), client.getGroupId(), callId, peersInNewConf));
  }

  @Override
  public RaftClientReply transferLeadership(RaftPeerId newLeader, long timeoutMs) throws IOException {
    final long callId = CallId.getAndIncrement();
    return client.io().sendRequestWithRetry(() -> new TransferLeadershipRequest(
        client.getId(), client.getLeaderId(), client.getGroupId(), callId, newLeader, timeoutMs));
  }
}
