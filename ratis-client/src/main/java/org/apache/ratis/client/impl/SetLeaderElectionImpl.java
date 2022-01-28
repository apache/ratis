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

import org.apache.ratis.client.api.SetLeaderElectionApi;
import org.apache.ratis.protocol.LeaderElectionRequest;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.CallId;

import java.io.IOException;
import java.util.Objects;

public class SetLeaderElectionImpl implements SetLeaderElectionApi {

  private final RaftClientImpl client;
  private final RaftPeerId server;

  SetLeaderElectionImpl(RaftPeerId server, RaftClientImpl client) {
    this.server =  Objects.requireNonNull(server, "server == null");
    this.client = Objects.requireNonNull(client, "client == null");
  }
  @Override
  public RaftClientReply pause(long timeoutMs) throws IOException {
    final long callId = CallId.getAndIncrement();
    return client.io().sendRequestWithRetry(() -> LeaderElectionRequest.newPause(client.getId(),
        server, client.getGroupId(), callId, timeoutMs));
  }

  @Override
  public RaftClientReply resume(long timeoutMs) throws IOException {
    final long callId = CallId.getAndIncrement();
    return client.io().sendRequestWithRetry(() -> LeaderElectionRequest.newResume(client.getId(),
        server, client.getGroupId(), callId, timeoutMs));
  }
}
