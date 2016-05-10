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
package org.apache.hadoop.raft.client;

import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.protocol.NotLeaderException;
import org.apache.hadoop.raft.protocol.RaftClientReply;
import org.apache.hadoop.raft.protocol.RaftClientRequest;
import org.apache.hadoop.raft.server.RaftRpc;
import org.apache.hadoop.raft.server.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RaftClient {
  public static final Logger LOG = LoggerFactory.getLogger(RaftClient.class);

  final String clientId;
  final Map<String, RaftPeer> servers = new HashMap<>();
  final RaftRpc<RaftClientRequest, RaftClientReply> client2serverRpc;

  private volatile String leaderId;

  public RaftClient(String clientId, Collection<RaftPeer> servers,
      RaftRpc<RaftClientRequest, RaftClientReply> client2serverRpc,
      String leaderId) {
    this.clientId = clientId;
    this.client2serverRpc = client2serverRpc;
    for(RaftPeer p : servers) {
      this.servers.put(p.getId(), p);
    }

    this.leaderId = leaderId != null? leaderId : servers.iterator().next().getId();
  }

  public RaftClientReply send(Message message) throws IOException {
    LOG.debug("{} sends {} to {}", clientId, message, leaderId);
    for(;;) {
      final String lid = leaderId;
      final RaftClientRequest r = new RaftClientRequest(clientId, lid, message);
      try {
        return client2serverRpc.sendRequest(r);
      } catch (NotLeaderException nle) {
        leaderId = nle.getLeader().getId();
        LOG.debug("Leader changed from {} to {}", lid, leaderId);
      }
    }
  }
}
