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
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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

  static String nextLeader(final String leaderId, final Iterator<String> i) {
    final String first = i.next();
    for(String previous = first; i.hasNext(); ) {
      final String current = i.next();
      if (leaderId.equals(previous)) {
        return current;
      }
      previous = current;
    }
    return first;
  }

  public RaftClientReply send(Message message) throws IOException {
    for(;;) {
      final String lid = leaderId;
      LOG.debug("{} sends {} to {}", clientId, message, lid);
      final RaftClientRequest r = new RaftClientRequest(clientId, lid, message);
      try {
        return client2serverRpc.sendRequest(r);
      } catch (NotLeaderException nle) {
        final String newLeader = nle.getLeader().getId();
        LOG.debug("{}: Leader changed from {} to {}", clientId, lid, newLeader);
        leaderId = newLeader;
      } catch (IOException ioe) {
        final String newLeader = nextLeader(lid, servers.keySet().iterator());
        LOG.debug("{}: Failed with {}, change Leader from {} to {}",
            clientId, ioe, lid, newLeader);
        leaderId = newLeader;
      }
    }
  }
}
