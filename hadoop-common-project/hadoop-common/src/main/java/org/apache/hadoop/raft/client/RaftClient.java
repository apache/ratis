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
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RaftClient {
  public static final Logger LOG = LoggerFactory.getLogger(RaftClient.class);

  final String clientId;
  final Map<String, RaftPeer> peers = new HashMap<>();
  final RaftClientRequestSender client2serverRpc;

  private volatile String leaderId;

  public RaftClient(String clientId, Collection<RaftPeer> peers,
                    RaftClientRequestSender client2serverRpc, String leaderId) {
    this.clientId = clientId;
    this.client2serverRpc = client2serverRpc;
    for(RaftPeer p : peers) {
      this.peers.put(p.getId(), p);
    }

    this.leaderId = leaderId != null? leaderId : peers.iterator().next().getId();
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

  private void refreshPeers(RaftPeer[] newPeers) {
    if (newPeers != null && newPeers.length > 0) {
      peers.clear();
      for (RaftPeer p : newPeers) {
        peers.put(p.getId(), p);
      }
    }
  }

  public RaftClientReply send(Message message) throws IOException {
    for(;;) {
      final String lid = leaderId;
      LOG.debug("{} sends {} to {}", clientId, message, lid);
      final RaftClientRequest r = new RaftClientRequest(clientId, lid, message);
      RaftClientReply reply = sendRequest(r, lid);
      if (reply != null) {
        return reply;
      }
    }
  }

  public RaftClientReply setConfiguration(RaftPeer[] peersInNewConf)
      throws IOException {
    for(;;) {
      final String lid = leaderId;
      LOG.debug("{} sends new configuration to {}: {}", clientId, lid,
          Arrays.asList(peersInNewConf));
      final SetConfigurationRequest r = new SetConfigurationRequest(clientId,
          lid, peersInNewConf);
      RaftClientReply reply = sendRequest(r, lid);
      if (reply != null) {
        LOG.debug("{} gets reply of setConfiguration: {}", clientId, reply);
        return reply;
      }
    }
  }

  private RaftClientReply sendRequest(RaftClientRequest r,
      final String leader) throws InterruptedIOException {
    try {
      return client2serverRpc.sendRequest(r);
    } catch (NotLeaderException nle) {
      handleNotLeaderException(nle);
    } catch (IOException ioe) {
      final String newLeader = nextLeader(leader, peers.keySet().iterator());
      LOG.debug("{}: Failed with {}, change Leader from {} to {}",
          clientId, ioe, leader, newLeader);
      this.leaderId = newLeader;
      try {
        Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException(
            "Interrupted while waiting for the next retry");
      }
    }
    return null;
  }

  private void handleNotLeaderException(NotLeaderException e)
      throws InterruptedIOException {
    LOG.debug("{}: got NotLeaderException", clientId, e);
    refreshPeers(e.getPeers());
    String newLeader = e.getSuggestedLeader() != null ?
        e.getSuggestedLeader().getId() : null;
    if (newLeader == null) { // usually this should not happen
      newLeader = nextLeader(leaderId, peers.keySet().iterator());
    }
    LOG.debug("{}: use {} as new leader to replace {}", clientId, newLeader,
        leaderId);
    this.leaderId = newLeader;
    try {
      Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException(
          "Interrupted while waiting for the next retry");
    }
  }
}
