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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRequestSender;
import org.apache.ratis.protocol.*;
import org.apache.ratis.util.RaftUtils;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** A client who sends requests to a raft service. */
final class RaftClientImpl implements RaftClient {
  private final ClientId clientId;
  private final RaftClientRequestSender requestSender;
  private final Map<RaftPeerId, RaftPeer> peers;
  private final int retryInterval;

  private volatile RaftPeerId leaderId;

  RaftClientImpl(ClientId clientId, Collection<RaftPeer> peers,
      RaftPeerId leaderId, RaftClientRequestSender requestSender,
      int retryInterval) {
    this.clientId = clientId;
    this.requestSender = requestSender;
    this.peers = peers.stream().collect(
        Collectors.toMap(RaftPeer::getId, Function.identity()));
    this.leaderId = leaderId != null? leaderId : peers.iterator().next().getId();
    this.retryInterval = retryInterval;
  }

  @Override
  public ClientId getId() {
    return clientId;
  }

  @Override
  public RaftClientReply send(Message message) throws IOException {
    return send(message, false);
  }

  @Override
  public RaftClientReply sendReadOnly(Message message) throws IOException {
    return send(message, true);
  }

  private RaftClientReply send(Message message, boolean readOnly) throws IOException {
    return sendRequestWithRetry(() -> new RaftClientRequest(
        clientId, leaderId, DEFAULT_SEQNUM, message, readOnly));
  }

  @Override
  public RaftClientReply setConfiguration(RaftPeer[] peersInNewConf)
      throws IOException {
    return sendRequestWithRetry(() -> new SetConfigurationRequest(
        clientId, leaderId, DEFAULT_SEQNUM, peersInNewConf));
  }

  private RaftClientReply sendRequestWithRetry(
      Supplier<RaftClientRequest> supplier)
      throws InterruptedIOException, StateMachineException {
    for(;;) {
      final RaftClientRequest request = supplier.get();
      LOG.debug("{}: {}", clientId, request);
      final RaftClientReply reply = sendRequest(request);
      if (reply != null) {
        LOG.debug("{}: {}", clientId, reply);
        return reply;
      }

      // sleep and then retry
      try {
        Thread.sleep(retryInterval);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw RaftUtils.toInterruptedIOException(
            "Interrupted when sending " + request, ie);
      }
    }
  }

  private RaftClientReply sendRequest(RaftClientRequest request)
      throws StateMachineException {
    try {
      RaftClientReply reply = requestSender.sendRequest(request);
      if (reply.isNotLeader()) {
        handleNotLeaderException(request, reply.getNotLeaderException());
        return null;
      } else {
        return reply;
      }
    } catch (StateMachineException e) {
      throw e;
    } catch (IOException ioe) {
      // TODO different retry policies for different exceptions
      handleIOException(request, ioe, null);
    }
    return null;
  }

  private void handleNotLeaderException(RaftClientRequest request,
      NotLeaderException nle) {
    refreshPeers(nle.getPeers());
    final RaftPeerId newLeader = nle.getSuggestedLeader() == null ? null
        : nle.getSuggestedLeader().getId();
    handleIOException(request, nle, newLeader);
  }

  private void refreshPeers(RaftPeer[] newPeers) {
    if (newPeers != null && newPeers.length > 0) {
      peers.clear();
      for (RaftPeer p : newPeers) {
        peers.put(p.getId(), p);
      }
      // also refresh the rpc proxies for these peers
      requestSender.addServers(Arrays.asList(newPeers));
    }
  }

  private void handleIOException(RaftClientRequest request, IOException ioe,
      RaftPeerId newLeader) {
    LOG.debug("{}: Failed with {}", clientId, ioe);
    final RaftPeerId oldLeader = request.getServerId();
    if (newLeader == null && oldLeader.equals(leaderId)) {
      newLeader = RaftUtils.next(oldLeader, peers.keySet());
    }
    if (newLeader != null && oldLeader.equals(leaderId)) {
      LOG.debug("{}: change Leader from {} to {}", clientId, oldLeader, newLeader);
      this.leaderId = newLeader;
    }
  }

  @Override
  public RaftClientRequestSender getRequestSender() {
    return requestSender;
  }

  @Override
  public void close() throws IOException {
    requestSender.close();
  }
}
