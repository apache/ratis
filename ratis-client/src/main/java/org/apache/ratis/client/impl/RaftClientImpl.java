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
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.protocol.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** A client who sends requests to a raft service. */
final class RaftClientImpl implements RaftClient {
  private static final AtomicLong callIdCounter = new AtomicLong();

  private static long nextCallId() {
    return callIdCounter.getAndIncrement() & Long.MAX_VALUE;
  }

  private final ClientId clientId;
  private final RaftClientRpc clientRpc;
  private final Collection<RaftPeer> peers;
  private final TimeDuration retryInterval;

  private volatile RaftPeerId leaderId;

  RaftClientImpl(ClientId clientId, Collection<RaftPeer> peers,
      RaftPeerId leaderId, RaftClientRpc clientRpc,
      TimeDuration retryInterval) {
    this.clientId = clientId;
    this.clientRpc = clientRpc;
    this.peers = peers;
    this.leaderId = leaderId != null? leaderId : peers.iterator().next().getId();
    this.retryInterval = retryInterval;

    clientRpc.addServers(peers);
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
    final long callId = nextCallId();
    return sendRequestWithRetry(() -> new RaftClientRequest(
        clientId, leaderId, callId, message, readOnly));
  }

  @Override
  public RaftClientReply setConfiguration(RaftPeer[] peersInNewConf)
      throws IOException {
    final long callId = nextCallId();
    // also refresh the rpc proxies for these peers
    clientRpc.addServers(Arrays.stream(peersInNewConf).filter(peers::contains)
        .collect(Collectors.toCollection(ArrayList::new)));
    return sendRequestWithRetry(() -> new SetConfigurationRequest(
        clientId, leaderId, callId, peersInNewConf));
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
        retryInterval.sleep();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw IOUtils.toInterruptedIOException(
            "Interrupted when sending " + request, ie);
      }
    }
  }

  private RaftClientReply sendRequest(RaftClientRequest request)
      throws StateMachineException {
    RaftClientReply reply = null;
    try {
      reply = clientRpc.sendRequest(request);
    } catch (IOException ioe) {
      handleIOException(request, ioe, null);
    }
    if (reply != null) {
      if (reply.isNotLeader()) {
        handleNotLeaderException(request, reply.getNotLeaderException());
        return null;
      } else if (reply.hasStateMachineException()) {
        throw reply.getStateMachineException();
      } else {
        return reply;
      }
    }
    return null;
  }

  private void handleNotLeaderException(RaftClientRequest request,
      NotLeaderException nle) {
    refreshPeers(Arrays.asList(nle.getPeers()));
    final RaftPeerId newLeader = nle.getSuggestedLeader() == null ? null
        : nle.getSuggestedLeader().getId();
    handleIOException(request, nle, newLeader);
  }

  private void refreshPeers(Collection<RaftPeer> newPeers) {
    if (newPeers != null && newPeers.size() > 0) {
      peers.clear();
      peers.addAll(newPeers);
      // also refresh the rpc proxies for these peers
      clientRpc.addServers(newPeers);
    }
  }

  private void handleIOException(RaftClientRequest request, IOException ioe,
      RaftPeerId newLeader) {
    LOG.debug("{}: suggested new leader: {}. Failed with {}", clientId,
        newLeader, ioe);
    final RaftPeerId oldLeader = request.getServerId();
    if (newLeader == null && oldLeader.equals(leaderId)) {
      newLeader = CollectionUtils.next(oldLeader, CollectionUtils.as(peers, RaftPeer::getId));
    }
    if (newLeader != null && oldLeader.equals(leaderId)) {
      LOG.debug("{}: change Leader from {} to {}", clientId, oldLeader, newLeader);
      this.leaderId = newLeader;
    }
  }

  @Override
  public RaftClientRpc getClientRpc() {
    return clientRpc;
  }

  @Override
  public void close() throws IOException {
    clientRpc.close();
  }
}
