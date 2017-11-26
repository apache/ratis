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
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.protocol.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** A client who sends requests to a raft service. */
final class RaftClientImpl implements RaftClient {
  private static final AtomicLong callIdCounter = new AtomicLong();

  private static long nextCallId() {
    return callIdCounter.getAndIncrement() & Long.MAX_VALUE;
  }

  private final ClientId clientId;
  private final RaftClientRpc clientRpc;
  private final Collection<RaftPeer> peers;
  private final RaftGroupId groupId;
  private final TimeDuration retryInterval;

  private volatile RaftPeerId leaderId;

  private final ScheduledExecutorService scheduler;
  private final Semaphore asyncRequestSemaphore;

  RaftClientImpl(ClientId clientId, RaftGroup group, RaftPeerId leaderId,
      RaftClientRpc clientRpc, TimeDuration retryInterval, RaftProperties properties) {
    this.clientId = clientId;
    this.clientRpc = clientRpc;
    this.peers = new ConcurrentLinkedQueue<>(group.getPeers());
    this.groupId = group.getGroupId();
    this.leaderId = leaderId != null? leaderId
        : !peers.isEmpty()? peers.iterator().next().getId(): null;
    this.retryInterval = retryInterval;
    asyncRequestSemaphore = new Semaphore(RaftClientConfigKeys.Async.maxOutstandingRequests(properties));
    scheduler = Executors.newScheduledThreadPool(RaftClientConfigKeys.Async.schedulerThreads(properties));
    clientRpc.addServers(peers);
  }

  @Override
  public ClientId getId() {
    return clientId;
  }

  @Override
  public CompletableFuture<RaftClientReply> sendAsync(Message message) {
    return sendAsync(message, false);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendReadOnlyAsync(Message message) {
    return sendAsync(message, true);
  }

  private CompletableFuture<RaftClientReply> sendAsync(Message message,
      boolean readOnly) {
    Objects.requireNonNull(message, "message == null");
    try {
      asyncRequestSemaphore.acquire();
    } catch (InterruptedException e) {
      throw new CompletionException(IOUtils.toInterruptedIOException(
          "Interrupted when sending " + message, e));
    }
    final long callId = nextCallId();
    return sendRequestWithRetryAsync(
        () -> new RaftClientRequest(clientId, leaderId, groupId, callId, message, readOnly)
    ).thenApply(reply -> {
      if (reply.hasStateMachineException() || reply.hasGroupMismatchException()) {
        throw new CompletionException(reply.getException());
      }
      return reply;
    }).whenComplete((r, e) -> asyncRequestSemaphore.release());
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
    Objects.requireNonNull(message, "message == null");

    final long callId = nextCallId();
    return sendRequestWithRetry(() -> new RaftClientRequest(
        clientId, leaderId, groupId, callId, message, readOnly));
  }

  @Override
  public RaftClientReply setConfiguration(RaftPeer[] peersInNewConf)
      throws IOException {
    Objects.requireNonNull(peersInNewConf, "peersInNewConf == null");

    final long callId = nextCallId();
    // also refresh the rpc proxies for these peers
    addServers(Arrays.stream(peersInNewConf));
    return sendRequestWithRetry(() -> new SetConfigurationRequest(
        clientId, leaderId, groupId, callId, peersInNewConf));
  }

  @Override
  public RaftClientReply reinitialize(RaftGroup newGroup, RaftPeerId server)
      throws IOException {
    Objects.requireNonNull(newGroup, "newGroup == null");
    Objects.requireNonNull(server, "server == null");

    final long callId = nextCallId();
    addServers(newGroup.getPeers().stream());
    return sendRequest(new ReinitializeRequest(
        clientId, server, groupId, callId, newGroup));
  }

  @Override
  public RaftClientReply serverInformation(RaftPeerId server)
      throws IOException {
    Objects.requireNonNull(server, "server == null");

    return sendRequest(new ServerInformatonRequest(clientId, server,
        groupId, nextCallId()));
  }

  private void addServers(Stream<RaftPeer> peersInNewConf) {
    clientRpc.addServers(
        peersInNewConf.filter(p -> !peers.contains(p))::iterator);
  }

  private CompletableFuture<RaftClientReply> sendRequestWithRetryAsync(
      Supplier<RaftClientRequest> supplier) {
    return sendRequestAsync(supplier.get()).thenComposeAsync(reply -> {
      final CompletableFuture<RaftClientReply> f = new CompletableFuture<>();
      if (reply == null) {
        final TimeUnit unit = retryInterval.getUnit();
        scheduler.schedule(() -> sendRequestWithRetryAsync(supplier)
            .thenApply(r -> f.complete(r)), retryInterval.toLong(unit), unit);
      } else {
        f.complete(reply);
      }
      return f;
    });
  }

  private RaftClientReply sendRequestWithRetry(
      Supplier<RaftClientRequest> supplier)
      throws InterruptedIOException, StateMachineException, GroupMismatchException {
    for(;;) {
      final RaftClientRequest request = supplier.get();
      final RaftClientReply reply = sendRequest(request);
      if (reply != null) {
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

  private CompletableFuture<RaftClientReply> sendRequestAsync(
      RaftClientRequest request) {
    LOG.debug("{}: sendAsync {}", clientId, request);
    return clientRpc.sendRequestAsync(request).thenApply(reply -> {
      LOG.debug("{}: receive {}", clientId, reply);
      if (reply != null && reply.isNotLeader()) {
        handleNotLeaderException(request, reply.getNotLeaderException());
        return null;
      }
      return reply;
    }).exceptionally(e -> {
      final Throwable cause = e.getCause();
      if (cause instanceof GroupMismatchException) {
        return new RaftClientReply(request, (RaftException) cause);
      } else if (cause instanceof IOException) {
        handleIOException(request, (IOException) cause, null);
      }
      return null;
    });
  }

  private RaftClientReply sendRequest(RaftClientRequest request)
      throws StateMachineException, GroupMismatchException {
    LOG.debug("{}: send {}", clientId, request);
    RaftClientReply reply = null;
    try {
      reply = clientRpc.sendRequest(request);
    } catch (GroupMismatchException gme) {
      throw gme;
    } catch (IOException ioe) {
      handleIOException(request, ioe, null);
    }
    if (reply != null) {
      LOG.debug("{}: receive {}", clientId, reply);
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
    if (LOG.isTraceEnabled()) {
      LOG.trace("Stack trace", new Throwable("TRACE"));
    }

    final RaftPeerId oldLeader = request.getServerId();
    clientRpc.handleException(oldLeader, ioe);

    if (newLeader == null && oldLeader.equals(leaderId)) {
      newLeader = CollectionUtils.random(oldLeader,
          CollectionUtils.as(peers, RaftPeer::getId));
    }
    if (newLeader != null && oldLeader.equals(leaderId)) {
      LOG.debug("{}: change Leader from {} to {}", clientId, oldLeader, newLeader);
      this.leaderId = newLeader;
    }
  }

  void assertAsyncRequestSemaphore(int expectedAvailablePermits, int expectedQueueLength) {
    Preconditions.assertTrue(asyncRequestSemaphore.availablePermits() == expectedAvailablePermits);
    Preconditions.assertTrue(asyncRequestSemaphore.getQueueLength() == expectedQueueLength);
  }

  void assertScheduler(int numThreads) {
    Preconditions.assertTrue(((ScheduledThreadPoolExecutor) scheduler).getCorePoolSize() == numThreads);
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
