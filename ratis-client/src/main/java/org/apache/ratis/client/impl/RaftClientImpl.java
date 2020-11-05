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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.client.api.MessageStreamApi;
import org.apache.ratis.client.retry.ClientRetryEvent;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.SlidingWindowEntry;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** A client who sends requests to a raft service. */
public final class RaftClientImpl implements RaftClient {
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  public abstract static class PendingClientRequest {
    private final long creationTimeInMs = System.currentTimeMillis();
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
    private final AtomicInteger attemptCount = new AtomicInteger();
    private final Map<Class<?>, Integer> exceptionCount = new ConcurrentHashMap<>();

    public abstract RaftClientRequest newRequestImpl();

    final RaftClientRequest newRequest() {
      attemptCount.incrementAndGet();
      return newRequestImpl();
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    public int getAttemptCount() {
      return attemptCount.get();
    }

    int incrementExceptionCount(Throwable t) {
      return t != null ? exceptionCount.compute(t.getClass(), (k, v) -> v != null ? v + 1 : 1) : 0;
    }

    public int getExceptionCount(Throwable t) {
      return t != null ? Optional.ofNullable(exceptionCount.get(t.getClass())).orElse(0) : 0;
    }

    public boolean isRequestTimeout(TimeDuration timeout) {
      if (timeout == null) {
        return false;
      }
      return System.currentTimeMillis() - creationTimeInMs > timeout.toLong(TimeUnit.MILLISECONDS);
    }
  }

  private final ClientId clientId;
  private final RaftClientRpc clientRpc;
  private final Collection<RaftPeer> peers;
  private final RaftGroupId groupId;
  private final RetryPolicy retryPolicy;

  private volatile RaftPeerId leaderId;

  private final TimeoutScheduler scheduler;

  private final Supplier<OrderedAsync> orderedAsync;
  private final Supplier<MessageStreamApi> streamApi;
  private final Supplier<AsyncImpl> asyncApi;
  private final Supplier<BlockingImpl> blockingApi;
  private final Supplier<DataStreamApi> dataStreamApi;

  RaftClientImpl(ClientId clientId, RaftGroup group, RaftPeerId leaderId, RaftPeer primaryDataStreamServer,
      RaftClientRpc clientRpc, RaftProperties properties, RetryPolicy retryPolicy) {
    this.clientId = clientId;
    this.clientRpc = clientRpc;
    this.peers = new ConcurrentLinkedQueue<>(group.getPeers());
    this.groupId = group.getGroupId();
    this.leaderId = leaderId != null? leaderId : getHighestPriorityPeerId();
    Preconditions.assertTrue(retryPolicy != null, "retry policy can't be null");
    this.retryPolicy = retryPolicy;

    scheduler = TimeoutScheduler.getInstance();
    clientRpc.addRaftPeers(peers);

    this.orderedAsync = JavaUtils.memoize(() -> OrderedAsync.newInstance(this, properties));
    this.streamApi = JavaUtils.memoize(() -> MessageStreamImpl.newInstance(this, properties));
    this.asyncApi = JavaUtils.memoize(() -> new AsyncImpl(this));
    this.blockingApi = JavaUtils.memoize(() -> new BlockingImpl(this));
    this.dataStreamApi = JavaUtils.memoize(
        () ->new DataStreamClientImpl(clientId, groupId, primaryDataStreamServer, properties, null));
  }

  public RaftPeerId getLeaderId() {
    return leaderId;
  }

  RaftGroupId getGroupId() {
    return groupId;
  }

  private RaftPeerId getHighestPriorityPeerId() {
    if (peers == null) {
      return null;
    }

    int maxPriority = Integer.MIN_VALUE;
    RaftPeerId highestPriorityPeerId = null;
    for (RaftPeer peer : peers) {
      if (maxPriority < peer.getPriority()) {
        maxPriority = peer.getPriority();
        highestPriorityPeerId = peer.getId();
      }
    }

    return highestPriorityPeerId;
  }

  @Override
  public ClientId getId() {
    return clientId;
  }

  RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  TimeDuration getEffectiveSleepTime(Throwable t, TimeDuration sleepDefault) {
    return t instanceof NotLeaderException && ((NotLeaderException) t).getSuggestedLeader() != null ?
        TimeDuration.ZERO : sleepDefault;
  }

  TimeoutScheduler getScheduler() {
    return scheduler;
  }

  OrderedAsync getOrderedAsync() {
    return orderedAsync.get();
  }

  @Override
  public MessageStreamApi getMessageStreamApi() {
    return streamApi.get();
  }

  RaftClientRequest newRaftClientRequest(
      RaftPeerId server, long callId, Message message, RaftClientRequest.Type type,
      SlidingWindowEntry slidingWindowEntry) {
    return new RaftClientRequest(clientId, server != null? server: leaderId, groupId,
        callId, message, type, slidingWindowEntry);
  }


  // TODO: change peersInNewConf to List<RaftPeer>
  @Override
  public RaftClientReply setConfiguration(RaftPeer[] peersInNewConf)
      throws IOException {
    Objects.requireNonNull(peersInNewConf, "peersInNewConf == null");

    final long callId = nextCallId();
    // also refresh the rpc proxies for these peers
    clientRpc.addRaftPeers(peersInNewConf);
    return io().sendRequestWithRetry(() -> new SetConfigurationRequest(
        clientId, leaderId, groupId, callId, Arrays.asList(peersInNewConf)));
  }

  @Override
  public AsyncImpl async() {
    return asyncApi.get();
  }

  @Override
  public GroupManagementApi getGroupManagementApi(RaftPeerId server) {
    return new GroupManagementImpl(server, this);
  }

  @Override
  public BlockingImpl io() {
    return blockingApi.get();
  }

  @Override
  public DataStreamApi getDataStreamApi() {
    return dataStreamApi.get();
  }

  Throwable noMoreRetries(ClientRetryEvent event) {
    final int attemptCount = event.getAttemptCount();
    final Throwable throwable = event.getCause();
    if (attemptCount == 1 && throwable != null) {
      return throwable;
    }
    return new RaftRetryFailureException(event.getRequest(), attemptCount, retryPolicy, throwable);
  }

  static <E extends Throwable> RaftClientReply handleRaftException(
      RaftClientReply reply, Function<RaftException, E> converter) throws E {
    if (reply != null) {
      final RaftException e = reply.getException();
      if (e != null) {
        throw converter.apply(e);
      }
    }
    return reply;
  }

  /**
   * @return null if the reply is null or it has
   * {@link NotLeaderException} or {@link LeaderNotReadyException}
   * otherwise return the same reply.
   */
  RaftClientReply handleLeaderException(RaftClientRequest request, RaftClientReply reply) {
    if (reply == null || reply.getException() instanceof LeaderNotReadyException) {
      return null;
    }
    final NotLeaderException nle = reply.getNotLeaderException();
    if (nle == null) {
      return reply;
    }
    return handleNotLeaderException(request, nle, null);
  }

  RaftClientReply handleNotLeaderException(RaftClientRequest request, NotLeaderException nle,
      Consumer<RaftClientRequest> handler) {
    refreshPeers(nle.getPeers());
    final RaftPeerId newLeader = nle.getSuggestedLeader() == null ? null
        : nle.getSuggestedLeader().getId();
    handleIOException(request, nle, newLeader, handler);
    return null;
  }

  private void refreshPeers(Collection<RaftPeer> newPeers) {
    if (newPeers != null && newPeers.size() > 0) {
      peers.clear();
      peers.addAll(newPeers);
      // also refresh the rpc proxies for these peers
      clientRpc.addRaftPeers(newPeers);
    }
  }

  void handleIOException(RaftClientRequest request, IOException ioe) {
    handleIOException(request, ioe, null, null);
  }

  void handleIOException(RaftClientRequest request, IOException ioe,
      RaftPeerId newLeader, Consumer<RaftClientRequest> handler) {
    LOG.debug("{}: suggested new leader: {}. Failed {} with {}",
        clientId, newLeader, request, ioe);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Stack trace", new Throwable("TRACE"));
    }

    Optional.ofNullable(handler).ifPresent(h -> h.accept(request));

    if (ioe instanceof LeaderNotReadyException || ioe instanceof ResourceUnavailableException) {
      return;
    }

    final RaftPeerId oldLeader = request.getServerId();
    final RaftPeerId curLeader = leaderId;
    final boolean stillLeader = oldLeader.equals(curLeader);
    if (newLeader == null && stillLeader) {
      newLeader = CollectionUtils.random(oldLeader,
          CollectionUtils.as(peers, RaftPeer::getId));
    }
    LOG.debug("{}: oldLeader={},  curLeader={}, newLeader={}", clientId, oldLeader, curLeader, newLeader);

    final boolean changeLeader = newLeader != null && stillLeader;
    final boolean reconnect = changeLeader || clientRpc.shouldReconnect(ioe);
    if (reconnect) {
      if (changeLeader && oldLeader.equals(leaderId)) {
        LOG.debug("{} {}: client change Leader from {} to {} ex={}", groupId,
            clientId, oldLeader, newLeader, ioe.getClass().getName());
        this.leaderId = newLeader;
      }
      clientRpc.handleException(oldLeader, ioe, true);
    }
  }

  long getCallId() {
    return CALL_ID_COUNTER.get();
  }

  @Override
  public RaftClientRpc getClientRpc() {
    return clientRpc;
  }

  @Override
  public void close() throws IOException {
    scheduler.close();
    clientRpc.close();
  }
}
