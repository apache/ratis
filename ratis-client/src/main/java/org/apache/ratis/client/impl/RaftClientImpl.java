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

import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.client.api.LeaderElectionManagementApi;
import org.apache.ratis.client.api.SnapshotManagementApi;
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
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.thirdparty.com.google.common.cache.Cache;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** A client who sends requests to a raft service. */
public final class RaftClientImpl implements RaftClient {
  private static final Cache<RaftGroupId, RaftPeerId> LEADER_CACHE = CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.SECONDS)
      .maximumSize(1024)
      .build();

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

  static class RaftPeerList implements Iterable<RaftPeer> {
    private final AtomicReference<List<RaftPeer>> list = new AtomicReference<>();

    @Override
    public Iterator<RaftPeer> iterator() {
      return list.get().iterator();
    }

    void set(Collection<RaftPeer> newPeers) {
      list.set(Collections.unmodifiableList(new ArrayList<>(newPeers)));
    }
  }

  private final ClientId clientId;
  private final RaftClientRpc clientRpc;
  private final RaftPeerList peers = new RaftPeerList();
  private final RaftGroupId groupId;
  private final RetryPolicy retryPolicy;

  private volatile RaftPeerId leaderId;

  private final TimeoutScheduler scheduler = TimeoutScheduler.getInstance();

  private final Supplier<OrderedAsync> orderedAsync;
  private final Supplier<AsyncImpl> asyncApi;
  private final Supplier<BlockingImpl> blockingApi;
  private final Supplier<MessageStreamImpl> messageStreamApi;
  private final MemoizedSupplier<DataStreamApi> dataStreamApi;

  private final Supplier<AdminImpl> adminApi;
  private final ConcurrentMap<RaftPeerId, GroupManagementImpl> groupManagement = new ConcurrentHashMap<>();
  private final ConcurrentMap<RaftPeerId, SnapshotManagementApi> snapshotManagement = new ConcurrentHashMap<>();
  private final ConcurrentMap<RaftPeerId, LeaderElectionManagementApi>
      leaderElectionManagement = new ConcurrentHashMap<>();

  RaftClientImpl(ClientId clientId, RaftGroup group, RaftPeerId leaderId, RaftPeer primaryDataStreamServer,
      RaftClientRpc clientRpc, RaftProperties properties, RetryPolicy retryPolicy) {
    this.clientId = clientId;
    this.peers.set(group.getPeers());
    this.groupId = group.getGroupId();

    if (leaderId == null) {
      final RaftPeerId cached = LEADER_CACHE.getIfPresent(groupId);
      if (cached != null && group.getPeer(cached) != null) {
        leaderId = cached;
      }
    }
    this.leaderId = leaderId != null? leaderId : getHighestPriorityPeerId();
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retry policy can't be null");

    clientRpc.addRaftPeers(group.getPeers());
    this.clientRpc = clientRpc;

    this.orderedAsync = JavaUtils.memoize(() -> OrderedAsync.newInstance(this, properties));
    this.messageStreamApi = JavaUtils.memoize(() -> MessageStreamImpl.newInstance(this, properties));
    this.asyncApi = JavaUtils.memoize(() -> new AsyncImpl(this));
    this.blockingApi = JavaUtils.memoize(() -> new BlockingImpl(this));
    this.dataStreamApi = JavaUtils.memoize(() -> DataStreamClient.newBuilder()
        .setClientId(clientId)
        .setRaftGroupId(groupId)
        .setDataStreamServer(primaryDataStreamServer)
        .setProperties(properties)
        .build());
    this.adminApi = JavaUtils.memoize(() -> new AdminImpl(this));
  }

  public RaftPeerId getLeaderId() {
    return leaderId;
  }

  RaftGroupId getGroupId() {
    return groupId;
  }

  private RaftPeerId getHighestPriorityPeerId() {
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

  RaftClientRequest newRaftClientRequest(
      RaftPeerId server, long callId, Message message, RaftClientRequest.Type type,
      SlidingWindowEntry slidingWindowEntry) {
    final RaftClientRequest.Builder b = RaftClientRequest.newBuilder();
    if (server != null) {
      b.setServerId(server);
    } else {
      b.setLeaderId(leaderId);
    }
    return b.setClientId(clientId)
        .setGroupId(groupId)
        .setCallId(callId)
        .setMessage(message)
        .setType(type)
        .setSlidingWindowEntry(slidingWindowEntry)
        .build();
  }

  @Override
  public AdminImpl admin() {
    return adminApi.get();
  }

  @Override
  public GroupManagementImpl getGroupManagementApi(RaftPeerId server) {
    return groupManagement.computeIfAbsent(server, id -> new GroupManagementImpl(id, this));
  }

  @Override
  public SnapshotManagementApi getSnapshotManagementApi() {
    return JavaUtils.memoize(() -> new SnapshotManagementImpl(null, this)).get();
  }

  @Override
  public SnapshotManagementApi getSnapshotManagementApi(RaftPeerId server) {
    return snapshotManagement.computeIfAbsent(server, id -> new SnapshotManagementImpl(id, this));
  }

  @Override
  public LeaderElectionManagementApi getLeaderElectionManagementApi(RaftPeerId server) {
    return leaderElectionManagement.computeIfAbsent(server, id -> new LeaderElectionManagementImpl(id, this));
  }

  @Override
  public BlockingImpl io() {
    return blockingApi.get();
  }

  @Override
  public AsyncImpl async() {
    return asyncApi.get();
  }

  @Override
  public MessageStreamImpl getMessageStreamApi() {
    return messageStreamApi.get();
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

  RaftClientReply handleReply(RaftClientRequest request, RaftClientReply reply) {
    if (request.isToLeader() && reply != null && reply.getException() == null) {
      LEADER_CACHE.put(reply.getRaftGroupId(), reply.getServerId());
    }
    return reply;
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
      peers.set(newPeers);
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

  @Override
  public RaftClientRpc getClientRpc() {
    return clientRpc;
  }

  @Override
  public void close() throws IOException {
    scheduler.close();
    clientRpc.close();
    if (dataStreamApi.isInitialized()) {
      dataStreamApi.get().close();
    }
  }
}
