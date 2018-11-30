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
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.util.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.STALEREAD;
import static org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.WATCH;

/** A client who sends requests to a raft service. */
final class RaftClientImpl implements RaftClient {
  private static final AtomicLong callIdCounter = new AtomicLong();

  private static long nextCallId() {
    return callIdCounter.getAndIncrement() & Long.MAX_VALUE;
  }

  static class PendingAsyncRequest implements SlidingWindow.Request<RaftClientReply> {
    private final long seqNum;
    private final LongFunction<RaftClientRequest> requestConstructor;
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
    private volatile int attemptCount;

    PendingAsyncRequest(long seqNum, LongFunction<RaftClientRequest> requestConstructor) {
      this.seqNum = seqNum;
      this.requestConstructor = requestConstructor;
    }

    RaftClientRequest newRequest() {
      attemptCount++;
      return requestConstructor.apply(seqNum);
    }

    @Override
    public long getSeqNum() {
      return seqNum;
    }

    @Override
    public boolean hasReply() {
      return replyFuture.isDone();
    }

    @Override
    public void setReply(RaftClientReply reply) {
      replyFuture.complete(reply);
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    public int getAttemptCount() {
      return attemptCount;
    }

    @Override
    public String toString() {
      return "[seq=" + getSeqNum() + "]";
    }
  }

  private final ClientId clientId;
  private final RaftClientRpc clientRpc;
  private final Collection<RaftPeer> peers;
  private final RaftGroupId groupId;
  private final RetryPolicy retryPolicy;

  private volatile RaftPeerId leaderId;

  /** Map: id -> {@link SlidingWindow}, in order to support async calls to the RAFT service or individual servers. */
  private final ConcurrentMap<String, SlidingWindow.Client<PendingAsyncRequest, RaftClientReply>>
      slidingWindows = new ConcurrentHashMap<>();
  private final TimeoutScheduler scheduler;
  private final Semaphore asyncRequestSemaphore;

  RaftClientImpl(ClientId clientId, RaftGroup group, RaftPeerId leaderId,
      RaftClientRpc clientRpc, RaftProperties properties, RetryPolicy retryPolicy) {
    this.clientId = clientId;
    this.clientRpc = clientRpc;
    this.peers = new ConcurrentLinkedQueue<>(group.getPeers());
    this.groupId = group.getGroupId();
    this.leaderId = leaderId != null? leaderId
        : !peers.isEmpty()? peers.iterator().next().getId(): null;
    Preconditions.assertTrue(retryPolicy != null, "retry policy can't be null");
    this.retryPolicy = retryPolicy;

    asyncRequestSemaphore = new Semaphore(RaftClientConfigKeys.Async.maxOutstandingRequests(properties));
    scheduler = TimeoutScheduler.newInstance(RaftClientConfigKeys.Async.schedulerThreads(properties));
    clientRpc.addServers(peers);
  }

  @Override
  public ClientId getId() {
    return clientId;
  }

  private SlidingWindow.Client<PendingAsyncRequest, RaftClientReply> getSlidingWindow(RaftClientRequest request) {
    return getSlidingWindow(request.is(STALEREAD)? request.getServerId(): null);
  }

  private SlidingWindow.Client<PendingAsyncRequest, RaftClientReply> getSlidingWindow(RaftPeerId target) {
    final String id = target != null? target.toString(): "RAFT";
    return slidingWindows.computeIfAbsent(id, key -> new SlidingWindow.Client<>(getId() + "->" + key));
  }

  @Override
  public CompletableFuture<RaftClientReply> sendAsync(Message message) {
    return sendAsync(RaftClientRequest.writeRequestType(), message, null);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendReadOnlyAsync(Message message) {
    return sendAsync(RaftClientRequest.readRequestType(), message, null);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendStaleReadAsync(Message message, long minIndex, RaftPeerId server) {
    return sendAsync(RaftClientRequest.staleReadRequestType(minIndex), message, server);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendWatchAsync(long index, ReplicationLevel replication) {
    return sendAsync(RaftClientRequest.watchRequestType(index, replication), null, null);
  }

  private CompletableFuture<RaftClientReply> sendAsync(
      RaftClientRequest.Type type, Message message, RaftPeerId server) {
    if (!type.is(WATCH)) {
      Objects.requireNonNull(message, "message == null");
    }
    try {
      asyncRequestSemaphore.acquire();
    } catch (InterruptedException e) {
      throw new CompletionException(IOUtils.toInterruptedIOException(
          "Interrupted when sending " + type + ", message=" + message, e));
    }
    final long callId = nextCallId();
    final LongFunction<PendingAsyncRequest> constructor = seqNum -> new PendingAsyncRequest(seqNum,
        seq -> newRaftClientRequest(server, callId, seq, message, type));
    return getSlidingWindow(server).submitNewRequest(constructor, this::sendRequestWithRetryAsync
    ).getReplyFuture(
    ).thenApply(reply -> handleStateMachineException(reply, CompletionException::new)
    ).whenComplete((r, e) -> asyncRequestSemaphore.release());
  }

  private RaftClientRequest newRaftClientRequest(
      RaftPeerId server, long callId, long seq, Message message, RaftClientRequest.Type type) {
    return new RaftClientRequest(clientId, server != null? server: leaderId, groupId,
        callId, seq, message, type);
  }

  @Override
  public RaftClientReply send(Message message) throws IOException {
    return send(RaftClientRequest.writeRequestType(), message, null);
  }

  @Override
  public RaftClientReply sendReadOnly(Message message) throws IOException {
    return send(RaftClientRequest.readRequestType(), message, null);
  }

  @Override
  public RaftClientReply sendStaleRead(Message message, long minIndex, RaftPeerId server)
      throws IOException {
    return send(RaftClientRequest.staleReadRequestType(minIndex), message, server);
  }

  @Override
  public RaftClientReply sendWatch(long index, ReplicationLevel replication) throws IOException {
    return send(RaftClientRequest.watchRequestType(index, replication), null, null);
  }

  private RaftClientReply send(RaftClientRequest.Type type, Message message, RaftPeerId server)
      throws IOException {
    if (!type.is(WATCH)) {
      Objects.requireNonNull(message, "message == null");
    }

    final long callId = nextCallId();
    return sendRequestWithRetry(() -> newRaftClientRequest(
        server, callId, 0L, message, type));
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
  public RaftClientReply groupAdd(RaftGroup newGroup, RaftPeerId server) throws IOException {
    Objects.requireNonNull(newGroup, "newGroup == null");
    Objects.requireNonNull(server, "server == null");

    final long callId = nextCallId();
    addServers(newGroup.getPeers().stream());
    return sendRequest(GroupManagementRequest.newAdd(clientId, server, callId, newGroup));
  }

  @Override
  public RaftClientReply groupRemove(RaftGroupId groupId, boolean deleteDirectory, RaftPeerId server) throws IOException {
    Objects.requireNonNull(groupId, "groupId == null");
    Objects.requireNonNull(server, "server == null");

    final long callId = nextCallId();
    return sendRequest(GroupManagementRequest.newRemove(clientId, server, callId, groupId, deleteDirectory));
  }

  @Override
  public GroupListReply getGroupList(RaftPeerId server) throws IOException {
    Objects.requireNonNull(server, "server == null");

    final RaftClientReply reply = sendRequest(new GroupListRequest(clientId, server, groupId, nextCallId()));
    Preconditions.assertTrue(reply instanceof GroupListReply, () -> "Unexpected reply: " + reply);
    return (GroupListReply)reply;
  }

  @Override
  public GroupInfoReply getGroupInfo(RaftGroupId raftGroupId, RaftPeerId server) throws IOException {
    Objects.requireNonNull(server, "server == null");
    RaftGroupId rgi = raftGroupId == null ? groupId : raftGroupId;
    final RaftClientReply reply = sendRequest(new GroupInfoRequest(clientId, server, rgi, nextCallId()));
    Preconditions.assertTrue(reply instanceof GroupInfoReply, () -> "Unexpected reply: " + reply);
    return (GroupInfoReply)reply;
  }

  private void addServers(Stream<RaftPeer> peersInNewConf) {
    clientRpc.addServers(
        peersInNewConf.filter(p -> !peers.contains(p))::iterator);
  }

  private CompletableFuture<RaftClientReply> sendRequestWithRetryAsync(
      PendingAsyncRequest pending) {
    final RaftClientRequest request = pending.newRequest();
    final CompletableFuture<RaftClientReply> f = pending.getReplyFuture();
    return sendRequestAsync(request, pending.getAttemptCount()).thenCompose(reply -> {
      if (reply == null) {
        LOG.debug("schedule attempt #{} with policy {} for {}", pending.getAttemptCount(), retryPolicy, request);
        scheduler.onTimeout(retryPolicy.getSleepTime(),
            () -> getSlidingWindow(request).retry(pending, this::sendRequestWithRetryAsync),
            LOG, () -> "Failed to retry " + request);
      } else {
        f.complete(reply);
      }
      return f;
    });
  }

  private RaftClientReply sendRequestWithRetry(
      Supplier<RaftClientRequest> supplier)
      throws InterruptedIOException, StateMachineException, GroupMismatchException {
    for(int attemptCount = 0;; attemptCount++) {
      final RaftClientRequest request = supplier.get();
      final RaftClientReply reply = sendRequest(request);
      if (reply != null) {
        return reply;
      }
      if (!retryPolicy.shouldRetry(attemptCount)) {
        return null;
      }
      try {
        retryPolicy.getSleepTime().sleep();
      } catch (InterruptedException e) {
        throw new InterruptedIOException("retry policy=" + retryPolicy);
      }
    }
  }

  private CompletableFuture<RaftClientReply> sendRequestAsync(
      RaftClientRequest request, int attemptCount) {
    LOG.debug("{}: send* {}", clientId, request);
    return clientRpc.sendRequestAsync(request).thenApply(reply -> {
      LOG.debug("{}: receive* {}", clientId, reply);
      reply = handleNotLeaderException(request, reply);
      if (reply != null) {
        getSlidingWindow(request).receiveReply(
            request.getSeqNum(), reply, this::sendRequestWithRetryAsync);
      } else if (!retryPolicy.shouldRetry(attemptCount)) {
        return handleAsyncRetry(request, attemptCount);
      }
      return reply;
    }).exceptionally(e -> {
      if (LOG.isTraceEnabled()) {
        LOG.trace(clientId + ": Failed " + request, e);
      } else {
        LOG.debug("{}: Failed {} with {}", clientId, request, e);
      }
      e = JavaUtils.unwrapCompletionException(e);
      if (e instanceof GroupMismatchException) {
        throw new CompletionException(e);
      } else if (e instanceof IOException) {
        // once the retryLimit is hit, just remove the request from the
        // sliding window and throw an exception. The exception thrown here will
        // make sure its not retried any more with sendRequestWithRetryAsync call.
        if (!retryPolicy.shouldRetry(attemptCount)) {
          return handleAsyncRetry(request, attemptCount);
        }
        handleIOException(request, (IOException)e, null);
      } else {
        throw new CompletionException(e);
      }
      return null;
    });
  }

  private RaftClientReply handleAsyncRetry(RaftClientRequest request, int attemptCount) {
    RaftClientReply reply = new RaftClientReply(request,
        new RaftRetryFailureException(
            "Failed " + request + " for " + attemptCount + " attempts with " + retryPolicy), null);
    getSlidingWindow(request).receiveReply(
        request.getSeqNum(), reply, this::sendRequestWithRetryAsync);
    return reply;
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
    LOG.debug("{}: receive {}", clientId, reply);
    reply = handleNotLeaderException(request, reply);
    reply = handleStateMachineException(reply, Function.identity());
    return reply;
  }

  static <E extends Throwable> RaftClientReply handleStateMachineException(
      RaftClientReply reply, Function<StateMachineException, E> converter) throws E {
    if (reply != null) {
      final StateMachineException sme = reply.getStateMachineException();
      if (sme != null) {
        throw converter.apply(sme);
      }
    }
    return reply;
  }

  /**
   * @return null if the reply is null or it has {@link NotLeaderException};
   *         otherwise return the same reply.
   */
  private RaftClientReply handleNotLeaderException(RaftClientRequest request, RaftClientReply reply) {
    if (reply == null) {
      return null;
    }
    final NotLeaderException nle = reply.getNotLeaderException();
    if (nle == null) {
      return reply;
    }
    refreshPeers(Arrays.asList(nle.getPeers()));
    final RaftPeerId newLeader = nle.getSuggestedLeader() == null ? null
        : nle.getSuggestedLeader().getId();
    handleIOException(request, nle, newLeader);
    return null;
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
    LOG.debug("{}: suggested new leader: {}. Failed {} with {}",
        clientId, newLeader, request, ioe);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Stack trace", new Throwable("TRACE"));
    }

    getSlidingWindow(request).resetFirstSeqNum();
    if (ioe instanceof LeaderNotReadyException) {
      return;
    }

    final RaftPeerId oldLeader = request.getServerId();
    final boolean stillLeader = oldLeader.equals(leaderId);
    if (newLeader == null && stillLeader) {
      newLeader = CollectionUtils.random(oldLeader,
          CollectionUtils.as(peers, RaftPeer::getId));
    }

    final boolean changeLeader = newLeader != null && stillLeader;
    if (changeLeader) {
      LOG.debug("{}: change Leader from {} to {}", clientId, oldLeader, newLeader);
      this.leaderId = newLeader;
    }
    clientRpc.handleException(oldLeader, ioe, changeLeader);
  }

  void assertAsyncRequestSemaphore(int expectedAvailablePermits, int expectedQueueLength) {
    Preconditions.assertTrue(asyncRequestSemaphore.availablePermits() == expectedAvailablePermits);
    Preconditions.assertTrue(asyncRequestSemaphore.getQueueLength() == expectedQueueLength);
  }

  void assertScheduler(int numThreads) {
    Preconditions.assertTrue(scheduler.getNumThreads() == numThreads);
  }

  long getCallId() {
    return callIdCounter.get();
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
