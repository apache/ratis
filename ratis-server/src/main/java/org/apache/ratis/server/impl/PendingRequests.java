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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.*;
import org.apache.ratis.server.impl.RetryCache.CacheEntry;
import org.apache.ratis.shaded.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

class PendingRequests {
  public static final Logger LOG = LoggerFactory.getLogger(PendingRequests.class);

  private static class RequestMap {
    private final Object name;
    private final ConcurrentMap<Long, PendingRequest> map = new ConcurrentHashMap<>();

    RequestMap(Object name) {
      this.name = name;
    }

    void put(long index, PendingRequest p) {
      LOG.debug("{}: PendingRequests.put {} -> {}", name, index, p);
      final PendingRequest previous = map.put(index, p);
      Preconditions.assertTrue(previous == null);
    }

    PendingRequest get(long index) {
      final PendingRequest r = map.get(index);
      LOG.debug("{}: PendingRequests.get {} returns {}", name, index, r);
      return r;
    }

    PendingRequest remove(long index) {
      final PendingRequest r = map.remove(index);
      LOG.debug("{}: PendingRequests.remove {} returns {}", name, index, r);
      return r;
    }

    Collection<TransactionContext> setNotLeaderException(NotLeaderException nle) {
      LOG.debug("{}: PendingRequests.setNotLeaderException", name);
      try {
        return map.values().stream()
            .map(p -> p.setNotLeaderException(nle))
            .collect(Collectors.toList());
      } finally {
        map.clear();
      }
    }
  }

  private static class DelayedReplies {
    private final String name;
    private final PriorityQueue<PendingRequest> q = new PriorityQueue<>();
    private AtomicLong allAckedIndex = new AtomicLong();

    private DelayedReplies(Object name) {
      this.name = name + "-" + getClass().getSimpleName();
    }

    boolean delay(PendingRequest request, RaftClientReply reply, CacheEntry cacheEntry) {
      if (request.getIndex() <= allAckedIndex.get()) {
        return false; // delay is not required.
      }

      LOG.debug("{}: delay request {}", name, request);
      request.setDelayedReply(reply, cacheEntry);
      final boolean offered;
      synchronized (q) {
        offered = q.offer(request);
      }
      Preconditions.assertTrue(offered);
      return true;
    }

    void update(final long allAcked) {
      final long old = allAckedIndex.getAndUpdate(n -> allAcked > n? allAcked : n);
      if (allAcked <= old) {
        return;
      }

      LOG.debug("{}: update allAckedIndex {} -> {}", name, old, allAcked);
      for(;;) {
        final PendingRequest polled;
        synchronized (q) {
          final PendingRequest peeked = q.peek();
          if (peeked == null || peeked.getIndex() > allAcked) {
            return;
          }
          polled = q.poll();
          Preconditions.assertTrue(polled == peeked);
        }
        LOG.debug("{}: complete delay request {}", name, polled);
        polled.completeDelayedReply();
      }
    }

    void failReplies() {
      synchronized (q) {
        for(; !q.isEmpty();) {
          q.poll().failDelayedReply();
        }
      }
    }
  }

  private PendingRequest pendingSetConf;
  private final RaftServerImpl server;
  private final RequestMap pendingRequests;
  private PendingRequest last = null;

  private final DelayedReplies delayedReplies;

  PendingRequests(RaftServerImpl server) {
    this.server = server;
    this.pendingRequests = new RequestMap(server.getId());
    this.delayedReplies = new DelayedReplies(server.getId());
  }

  PendingRequest addPendingRequest(long index, RaftClientRequest request,
      TransactionContext entry) {
    // externally synced for now
    Preconditions.assertTrue(request.is(RaftClientRequestProto.TypeCase.WRITE));
    if (last != null && !(last.getRequest() instanceof SetConfigurationRequest)) {
      Preconditions.assertTrue(index == last.getIndex() + 1,
          () -> "index = " + index + " != last.getIndex() + 1, last=" + last);
    }
    return add(index, request, entry);
  }

  private PendingRequest add(long index, RaftClientRequest request,
      TransactionContext entry) {
    final PendingRequest pending = new PendingRequest(index, request, entry);
    pendingRequests.put(index, pending);
    last = pending;
    return pending;
  }

  PendingRequest addConfRequest(SetConfigurationRequest request) {
    Preconditions.assertTrue(pendingSetConf == null);
    pendingSetConf = new PendingRequest(request);
    last = pendingSetConf;
    return pendingSetConf;
  }

  void replySetConfiguration() {
    // we allow the pendingRequest to be null in case that the new leader
    // commits the new configuration while it has not received the retry
    // request from the client
    if (pendingSetConf != null) {
      // for setConfiguration we do not need to wait for statemachine. send back
      // reply after it's committed.
      pendingSetConf.setReply(new RaftClientReply(pendingSetConf.getRequest(), server.getCommitInfos()));
      pendingSetConf = null;
    }
  }

  void failSetConfiguration(RaftException e) {
    Preconditions.assertTrue(pendingSetConf != null);
    pendingSetConf.setException(e);
    pendingSetConf = null;
  }

  TransactionContext getTransactionContext(long index) {
    PendingRequest pendingRequest = pendingRequests.get(index);
    // it is possible that the pendingRequest is null if this peer just becomes
    // the new leader and commits transactions received by the previous leader
    return pendingRequest != null ? pendingRequest.getEntry() : null;
  }

  /** @return true if the request is replied; otherwise, the reply is delayed, return false. */
  boolean replyPendingRequest(long index, RaftClientReply reply, CacheEntry cacheEntry) {
    final PendingRequest pending = pendingRequests.remove(index);
    if (pending != null) {
      Preconditions.assertTrue(pending.getIndex() == index);

      final ReplicationLevel replication = pending.getRequest().getType().getWrite().getReplication();
      if (replication == ReplicationLevel.ALL) {
        if (delayedReplies.delay(pending, reply, cacheEntry)) {
          return false;
        }
      }
      pending.setReply(reply);
    }
    return true;
  }

  /**
   * The leader state is stopped. Send NotLeaderException to all the pending
   * requests since they have not got applied to the state machine yet.
   */
  void sendNotLeaderResponses() throws IOException {
    LOG.info("{} sends responses before shutting down PendingRequestsHandler",
        server.getId());

    // notify the state machine about stepping down
    final NotLeaderException nle = server.generateNotLeaderException();
    server.getStateMachine().notifyNotLeader(pendingRequests.setNotLeaderException(nle));
    if (pendingSetConf != null) {
      pendingSetConf.setNotLeaderException(nle);
    }
    delayedReplies.failReplies();
  }

  void checkDelayedReplies(long allAckedIndex) {
    delayedReplies.update(allAckedIndex);
  }
}
