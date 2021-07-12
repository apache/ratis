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
package org.apache.ratis.server.impl;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ResourceSemaphore;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class PendingRequests {
  public static final Logger LOG = LoggerFactory.getLogger(PendingRequests.class);

  private static final int ONE_MB = SizeInBytes.ONE_MB.getSizeInt();

  /**
   * Round up to the nearest MB.
   */
  static int roundUpMb(long bytes) {
    return Math.toIntExact((bytes - 1) / ONE_MB + 1);
  }

  static class Permit {}

  static class RequestLimits extends ResourceSemaphore.Group {
    RequestLimits(int elementLimit, int megabyteLimit) {
      super(elementLimit, megabyteLimit);
    }

    int getElementCount() {
      return get(0).used();
    }

    int getMegaByteSize() {
      return get(1).used();
    }

    ResourceSemaphore.ResourceAcquireStatus tryAcquire(int messageSizeMb) {
      return tryAcquire(1, messageSizeMb);
    }

    void releaseExtraMb(int extraMb) {
      release(0, extraMb);
    }

    void release(int diffMb) {
      release(1, diffMb);
    }
  }

  private static class RequestMap {
    private final Object name;
    private final ConcurrentMap<Long, PendingRequest> map = new ConcurrentHashMap<>();
    private final RaftServerMetricsImpl raftServerMetrics;

    /** Permits to put new requests, always synchronized. */
    private final Map<Permit, Permit> permits = new HashMap<>();
    /** Track and limit the number of requests and the total message size. */
    private final RequestLimits resource;
    /** The size (in byte) of all the requests in this map. */
    private final AtomicLong requestSize = new AtomicLong();


    RequestMap(Object name, int elementLimit, int megabyteLimit, RaftServerMetricsImpl raftServerMetrics) {
      this.name = name;
      this.resource = new RequestLimits(elementLimit, megabyteLimit);
      this.raftServerMetrics = raftServerMetrics;

      raftServerMetrics.addNumPendingRequestsGauge(resource::getElementCount);
      raftServerMetrics.addNumPendingRequestsMegaByteSize(resource::getMegaByteSize);
    }

    Permit tryAcquire(Message message) {
      final int messageSize = Message.getSize(message);
      final int messageSizeMb = roundUpMb(messageSize );
      final ResourceSemaphore.ResourceAcquireStatus acquired = resource.tryAcquire(messageSizeMb);
      LOG.trace("tryAcquire {} MB? {}", messageSizeMb, acquired);
      if (acquired == ResourceSemaphore.ResourceAcquireStatus.FAILED_IN_ELEMENT_LIMIT) {
        raftServerMetrics.onRequestQueueLimitHit();
        raftServerMetrics.onResourceLimitHit();
        return null;
      } else if (acquired == ResourceSemaphore.ResourceAcquireStatus.FAILED_IN_BYTE_SIZE_LIMIT) {
        raftServerMetrics.onRequestByteSizeLimitHit();
        raftServerMetrics.onResourceLimitHit();
        return null;
      }

      // release extra MB
      final long oldSize = requestSize.getAndAdd(messageSize);
      final long newSize = oldSize + messageSize;
      final int diffMb = roundUpMb(newSize) - roundUpMb(oldSize);
      if (messageSizeMb > diffMb) {
        resource.releaseExtraMb(messageSizeMb - diffMb);
      }
      return putPermit();
    }

    private synchronized Permit putPermit() {
      if (resource.isClosed()) {
        return null;
      }
      final Permit permit = new Permit();
      permits.put(permit, permit);
      return permit;
    }

    synchronized PendingRequest put(Permit permit, long index, PendingRequest p) {
      LOG.debug("{}: PendingRequests.put {} -> {}", name, index, p);
      final Permit removed = permits.remove(permit);
      if (removed == null) {
        return null;
      }
      Preconditions.assertTrue(removed == permit);
      final PendingRequest previous = map.put(index, p);
      Preconditions.assertTrue(previous == null);
      return p;
    }

    PendingRequest get(long index) {
      final PendingRequest r = map.get(index);
      LOG.debug("{}: PendingRequests.get {} returns {}", name, index, r);
      return r;
    }

    PendingRequest remove(long index) {
      final PendingRequest r = map.remove(index);
      LOG.debug("{}: PendingRequests.remove {} returns {}", name, index, r);
      if (r == null) {
        return null;
      }
      final int messageSize = Message.getSize(r.getRequest().getMessage());
      final long oldSize = requestSize.getAndAdd(-messageSize);
      final long newSize = oldSize - messageSize;
      final int diffMb = roundUpMb(oldSize) - roundUpMb(newSize);
      resource.release(diffMb);
      LOG.trace("release {} MB", diffMb);
      return r;
    }

    Collection<TransactionContext> setNotLeaderException(NotLeaderException nle,
                                                         Collection<CommitInfoProto> commitInfos) {
      synchronized (this) {
        resource.close();
        permits.clear();
      }

      LOG.debug("{}: PendingRequests.setNotLeaderException", name);
      final List<TransactionContext> transactions = new ArrayList<>(map.size());
      for(;;) {
        final Iterator<Long> i = map.keySet().iterator();
        if (!i.hasNext()) { // the map is empty
          return transactions;
        }

        final PendingRequest pending = map.remove(i.next());
        if (pending != null) {
          transactions.add(pending.setNotLeaderException(nle, commitInfos));
        }
      }
    }

    void close() {
      if (raftServerMetrics != null) {
        raftServerMetrics.removeNumPendingRequestsGauge();
        raftServerMetrics.removeNumPendingRequestsByteSize();
      }
    }
  }

  private PendingRequest pendingSetConf;
  private final String name;
  private final RequestMap pendingRequests;

  PendingRequests(RaftGroupMemberId id, RaftProperties properties, RaftServerMetricsImpl raftServerMetrics) {
    this.name = id + "-" + JavaUtils.getClassSimpleName(getClass());
    this.pendingRequests = new RequestMap(id,
        RaftServerConfigKeys.Write.elementLimit(properties),
        Math.toIntExact(
            RaftServerConfigKeys.Write.byteLimit(properties).getSize()
                / SizeInBytes.ONE_MB.getSize()), //round down
        raftServerMetrics);
  }

  Permit tryAcquire(Message message) {
    return pendingRequests.tryAcquire(message);
  }

  PendingRequest add(Permit permit, RaftClientRequest request, TransactionContext entry) {
    // externally synced for now
    final long index = entry.getLogEntry().getIndex();
    LOG.debug("{}: addPendingRequest at index={}, request={}", name, index, request);
    final PendingRequest pending = new PendingRequest(index, request, entry);
    return pendingRequests.put(permit, index, pending);
  }

  PendingRequest addConfRequest(SetConfigurationRequest request) {
    Preconditions.assertTrue(pendingSetConf == null);
    pendingSetConf = new PendingRequest(request);
    return pendingSetConf;
  }

  void replySetConfiguration(Function<RaftClientRequest, RaftClientReply> newSuccessReply) {
    // we allow the pendingRequest to be null in case that the new leader
    // commits the new configuration while it has not received the retry
    // request from the client
    if (pendingSetConf != null) {
      final RaftClientRequest request = pendingSetConf.getRequest();
      LOG.debug("{}: sends success for {}", name, request);
      // for setConfiguration we do not need to wait for statemachine. send back
      // reply after it's committed.
      pendingSetConf.setReply(newSuccessReply.apply(request));
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

  void replyPendingRequest(long index, RaftClientReply reply) {
    final PendingRequest pending = pendingRequests.remove(index);
    if (pending != null) {
      Preconditions.assertTrue(pending.getIndex() == index);
      pending.setReply(reply);
    }
  }

  /**
   * The leader state is stopped. Send NotLeaderException to all the pending
   * requests since they have not got applied to the state machine yet.
   */
  Collection<TransactionContext> sendNotLeaderResponses(NotLeaderException nle,
                                                        Collection<CommitInfoProto> commitInfos) {
    LOG.info("{}: sendNotLeaderResponses", name);

    final Collection<TransactionContext> transactions = pendingRequests.setNotLeaderException(nle, commitInfos);
    if (pendingSetConf != null) {
      pendingSetConf.setNotLeaderException(nle, commitInfos);
    }
    return transactions;
  }

  void close() {
    if (pendingRequests != null) {
      pendingRequests.close();
    }
  }
}
