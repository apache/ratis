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

import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.proto.RaftProtos.WatchRequestTypeProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;

class WatchRequests {
  public static final Logger LOG = LoggerFactory.getLogger(WatchRequests.class);

  static class PendingWatch {
    private final WatchRequestTypeProto watch;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    PendingWatch(WatchRequestTypeProto watch) {
      this.watch = watch;
    }

    CompletableFuture<Void> getFuture() {
      return future;
    }

    long getIndex() {
      return watch.getIndex();
    }

    @Override
    public String toString() {
      return RaftClientRequest.Type.toString(watch);
    }
  }

  private class WatchQueue {
    private final ReplicationLevel replication;
    private final PriorityQueue<PendingWatch> q = new PriorityQueue<>(Comparator.comparing(PendingWatch::getIndex));
    private volatile long index; //Invariant: q.isEmpty() or index < any element q

    WatchQueue(ReplicationLevel replication) {
      this.replication = replication;
    }

    long getIndex() {
      return index;
    }

    synchronized boolean offer(PendingWatch pending) {
      if (pending.getIndex() > getIndex()) { // compare again synchronized
        final boolean offered = q.offer(pending);
        Preconditions.assertTrue(offered);
        return true;
      }
      return false;
    }

    synchronized void updateIndex(final long newIndex) {
      if (newIndex <= getIndex()) { // compare again synchronized
        return;
      }
      LOG.debug("{}: update {} index from {} to {}", name, replication, index, newIndex);
      index = newIndex;

      for(;;) {
        final PendingWatch peeked = q.peek();
        if (peeked == null || peeked.getIndex() > newIndex) {
          return;
        }
        final PendingWatch polled = q.poll();
        Preconditions.assertTrue(polled == peeked);
        LOG.debug("{}: complete {}", name, polled);
        polled.getFuture().complete(null);
      }
    }

    synchronized void failAll(Exception e) {
      for(; !q.isEmpty(); ) {
        q.poll().getFuture().completeExceptionally(e);
      }
    }
  }

  private final String name;
  private final Map<ReplicationLevel, WatchQueue> queues = new EnumMap<>(ReplicationLevel.class);

  WatchRequests(Object name) {
    this.name = name + "-" + getClass().getSimpleName();
    Arrays.stream(ReplicationLevel.values()).forEach(r -> queues.put(r, new WatchQueue(r)));
  }

  CompletableFuture<Void> add(WatchRequestTypeProto watch) {
    final WatchQueue queue = queues.get(watch.getReplication());
    if (watch.getIndex() > queue.getIndex()) { // compare without synchronization
      final PendingWatch pending = new PendingWatch(watch);
      if (queue.offer(pending)) {
        return pending.getFuture();
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  void update(ReplicationLevel replication, final long newIndex) {
    final WatchQueue queue = queues.get(replication);
    if (newIndex > queue.getIndex()) { // compare without synchronization
      queue.updateIndex(newIndex);
    }
  }

  void failWatches(Exception e) {
    queues.values().forEach(q -> q.failAll(e));
  }
}
