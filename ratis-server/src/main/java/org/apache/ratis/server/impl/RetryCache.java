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

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.shaded.com.google.common.cache.Cache;
import org.apache.ratis.shaded.com.google.common.cache.CacheBuilder;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryCache implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(RetryCache.class);
  private static final int MIN_CAPACITY = 128;

  static class CacheKey {
    private final ClientId clientId;
    private final long callId;

    CacheKey(ClientId clientId, long callId) {
      this.clientId = clientId;
      this.callId = callId;
    }

    @Override
    public int hashCode() {
      return clientId.hashCode() ^ Long.hashCode(callId);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof CacheKey) {
        CacheKey e = (CacheKey) obj;
        return e.clientId.equals(clientId) && callId == e.callId;
      }
      return false;
    }

    @Override
    public String toString() {
      return clientId.toString() + ":" + this.callId;
    }
  }

  /**
   * CacheEntry is tracked using unique client ID and callId of the RPC request
   */
  @VisibleForTesting
  public static class CacheEntry {
    private final CacheKey key;
    private final CompletableFuture<RaftClientReply> replyFuture =
        new CompletableFuture<>();

    /**
     * "failed" means we failed to commit the request into the raft group, or
     * the request did not get approved by the state machine before the raft
     * replication. Not once the request gets committed by the raft group, this
     * field is never true even if the state machine throws an exception when
     * applying the transaction.
     */
    private volatile boolean failed = false;

    CacheEntry(CacheKey key) {
      this.key = key;
    }

    @Override
    public String toString() {
      return key + ":" + (isDone() ? "done" : "pending");
    }

    boolean isDone() {
      return isFailed() || replyFuture.isDone();
    }

    void updateResult(RaftClientReply reply) {
      assert !replyFuture.isDone() && !replyFuture.isCancelled();
      replyFuture.complete(reply);
    }

    boolean isFailed() {
      return failed || replyFuture.isCompletedExceptionally();
    }

    void failWithReply(RaftClientReply reply) {
      failed = true;
      replyFuture.complete(reply);
    }

    void failWithException(Throwable t) {
      failed = true;
      replyFuture.completeExceptionally(t);
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }
  }

  static class CacheQueryResult {
    private final CacheEntry entry;
    private final boolean isRetry;

    CacheQueryResult(CacheEntry entry, boolean isRetry) {
      this.entry = entry;
      this.isRetry = isRetry;
    }

    public CacheEntry getEntry() {
      return entry;
    }

    public boolean isRetry() {
      return isRetry;
    }
  }

  private final Cache<CacheKey, CacheEntry> cache;

  /**
   * @param capacity the capacity of the cache
   * @param expirationTime time for an entry to expire in milliseconds
   */
  RetryCache(int capacity, TimeDuration expirationTime) {
    capacity = Math.max(capacity, MIN_CAPACITY);
    cache = CacheBuilder.newBuilder().maximumSize(capacity)
        .expireAfterWrite(expirationTime.toLong(TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS).build();
  }

  CacheEntry getOrCreateEntry(ClientId clientId, long callId) {
    final CacheKey key = new CacheKey(clientId, callId);
    final CacheEntry entry;
    try {
      entry = cache.get(key, () -> new CacheEntry(key));
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
    Preconditions.assertTrue(entry != null && !entry.isDone(),
        "retry cache entry should be pending: %s", entry);
    return entry;
  }

  private CacheEntry refreshEntry(CacheEntry newEntry) {
    cache.put(newEntry.key, newEntry);
    return newEntry;
  }

  CacheQueryResult queryCache(ClientId clientId, long callId) {
    CacheKey key = new CacheKey(clientId, callId);
    final CacheEntry newEntry = new CacheEntry(key);
    CacheEntry cacheEntry;
    try {
      cacheEntry = cache.get(key, () -> newEntry);
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }

    if (cacheEntry == newEntry) {
      // this is the entry we just newly created
      return new CacheQueryResult(cacheEntry, false);
    } else if (!cacheEntry.isDone() || !cacheEntry.isFailed()){
      // the previous attempt is either pending or successful
      return new CacheQueryResult(cacheEntry, true);
    }

    // the previous attempt failed, replace it with a new one.
    synchronized (this) {
      // need to recheck, since there may be other retry attempts being
      // processed at the same time. The recheck+replacement should be protected
      // by lock.
      CacheEntry currentEntry = cache.getIfPresent(key);
      if (currentEntry == cacheEntry || currentEntry == null) {
        // if the failed entry has not got replaced by another retry, or the
        // failed entry got invalidated, we add a new cache entry
        return new CacheQueryResult(refreshEntry(newEntry), false);
      } else {
        return new CacheQueryResult(currentEntry, true);
      }
    }
  }

  @VisibleForTesting
  long size() {
    return cache.size();
  }

  @VisibleForTesting
  CacheEntry get(ClientId clientId, long callId) {
    return cache.getIfPresent(new CacheKey(clientId, callId));
  }

  @Override
  public synchronized void close() {
    if (cache != null) {
      cache.invalidateAll();
    }
  }
}
