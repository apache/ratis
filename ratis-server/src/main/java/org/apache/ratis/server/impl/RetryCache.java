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

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.com.google.common.cache.Cache;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheStats;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryCache implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(RetryCache.class);

  /**
   * CacheEntry is tracked using unique client ID and callId of the RPC request
   */
  @VisibleForTesting
  public static class CacheEntry {
    private final ClientInvocationId key;
    private final CompletableFuture<RaftClientReply> replyFuture =
        new CompletableFuture<>();

    /**
     * "failed" means we failed to commit the request into the raft group, or
     * the request did not get approved by the state machine before the raft
     * replication. Note once the request gets committed by the raft group, this
     * field is never true even if the state machine throws an exception when
     * applying the transaction.
     */
    private volatile boolean failed = false;

    CacheEntry(ClientInvocationId key) {
      this.key = key;
    }

    @Override
    public String toString() {
      return key + ":" + (isDone() ? "done" : "pending");
    }

    boolean isDone() {
      return isFailed() || replyFuture.isDone();
    }

    boolean isCompletedNormally() {
      return !failed && replyFuture.isDone() && !replyFuture.isCompletedExceptionally() && !replyFuture.isCancelled();
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

    ClientInvocationId getKey() {
      return key;
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

  private final Cache<ClientInvocationId, CacheEntry> cache;

  /**
   * @param expirationTime time for an entry to expire in milliseconds
   */
  RetryCache(TimeDuration expirationTime) {
    cache = CacheBuilder.newBuilder()
        .recordStats()
        .expireAfterWrite(expirationTime.getDuration(), expirationTime.getUnit())
        .build();
  }

  CacheEntry getOrCreateEntry(ClientInvocationId key) {
    final CacheEntry entry;
    try {
      entry = cache.get(key, () -> new CacheEntry(key));
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
    return entry;
  }

  CacheEntry refreshEntry(CacheEntry newEntry) {
    cache.put(newEntry.key, newEntry);
    return newEntry;
  }

  CacheQueryResult queryCache(ClientInvocationId key) {
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
  public long size() {
    return cache.size();
  }

  public CacheStats stats() {
    return cache.stats();
  }

  @VisibleForTesting
  CacheEntry get(ClientInvocationId key) {
    return cache.getIfPresent(key);
  }

  @Override
  public synchronized void close() {
    if (cache != null) {
      cache.invalidateAll();
    }
  }

  static CompletableFuture<RaftClientReply> failWithReply(
      RaftClientReply reply, CacheEntry entry) {
    if (entry != null) {
      entry.failWithReply(reply);
      return entry.getReplyFuture();
    } else {
      return CompletableFuture.completedFuture(reply);
    }
  }

  static CompletableFuture<RaftClientReply> failWithException(
      Throwable t, CacheEntry entry) {
    if (entry != null) {
      entry.failWithException(t);
      return entry.getReplyFuture();
    } else {
      return JavaUtils.completeExceptionally(t);
    }
  }
}
