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
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.thirdparty.com.google.common.cache.Cache;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheStats;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class RetryCacheImpl implements RetryCache {
  static class CacheEntry implements Entry {
    private final ClientInvocationId key;
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();

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
      return !failed && JavaUtils.isCompletedNormally(replyFuture);
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

    @Override
    public CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    @Override
    public ClientInvocationId getKey() {
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

  class StatisticsImpl implements Statistics {
    private final long size;
    private final CacheStats cacheStats;
    private final Timestamp creation = Timestamp.currentTime();

    StatisticsImpl(Cache<?, ?> cache) {
      this.size = cache.size();
      this.cacheStats = cache.stats();
    }

    boolean isExpired() {
      return Optional.ofNullable(statisticsExpiryTime).map(t -> creation.elapsedTime().compareTo(t) > 0).orElse(true);
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public long hitCount() {
      return cacheStats.hitCount();
    }

    @Override
    public double hitRate() {
      return cacheStats.hitRate();
    }

    @Override
    public long missCount() {
      return cacheStats.missCount();
    }

    @Override
    public double missRate() {
      return cacheStats.missRate();
    }

    @Override
    public String toString() {
      return creation + ":size=" + size + "," + cacheStats;
    }
  }

  private final Cache<ClientInvocationId, CacheEntry> cache;
  /** Cache statistics to reduce the number of expensive statistics computations. */
  private final AtomicReference<StatisticsImpl> statistics = new AtomicReference<>();
  private final TimeDuration statisticsExpiryTime;

  RetryCacheImpl(RaftProperties properties) {
    this(RaftServerConfigKeys.RetryCache.expiryTime(properties),
         RaftServerConfigKeys.RetryCache.statisticsExpiryTime(properties));
  }

  /**
   * @param cacheExpiryTime time for a cache entry to expire.
   * @param statisticsExpiryTime time for a {@link RetryCache.Statistics} object to expire.
   */
  RetryCacheImpl(TimeDuration cacheExpiryTime, TimeDuration statisticsExpiryTime) {
    this.cache = CacheBuilder.newBuilder()
        .recordStats()
        .expireAfterWrite(cacheExpiryTime.getDuration(), cacheExpiryTime.getUnit())
        .build();
    this.statisticsExpiryTime = statisticsExpiryTime;
  }

  CacheEntry getOrCreateEntry(ClientInvocationId key) {
    return getOrCreateEntry(key, () -> new CacheEntry(key));
  }

  private CacheEntry getOrCreateEntry(ClientInvocationId key, Supplier<CacheEntry> constructor) {
    try {
      return cache.get(key, constructor::get);
    } catch (ExecutionException e) {
      throw new IllegalStateException("Failed to get " + key, e);
    }
  }

  CacheEntry refreshEntry(CacheEntry newEntry) {
    cache.put(newEntry.getKey(), newEntry);
    return newEntry;
  }

  CacheQueryResult queryCache(RaftClientRequest request) {
    final ClientInvocationId key = ClientInvocationId.valueOf(request);
    final MemoizedSupplier<CacheEntry> newEntry = MemoizedSupplier.valueOf(() -> new CacheEntry(key));
    final CacheEntry cacheEntry = getOrCreateEntry(key, newEntry);
    if (newEntry.isInitialized()) {
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
      final CacheEntry currentEntry = cache.getIfPresent(key);
      if (currentEntry == cacheEntry || currentEntry == null) {
        // if the failed entry has not got replaced by another retry, or the
        // failed entry got invalidated, we add a new cache entry
        return new CacheQueryResult(refreshEntry(newEntry.get()), false);
      } else {
        return new CacheQueryResult(currentEntry, true);
      }
    }
  }

  void invalidateRepliedRequests(RaftClientRequest request) {
    final ClientId clientId = request.getClientId();
    final Iterable<Long> callIds = request.getRepliedCallIds();
    if (!callIds.iterator().hasNext()) {
      return;
    }

    LOG.debug("invalidateRepliedRequests callIds {} for {}", callIds, clientId);
    cache.invalidateAll(CollectionUtils.as(callIds, callId -> ClientInvocationId.valueOf(clientId, callId)));
  }

  @Override
  public Statistics getStatistics() {
    return statistics.updateAndGet(old -> old == null || old.isExpired()? new StatisticsImpl(cache): old);
  }

  @Override
  public CacheEntry getIfPresent(ClientInvocationId key) {
    return cache.getIfPresent(key);
  }

  @Override
  public synchronized void close() {
    cache.invalidateAll();
    statistics.set(null);
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
