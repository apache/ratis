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
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.common.cache.Cache;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/** Caching the per client write index in order to support read-after-write consistency. */
class WriteIndexCache {
  private final Cache<ClientId, AtomicReference<CompletableFuture<Long>>> cache;

  WriteIndexCache(RaftProperties properties) {
    this(RaftServerConfigKeys.Read.ReadAfterWriteConsistent.writeIndexCacheExpiryTime(properties));
  }

  /**
   * @param cacheExpiryTime time for a cache entry to expire.
   */
  WriteIndexCache(TimeDuration cacheExpiryTime) {
    this.cache = CacheBuilder.newBuilder()
        .expireAfterAccess(cacheExpiryTime.getDuration(), cacheExpiryTime.getUnit())
        .build();
  }

  void add(ClientId key, CompletableFuture<Long> future) {
    final AtomicReference<CompletableFuture<Long>> ref;
    try {
      ref = cache.get(key, AtomicReference::new);
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
    ref.set(future);
  }

  CompletableFuture<Long> getWriteIndexFuture(RaftClientRequest request) {
    if (request != null && request.getType().getRead().getReadAfterWriteConsistent()) {
      final AtomicReference<CompletableFuture<Long>> ref = cache.getIfPresent(request.getClientId());
      if (ref != null) {
        return ref.get();
      }
    }
    return CompletableFuture.completedFuture(null);
  }
}
