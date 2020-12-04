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

import static org.apache.ratis.server.metrics.RaftServerMetrics.*;
import static org.junit.Assert.assertEquals;

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.metrics.RaftServerMetrics;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test for metrics of retry cache.
 */
public class TestRetryCacheMetrics {
    private static RatisMetricRegistry ratisMetricRegistry;
    private static RetryCache retryCache;

    @BeforeClass
    public static void setUp() {
      RaftGroupId raftGroupId = RaftGroupId.randomId();
      RaftPeerId raftPeerId = RaftPeerId.valueOf("TestId");
      RaftGroupMemberId raftGroupMemberId = RaftGroupMemberId
          .valueOf(raftPeerId, raftGroupId);
      retryCache = new RetryCache(TimeDuration.valueOf(60, TimeUnit.SECONDS));

      final RaftServerMetrics raftServerMetrics = RaftServerMetrics.computeIfAbsentRaftServerMetrics(
          raftGroupMemberId, () -> null, () -> retryCache);
      ratisMetricRegistry = raftServerMetrics.getRegistry();
    }
    
    @After
    public void tearDown() {
        retryCache.close();
        checkEntryCount(0);
    }

    @Test
    public void testRetryCacheEntryCount() {
      checkEntryCount(0);

      ClientId clientId = ClientId.randomId();
      final ClientInvocationId key = ClientInvocationId.valueOf(clientId, 1);
      RetryCache.CacheEntry entry = new RetryCache.CacheEntry(key);

      retryCache.refreshEntry(entry);
      checkEntryCount(1);
    }

    @Test
    public void testRetryCacheHitMissCount() {
      checkHit(0, 1.0);
      checkMiss(0, 0.0);

      final ClientInvocationId invocationId = ClientInvocationId.valueOf(ClientId.randomId(), 2);
      retryCache.getOrCreateEntry(invocationId);

      checkHit(0, 0.0);
      checkMiss(1, 1.0);

      retryCache.getOrCreateEntry(invocationId);

      checkHit(1, 0.5);
      checkMiss(1, 0.5);
    }

    private static void checkHit(long count, double rate) {
      Long hitCount = (Long) ratisMetricRegistry.getGauges((s, metric) ->
          s.contains(RETRY_CACHE_HIT_COUNT_METRIC)).values().iterator().next().getValue();
      assertEquals(hitCount.longValue(), count);

      Double hitRate = (Double) ratisMetricRegistry.getGauges((s, metric) ->
          s.contains(RETRY_CACHE_HIT_RATE_METRIC)).values().iterator().next().getValue();
      assertEquals(hitRate.doubleValue(), rate, 0.0);
    }

    private static void checkMiss(long count, double rate) {
      Long missCount = (Long) ratisMetricRegistry.getGauges((s, metric) ->
          s.contains(RETRY_CACHE_MISS_COUNT_METRIC)).values().iterator().next().getValue();
      assertEquals(missCount.longValue(), count);

      Double missRate = (Double) ratisMetricRegistry.getGauges((s, metric) ->
          s.contains(RETRY_CACHE_MISS_RATE_METRIC)).values().iterator().next().getValue();
      assertEquals(missRate.doubleValue(), rate, 0.0);
    }

    private static void checkEntryCount(long count) {
      Long entryCount = (Long) ratisMetricRegistry.getGauges((s, metric) ->
          s.contains(RETRY_CACHE_ENTRY_COUNT_METRIC)).values().iterator().next().getValue();
      assertEquals(entryCount.longValue(), count);
    }
}
