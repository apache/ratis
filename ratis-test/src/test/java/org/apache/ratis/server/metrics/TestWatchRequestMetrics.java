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
package org.apache.ratis.server.metrics;

import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestWatchRequestMetrics {
  @Test
  public void testCountersForEveryReplicationLevel() {
    final RetryCache.Statistics statistics = Mockito.mock(RetryCache.Statistics.class);
    final RaftServerMetricsImpl metrics = new RaftServerMetricsImpl(
        RaftServerTestUtil.TEST_MEMBER_ID, peer -> 0L, () -> statistics);
    try {
      for (ReplicationLevel level : ReplicationLevel.values()) {
        metrics.onWatchRequestQueueLimitHit(level);
        metrics.onWatchRequestTimeout(level);
        Assertions.assertEquals(1L, metrics.getNumWatchRequestQueueLimitHits(level).getCount());
        Assertions.assertEquals(1L, metrics.getNumWatchRequestsTimeout(level).getCount());
      }
    } finally {
      metrics.unregister();
    }
  }
}
