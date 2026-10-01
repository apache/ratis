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

import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.REQUEST_MEGA_BYTE_SIZE;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.REQUEST_QUEUE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.impl.RatisMetricRegistryImpl;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.thirdparty.com.codahale.metrics.Gauge;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * {@link RaftServerImpl#appendTransaction} acquires a {@link PendingRequests.Permit} before
 * {@code appendLog}. If preAppend throws {@link org.apache.ratis.protocol.exceptions.StateMachineException},
 * the permit is released via {@link PendingRequests#releasePermit} instead of {@link PendingRequests#add}.
 */
public class TestPendingRequests {
  private static final int ELEMENT_LIMIT = 4;
  private static final Message MESSAGE = Message.valueOf("message");

  private PendingRequests pendingRequests;
  private RatisMetricRegistryImpl registry;

  @BeforeEach
  public void setUp() {
    final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(
        RaftPeerId.valueOf("s0"), RaftGroupId.randomId());

    final RaftProperties p = new RaftProperties();
    RaftServerConfigKeys.Write.setElementLimit(p, ELEMENT_LIMIT);
    RaftServerConfigKeys.Write.setByteLimit(p, SizeInBytes.valueOf("8MB"));

    final RaftServerMetricsImpl metrics = RaftServerMetricsImpl.computeIfAbsentRaftServerMetrics(
        memberId, id -> 0L, () -> null);
    this.registry = (RatisMetricRegistryImpl) metrics.getRegistry();
    this.pendingRequests = new PendingRequests(memberId, p, metrics);
  }

  @Test
  public void testReleasePermitAfterPreAppendFailure() {
    // Same pattern as a leader that rejects preAppend: tryAcquire, never add, then release.
    for (int i = 0; i < ELEMENT_LIMIT * 2; i++) {
      final int iteration = i;
      final PendingRequests.Permit permit = pendingRequests.tryAcquire(MESSAGE);
      assertNotNull(permit, () -> "Failed to acquire permit on iteration " + iteration);
      pendingRequests.releasePermit(permit);
    }

    assertEquals(0, getGauge(REQUEST_QUEUE_SIZE));
    assertEquals(0, getGauge(REQUEST_MEGA_BYTE_SIZE));
    assertNotNull(pendingRequests.tryAcquire(MESSAGE),
        "Write capacity was not restored after rejected preAppend-style releases");
  }

  private int getGauge(String name) {
    final Map<String, Gauge> gauges = registry.getGauges((s, metric) -> s.contains(name));
    assertEquals(1, gauges.size(), () -> "Expected exactly one gauge matching " + name);
    return (Integer) gauges.values().iterator().next().getValue();
  }
}
