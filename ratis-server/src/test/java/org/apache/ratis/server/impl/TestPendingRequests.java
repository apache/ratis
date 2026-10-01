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
import static org.junit.jupiter.api.Assertions.assertNull;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Test the resource accounting of {@link PendingRequests}.
 *
 * A {@link PendingRequests.Permit} which is acquired but never put into the pending queue must
 * give its resources back, otherwise the write capacity of a leader shrinks on every request
 * which fails before it is added, e.g. a request rejected by the state machine in preAppend.
 */
public class TestPendingRequests {
  private static final int ELEMENT_LIMIT = 3;
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
  public void testReleasePermitRestoresCapacity() {
    final List<PendingRequests.Permit> permits = new ArrayList<>();
    for (int i = 0; i < ELEMENT_LIMIT; i++) {
      final PendingRequests.Permit permit = pendingRequests.tryAcquire(MESSAGE);
      assertNotNull(permit, () -> "Failed to acquire permit within the element limit");
      permits.add(permit);
    }
    assertEquals(ELEMENT_LIMIT, getGauge(REQUEST_QUEUE_SIZE));
    assertNull(pendingRequests.tryAcquire(MESSAGE), "Acquired more permits than the element limit");

    // Releasing a permit which was never put into the pending queue must give the capacity back.
    pendingRequests.releasePermit(permits.get(0));
    assertEquals(ELEMENT_LIMIT - 1, getGauge(REQUEST_QUEUE_SIZE));
    assertNotNull(pendingRequests.tryAcquire(MESSAGE), "Released capacity was not reusable");
  }

  @Test
  public void testReleaseAllPermits() {
    final List<PendingRequests.Permit> permits = new ArrayList<>();
    for (int i = 0; i < ELEMENT_LIMIT; i++) {
      permits.add(pendingRequests.tryAcquire(MESSAGE));
    }
    permits.forEach(pendingRequests::releasePermit);

    assertEquals(0, getGauge(REQUEST_QUEUE_SIZE));
    assertEquals(0, getGauge(REQUEST_MEGA_BYTE_SIZE));
  }

  @Test
  public void testReleasePermitIsIdempotent() {
    final PendingRequests.Permit permit = pendingRequests.tryAcquire(MESSAGE);
    assertNotNull(permit);

    pendingRequests.releasePermit(permit);
    // A second release must not give the resources back twice.
    pendingRequests.releasePermit(permit);

    assertEquals(0, getGauge(REQUEST_QUEUE_SIZE));
    assertEquals(0, getGauge(REQUEST_MEGA_BYTE_SIZE));
  }

  @Test
  public void testReleaseLargePermitRestoresByteCapacity() {
    final char[] chars = new char[2 * SizeInBytes.ONE_MB.getSizeInt()];
    Arrays.fill(chars, 'a');
    final Message large = Message.valueOf(new String(chars));

    // Acquire and release far more megabytes in total than the byte limit allows to be
    // outstanding at once.  This only keeps working if every release gives the bytes back.
    for (int i = 0; i < 20; i++) {
      final int index = i;
      final PendingRequests.Permit permit = pendingRequests.tryAcquire(large);
      assertNotNull(permit, () -> "Byte capacity was not released, failed on iteration " + index);
      pendingRequests.releasePermit(permit);
    }

    assertEquals(0, getGauge(REQUEST_QUEUE_SIZE));
    assertEquals(0, getGauge(REQUEST_MEGA_BYTE_SIZE));
  }

  private int getGauge(String name) {
    final Map<String, Gauge> gauges = registry.getGauges((s, metric) -> s.contains(name));
    assertEquals(1, gauges.size(), () -> "Expected exactly one gauge matching " + name);
    return (Integer) gauges.values().iterator().next().getValue();
  }
}
