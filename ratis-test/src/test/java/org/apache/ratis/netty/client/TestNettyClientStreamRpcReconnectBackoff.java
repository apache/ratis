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
package org.apache.ratis.netty.client;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNettyClientStreamRpcReconnectBackoff {
  @Test
  public void testReconnectPolicyBackoffRanges() throws Exception {
    // Use a small base/max to keep the test fast and deterministic in range checks.
    final RaftProperties properties = new RaftProperties();
    final TimeDuration base = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    final TimeDuration max = TimeDuration.valueOf(400, TimeUnit.MILLISECONDS);
    final int maxAttempts = 5;
    NettyConfigKeys.DataStream.Client.setReconnectDelay(properties, base);
    NettyConfigKeys.DataStream.Client.setReconnectMaxDelay(properties, max);
    NettyConfigKeys.DataStream.Client.setReconnectMaxAttempts(properties, maxAttempts);

    final RaftPeer peer = RaftPeer.newBuilder()
        .setId("s1")
        .setDataStreamAddress(new InetSocketAddress("127.0.0.1", 1))
        .build();

    final NettyClientStreamRpc rpc = new NettyClientStreamRpc(peer, null, properties);
    try {
      final Object connection = getField(rpc, "connection");
      // Verify the reconnect policy is exponential and uses the configured maxAttempts.
      final RetryPolicy policy = (RetryPolicy) getField(connection, "reconnectPolicy");
      assertTrue(policy instanceof ExponentialBackoffRetry);
      assertEquals(maxAttempts, (int) getField(policy, "maxAttempts"));

      // attempt=0 -> base delay; attempt=1 -> 2x base; attempt=3 -> capped by max.
      assertSleepInRange(policy, 0, base, max);
      assertSleepInRange(policy, 1, base, max);
      // Attempt 3 should be capped by max sleep time.
      assertSleepInRange(policy, 3, base, max);
    } finally {
      rpc.close();
    }
  }

  private static Object getField(Object object, String name) throws Exception {
    final Field field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(object);
  }

  private static void assertSleepInRange(RetryPolicy policy, int attempt, TimeDuration base, TimeDuration max) {
    final RetryPolicy.Action action = policy.handleAttemptFailure(() -> attempt);
    assertTrue(action.shouldRetry());

    final long baseMillis = base.toLong(TimeUnit.MILLISECONDS);
    final long maxMillis = max.toLong(TimeUnit.MILLISECONDS);
    final long expected = Math.min(maxMillis, baseMillis * (1L << attempt));
    final long actual = action.getSleepTime().toLong(TimeUnit.MILLISECONDS);

    assertTrue(actual >= expected / 2, "delay too small: " + actual);
    assertTrue(actual <= expected + expected / 2, "delay too large: " + actual);
  }
}
