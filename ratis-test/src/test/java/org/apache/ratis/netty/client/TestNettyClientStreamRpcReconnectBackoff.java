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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNettyClientStreamRpcReconnectBackoff {
  private static final Class<?> CONNECTION_CLASS;
  private static final Method NEXT_BACKOFF;
  private static final Method JITTER_DELAY;
  private static final long MIN_RECONNECT_MILLIS;
  private static final long MAX_RECONNECT_MILLIS;

  static {
    try {
      CONNECTION_CLASS = Class.forName("org.apache.ratis.netty.client.NettyClientStreamRpc$Connection");
      NEXT_BACKOFF = CONNECTION_CLASS.getDeclaredMethod("nextBackoffMillis", long.class, long.class, long.class);
      NEXT_BACKOFF.setAccessible(true);
      JITTER_DELAY = CONNECTION_CLASS.getDeclaredMethod("jitterDelay", long.class);
      JITTER_DELAY.setAccessible(true);
      MIN_RECONNECT_MILLIS = org.apache.ratis.netty.NettyConfigKeys.DataStream.Client
          .RECONNECT_DELAY_DEFAULT.getUnit().toMillis(
              org.apache.ratis.netty.NettyConfigKeys.DataStream.Client.RECONNECT_DELAY_DEFAULT.getDuration());
      MAX_RECONNECT_MILLIS = org.apache.ratis.netty.NettyConfigKeys.DataStream.Client
          .RECONNECT_MAX_DELAY_DEFAULT.getUnit().toMillis(
              org.apache.ratis.netty.NettyConfigKeys.DataStream.Client.RECONNECT_MAX_DELAY_DEFAULT.getDuration());
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Test
  public void testNextBackoffMillisDoublesAndCaps() throws Exception {
    assertEquals(200L, (long) NEXT_BACKOFF.invoke(null, 100L, MIN_RECONNECT_MILLIS, MAX_RECONNECT_MILLIS));
    assertEquals(200L, (long) NEXT_BACKOFF.invoke(null, 50L, MIN_RECONNECT_MILLIS, MAX_RECONNECT_MILLIS));
    assertEquals(MAX_RECONNECT_MILLIS,
        (long) NEXT_BACKOFF.invoke(null, MAX_RECONNECT_MILLIS, MIN_RECONNECT_MILLIS, MAX_RECONNECT_MILLIS));
    assertEquals(MAX_RECONNECT_MILLIS,
        (long) NEXT_BACKOFF.invoke(null, MAX_RECONNECT_MILLIS / 2, MIN_RECONNECT_MILLIS, MAX_RECONNECT_MILLIS));
  }

  @Test
  public void testJitterDelayWithinRange() throws Exception {
    final long base = 1000L;
    for (int i = 0; i < 50; i++) {
      final long delay = (long) JITTER_DELAY.invoke(null, base);
      assertTrue(delay >= base / 2, "delay too small: " + delay);
      assertTrue(delay <= base + base / 2, "delay too large: " + delay);
    }
  }
}
