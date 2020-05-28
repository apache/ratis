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
package org.apache.ratis.retry;

import org.apache.ratis.BaseTest;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests ExponentialBackoffRetry policy.
 */
public class TestExponentialBackoffRetry extends BaseTest {

  @Test
  public void testExponentialBackoffRetry() {
    TimeDuration baseSleep = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    TimeDuration maxSleep = TimeDuration.valueOf(40, TimeUnit.SECONDS);

    // Test maxAttempts
    ExponentialBackoffRetry retryPolicy = createPolicy(baseSleep, null, 1);
    Assert.assertFalse(retryPolicy.handleAttemptFailure(() -> 1).shouldRetry());

    try {
      // baseSleep should not be null
      createPolicy(null, null, 1);
      Assert.fail("Policy creation should have failed");
    } catch (Exception e) {
    }

    // test policy without max sleep
    retryPolicy = createPolicy(baseSleep, null,100);
    assertSleep(retryPolicy, baseSleep, null);

    // test policy with max sleep
    retryPolicy = createPolicy(baseSleep, maxSleep,100);
    assertSleep(retryPolicy, baseSleep, maxSleep);
  }

  private void assertSleep(ExponentialBackoffRetry retryPolicy,
      TimeDuration baseSleep, TimeDuration maxSleep) {
    for (int i = 1; i <= 50; i++) {
      int attempt = i;
      RetryPolicy.Action action = retryPolicy.handleAttemptFailure(() -> attempt);

      // sleep time based on geometric progresssion
      long d = (1L << attempt) * baseSleep.toLong(TimeUnit.MILLISECONDS);
      d = Math.min(d, maxSleep != null ? maxSleep.toLong(TimeUnit.MILLISECONDS) : Long.MAX_VALUE);

      // sleep time with randomness added
      long randomizedDuration = action.getSleepTime().toLong(TimeUnit.MILLISECONDS);

      Assert.assertTrue(action.shouldRetry());
      Assert.assertTrue(randomizedDuration >= d * 0.5);
      Assert.assertTrue(randomizedDuration <= d * 1.5);
    }
  }

  private ExponentialBackoffRetry createPolicy(TimeDuration baseSleep,
      TimeDuration maxSleep, int maxAttempts) {
    return ExponentialBackoffRetry.newBuilder().setBaseSleepTime(baseSleep)
        .setMaxAttempts(maxAttempts).setMaxSleepTime(maxSleep).build();
  }
}
