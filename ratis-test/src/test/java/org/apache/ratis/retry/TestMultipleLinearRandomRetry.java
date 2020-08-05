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

public class TestMultipleLinearRandomRetry extends BaseTest {
  @Override
  public int getGlobalTimeoutSeconds() {
    return 1;
  }

  @Test
  public void testParseCommaSeparated() {
    assertIllegalInput("");
    assertIllegalInput("11");
    assertIllegalInput("11,22,33");
    assertIllegalInput("11,22,33,44,55");
    assertIllegalInput("AA");
    assertIllegalInput("11,AA");
    assertIllegalInput("11,22,33,FF");
    assertIllegalInput("11,-22");
    assertIllegalInput("-11,22");

    assertLegalInput("[22x11ms]", "11,22");
    assertLegalInput("[22x11ms, 44x33s]", "1_1ms,22,33s,4_4");
    assertLegalInput("[22x11ms, 44x33ms, 66x55ms]", "11,2_2,33_MS,44,55,66");
    assertLegalInput("[22x11s, 44x33ms, 66x55ms]", "   11s,   22, 33,  44, 55__MS,  6_6   ");
    assertLegalInput("[10x100ms, 20x1s, 30x5s]", "100,10, 1s,20, 5s,30");
  }

  private static void assertIllegalInput(String input) {
    final MultipleLinearRandomRetry computed = MultipleLinearRandomRetry.parseCommaSeparated(input);
    Assert.assertNull(computed);
  }
  private static MultipleLinearRandomRetry assertLegalInput(String expected, String input) {
    final MultipleLinearRandomRetry computed = MultipleLinearRandomRetry.parseCommaSeparated(input);
    Assert.assertNotNull(computed);
    Assert.assertTrue(computed.toString().endsWith(expected));
    return computed;
  }

  @Test
  public void testMultipleLinearRandomRetry() {
    double precision = 0.00000001;
    final int[] counts = {10, 20, 30};
    final TimeDuration[] times = {HUNDRED_MILLIS, ONE_SECOND, FIVE_SECONDS};
    final MultipleLinearRandomRetry r = assertLegalInput("[10x100ms, 20x1s, 30x5s]", "100ms,10, 1s,20, 5s,30");
    int k = 0;
    for(int i = 0; i < counts.length; i++) {
      for (int j = 1; j <= counts[i]; j++) {
        final int attempt = ++k;
        final RetryPolicy.Action action = r.handleAttemptFailure(() -> attempt);
        Assert.assertTrue(action.shouldRetry());
        final TimeDuration randomized = action.getSleepTime();
        final TimeDuration expected = times[i].to(randomized.getUnit());
        final long d = expected.getDuration();
        LOG.info("times[{},{}] = {}, randomized={}", i, j, times[i], randomized);
        Assert.assertTrue(randomized.getDuration() >= d*0.5);
        Assert.assertTrue(randomized.getDuration() < (d*1.5 + precision));
      }
    }

    final int attempt = ++k;
    final RetryPolicy.Action action = r.handleAttemptFailure(() -> attempt);
    Assert.assertFalse(action.shouldRetry());
  }
}