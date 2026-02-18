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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestRetryPolicyParse {
  @Test
  void testParseExponentialBackoffRetry() {
    final RetryPolicy policy = RetryPolicy.parse("ExponentialBackoffRetry,100ms,5s,100");
    assertInstanceOf(ExponentialBackoffRetry.class, policy);
  }

  @Test
  void testParseMultipleLinearRandomRetryWithClassname() {
    final MultipleLinearRandomRetry expected =
        MultipleLinearRandomRetry.parseCommaSeparated("1ms,10,1s,20,5s,1000");
    final RetryPolicy actual =
        RetryPolicy.parse("MultipleLinearRandomRetry,1ms,10,1s,20,5s,1000");
    assertEquals(expected, actual);
  }

  @Test
  void testParseMultipleLinearRandomRetryWithoutClassname() {
    final MultipleLinearRandomRetry expected =
        MultipleLinearRandomRetry.parseCommaSeparated("1ms,10,1s,20,5s,1000");
    final RetryPolicy actual = RetryPolicy.parse("1ms,10,1s,20,5s,1000");
    assertEquals(expected, actual);
  }

  @Test
  void testParseUnknownClassnameThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> RetryPolicy.parse("UnknownRetry,1ms,10"));
  }

  @Test
  void testParseMultipleLinearRandomRetryMissingParamsThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> RetryPolicy.parse("MultipleLinearRandomRetry"));
  }

  @Test
  void testParseNonLegacyUnknownFirstTokenThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> RetryPolicy.parse("not_a_duration,1ms,10"));
  }
}
