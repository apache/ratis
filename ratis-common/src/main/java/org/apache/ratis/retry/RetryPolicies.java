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

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;

import java.util.function.Supplier;

/**
 * A collection of {@link RetryPolicy} implementations
 */
public interface RetryPolicies {
  /** For any requests, keep retrying forever with no sleep between attempts. */
  static RetryPolicy retryForeverNoSleep() {
    return Constants.RETRY_FOREVER_NO_SLEEP;
  }

  /** No retry. */
  static RetryPolicy noRetry() {
    return Constants.NO_RETRY;
  }

  /** For any requests, keep retrying forever with a fixed sleep time between attempts. */
  static RetryForeverWithSleep retryForeverWithSleep(TimeDuration sleepTime) {
    return new RetryForeverWithSleep(sleepTime);
  }

  /** For any requests, keep retrying a limited number of attempts with a fixed sleep time between attempts. */
  static RetryLimited retryUpToMaximumCountWithFixedSleep(int maxAttempts, TimeDuration sleepTime) {
    return new RetryLimited(maxAttempts, sleepTime);
  }

  class Constants {
    private static final RetryForeverNoSleep RETRY_FOREVER_NO_SLEEP = new RetryForeverNoSleep();
    private static final NoRetry NO_RETRY = new NoRetry();
  }

  final class RetryForeverNoSleep implements RetryPolicy {
    private RetryForeverNoSleep() {}

    @Override
    public Action handleAttemptFailure(Event event) {
      return RetryPolicy.RETRY_WITHOUT_SLEEP_ACTION;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass());
    }
  }

  final class NoRetry implements RetryPolicy {
    private NoRetry() {}

    @Override
    public Action handleAttemptFailure(Event event) {
      return RetryPolicy.NO_RETRY_ACTION;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass());
    }
  }

  /** For any requests, keep retrying forever with a fixed sleep time between attempts. */
  class RetryForeverWithSleep implements RetryPolicy {
    private final TimeDuration sleepTime;

    private RetryForeverWithSleep(TimeDuration sleepTime) {
      Preconditions.assertTrue(!sleepTime.isNegative(), () -> "sleepTime = " + sleepTime + " < 0");
      this.sleepTime = sleepTime;
    }

    @Override
    public Action handleAttemptFailure(Event event) {
      return () -> sleepTime;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + "(sleepTime = " + sleepTime + ")";
    }
  }

  /** For any requests, keep retrying a limited number of attempts with a fixed sleep time between attempts. */
  final class RetryLimited extends RetryForeverWithSleep  {
    private final int maxAttempts;
    private final Supplier<String> myString;

    private RetryLimited(int maxAttempts, TimeDuration sleepTime) {
      super(sleepTime);

      if (maxAttempts < 0) {
        throw new IllegalArgumentException("maxAttempts = " + maxAttempts+" < 0");
      }

      this.maxAttempts = maxAttempts;
      this.myString = JavaUtils.memoize(() -> JavaUtils.getClassSimpleName(getClass())
          + "(maxAttempts=" + maxAttempts + ", sleepTime=" + sleepTime + ")");
    }

    public int getMaxAttempts() {
      return maxAttempts;
    }

    @Override
    public Action handleAttemptFailure(Event event) {
      return event.getAttemptCount() < maxAttempts? super.handleAttemptFailure(event): NO_RETRY_ACTION;
    }

    @Override
    public String toString() {
      return myString.get();
    }
  }
}
