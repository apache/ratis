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

import org.apache.ratis.util.TimeDuration;

/**
 * A collection of {@link RetryPolicy} implementations
 */
public interface RetryPolicies {
  /**
   * Keep retrying forever with zero sleep.
   */
  static RetryPolicy retryForeverNoSleep() {
    return Constants.RETRY_FOREVER_NO_SLEEP;
  }

  static RetryPolicy noRetry() {
    return Constants.NO_RETRY;
  }

  /**
   * Keep retrying forever with fixed sleep.
   */
  static RetryPolicy retryForeverWithSleep(TimeDuration sleepTime) {
    return new RetryForeverWithSleep(sleepTime);
  }

  /**
   * Keep trying a limited number of times, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   */
  static RetryLimited retryUpToMaximumCountWithFixedSleep(int maxAttempts, TimeDuration sleepTime) {
    return new RetryLimited(maxAttempts, sleepTime);
  }

  class Constants {
    private static final RetryForeverNoSleep RETRY_FOREVER_NO_SLEEP = new RetryForeverNoSleep();
    private static final NoRetry NO_RETRY = new NoRetry();
  }

  class RetryForeverNoSleep implements RetryPolicy {
    private RetryForeverNoSleep() {}

    @Override
    public boolean shouldRetry(int attemptCount) {
      return true;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  class NoRetry implements RetryPolicy {
    private NoRetry() {}

    @Override
    public boolean shouldRetry(int attemptCount) {
      return false;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  class RetryForeverWithSleep implements RetryPolicy {
    private final TimeDuration sleepTime;

    private String myString;

    RetryForeverWithSleep(TimeDuration sleepTime) {
      if (sleepTime.isNegative()) {
        throw new IllegalArgumentException(
            "sleepTime = " + sleepTime.getDuration() + " < 0");
      }
      this.sleepTime = sleepTime;
    }

    @Override
    public TimeDuration getSleepTime() {
      return sleepTime;
    }

    @Override
    public boolean shouldRetry(int attemptCount) {
      return true;
    }

    @Override
    public String toString() {
      if (myString == null) {
        myString = getClass().getSimpleName() + "(sleepTime = " + sleepTime + ")";
      }
      return myString;
    }
  }
  /**
   * Retry up to maxAttempts.
   * The actual sleep time of the n-th retry is f(n, sleepTime),
   * where f is a function provided by the subclass implementation.
   *
   * The object of the subclasses should be immutable;
   * otherwise, the subclass must override hashCode(), equals(..) and toString().
   */
  class RetryLimited implements RetryPolicy {
    private final int maxAttempts;
    private final TimeDuration sleepTime;

    private String myString;

    RetryLimited(int maxAttempts, TimeDuration sleepTime) {
      if (maxAttempts < 0) {
        throw new IllegalArgumentException("maxAttempts = " + maxAttempts+" < 0");
      }
      if (sleepTime.isNegative()) {
        throw new IllegalArgumentException(
            "sleepTime = " + sleepTime.getDuration() + " < 0");
      }

      this.maxAttempts = maxAttempts;
      this.sleepTime = sleepTime;
    }

    @Override
    public TimeDuration getSleepTime() {
      return sleepTime;
    }

    public int getMaxAttempts() {
      return maxAttempts;
    }

    @Override
    public boolean shouldRetry(int attemptCount) {
      return attemptCount <= maxAttempts;
    }

    @Override
    public String toString() {
      if (myString == null) {
        myString = getClass().getSimpleName() + "(maxAttempts=" + maxAttempts
            + ", sleepTime=" + sleepTime + ")";
      }
      return myString;
    }
  }
}
