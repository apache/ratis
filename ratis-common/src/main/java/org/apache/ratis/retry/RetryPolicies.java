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
package org.apache.ratis.retry;

import com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

/**
 * A collection of {@link RetryPolicy} implementations
 */
public class RetryPolicies {
  /**
   * Keep retrying forever.
   */
  public static final RetryPolicy RETRY_FOREVER = new RetryForever();

  /**
   * Keep trying a limited number of times, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   */
  public static final RetryPolicy retryUpToMaximumCountWithFixedSleep(
      int maxRetries, TimeDuration sleepTime) {
    return new RetryUpToMaximumCountWithFixedSleep(maxRetries, sleepTime);
  }


  static class RetryForever implements RetryPolicy {
    @Override
    public boolean shouldRetry(int retryCount) {
      return true;
    }

    @Override
    public TimeDuration getSleepTime() {
      return TimeDuration.valueOf(0, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Retry up to maxRetries.
   * The actual sleep time of the n-th retry is f(n, sleepTime),
   * where f is a function provided by the subclass implementation.
   *
   * The object of the subclasses should be immutable;
   * otherwise, the subclass must override hashCode(), equals(..) and toString().
   */
  static abstract class RetryLimited implements RetryPolicy {
    private final int maxRetries;
    private final TimeDuration sleepTime;

    private String myString;

    RetryLimited(int maxRetries, TimeDuration sleepTime) {
      if (maxRetries < 0) {
        throw new IllegalArgumentException("maxRetries = " + maxRetries+" < 0");
      }
      if (sleepTime.isNegative()) {
        throw new IllegalArgumentException(
            "sleepTime = " + sleepTime.getDuration() + " < 0");
      }

      this.maxRetries = maxRetries;
      this.sleepTime = sleepTime;
    }

    @Override
    public TimeDuration getSleepTime() {
      return sleepTime;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    @Override
    public boolean shouldRetry(int retryCount) {
      if (retryCount >= maxRetries) {
        return false;
      } else {
        return true;
      }
    }

    protected String getReason() {
      return constructReasonString(maxRetries);
    }

    @VisibleForTesting
    public static String constructReasonString(int retries) {
      return "retries get failed due to exceeded maximum allowed retries " +
          "number: " + retries;
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public boolean equals(final Object that) {
      if (this == that) {
        return true;
      } else if (that == null || this.getClass() != that.getClass()) {
        return false;
      }
      return this.toString().equals(that.toString());
    }

    @Override
    public String toString() {
      if (myString == null) {
        myString = getClass().getSimpleName() + "(maxRetries=" + maxRetries
            + ", sleepTime=" + sleepTime + ")";
      }
      return myString;
    }
  }

  static class RetryUpToMaximumCountWithFixedSleep extends RetryLimited {
    public RetryUpToMaximumCountWithFixedSleep(int maxRetries, TimeDuration sleepTime) {
      super(maxRetries, sleepTime);
    }
  }
}
