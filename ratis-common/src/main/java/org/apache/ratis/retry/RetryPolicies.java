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

import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

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

  class RetryForeverNoSleep implements RetryPolicy {
    private RetryForeverNoSleep() {}

    @Override
    public boolean shouldRetry(int attemptCount, RaftClientRequest request) {
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
    public boolean shouldRetry(int attemptCount, RaftClientRequest request) {
      return false;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  /** For any requests, keep retrying forever with a fixed sleep time between attempts. */
  class RetryForeverWithSleep implements RetryPolicy {
    private final TimeDuration sleepTime;

    private RetryForeverWithSleep(TimeDuration sleepTime) {
      Preconditions.assertTrue(!sleepTime.isNegative(), () -> "sleepTime = " + sleepTime.getDuration() + " < 0");
      this.sleepTime = sleepTime;
    }

    @Override
    public TimeDuration getSleepTime(int attemptCount, RaftClientRequest request) {
      return sleepTime;
    }

    @Override
    public boolean shouldRetry(int attemptCount, RaftClientRequest request) {
      return true;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(sleepTime = " + sleepTime + ")";
    }
  }

  /** For any requests, keep retrying a limited number of attempts with a fixed sleep time between attempts. */
  class RetryLimited implements RetryPolicy {
    private final int maxAttempts;
    private final TimeDuration sleepTime;

    private String myString;

    private RetryLimited(int maxAttempts, TimeDuration sleepTime) {
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
    public TimeDuration getSleepTime(int attemptCount, RaftClientRequest request) {
      return shouldRetry(attemptCount, request)? sleepTime: ZERO_MILLIS;
    }

    public int getMaxAttempts() {
      return maxAttempts;
    }

    @Override
    public boolean shouldRetry(int attemptCount, RaftClientRequest request) {
      return attemptCount < maxAttempts;
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

  /**
   * A {@link RaftClientRequest.Type} dependent {@link RetryPolicy}
   * such that each type can be set to use an individual policy.
   * When the policy is not set for a particular type,
   * the {@link #retryForeverNoSleep()} policy is used as the default.
   */
  class RequestTypeDependentRetry implements RetryPolicy {
    public static class Builder {
      private final EnumMap<RaftClientRequestProto.TypeCase, RetryPolicy> map
          = new EnumMap<>(RaftClientRequestProto.TypeCase.class);

      /** Set the given policy for the given type. */
      public Builder set(RaftClientRequestProto.TypeCase type, RetryPolicy policy) {
        final RetryPolicy previous = map.put(type, policy);
        Preconditions.assertNull(previous, () -> "The type " + type + " is already set to " + previous);
        return this;
      }

      public RequestTypeDependentRetry build() {
        return new RequestTypeDependentRetry(map);
      }
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    private final Map<RaftClientRequestProto.TypeCase, RetryPolicy> map;

    private RequestTypeDependentRetry(EnumMap<RaftClientRequestProto.TypeCase, RetryPolicy> map) {
      this.map = Collections.unmodifiableMap(map);
    }

    @Override
    public boolean shouldRetry(int attemptCount, RaftClientRequest request) {
      return Optional.ofNullable(map.get(request.getType().getTypeCase()))
          .orElse(retryForeverNoSleep())
          .shouldRetry(attemptCount, request);
    }

    @Override
    public TimeDuration getSleepTime(int attemptCount, RaftClientRequest request) {
      return Optional.ofNullable(map.get(request.getType().getTypeCase()))
          .orElse(retryForeverNoSleep())
          .getSleepTime(attemptCount, request);
    }

    @Override
    public String toString() {
      final StringBuilder b = new StringBuilder(getClass().getSimpleName()).append("{");
      map.forEach((key, value) -> b.append(key).append("->").append(value).append(", "));
      b.setLength(b.length() - 2);
      return b.append("}").toString();
    }
  }
}
