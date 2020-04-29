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

import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Retry Policy exponentially increases sleep time with randomness on successive
 * retries. The sleep time is a geometric progression b*2, b*4, b*8, b*16...
 * bounded by maximum configured duration.
 *
 * If sleep time calculated using the progression is s then randomness is added
 * in the range [s*0.5, s*1.5).
 */
public final class ExponentialBackoffRetry implements RetryPolicy {

  public static final class Builder {

    private Builder() {}

    private TimeDuration baseSleepTime;
    private TimeDuration maxSleepTime = null;
    private int maxAttempts = Integer.MAX_VALUE;

    public Builder setMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder setBaseSleepTime(TimeDuration baseSleepTime) {
      this.baseSleepTime = baseSleepTime;
      return this;
    }

    public Builder setMaxSleepTime(TimeDuration maxSleepTime) {
      this.maxSleepTime = maxSleepTime;
      return this;
    }

    public ExponentialBackoffRetry build() {
      Preconditions.assertNotNull(baseSleepTime, "baseSleepTime");
      return new ExponentialBackoffRetry(baseSleepTime, maxSleepTime,
          maxAttempts);
    }
  }

  private final TimeDuration baseSleepTime;
  private final TimeDuration maxSleepTime;
  private final int maxAttempts;

  private ExponentialBackoffRetry(TimeDuration baseSleepTime, TimeDuration maxSleepTime, int maxAttempts) {
    this.baseSleepTime = baseSleepTime;
    this.maxSleepTime = maxSleepTime;
    this.maxAttempts = maxAttempts;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private TimeDuration getSleepTime(int attemptCount) {
    TimeDuration sleepTime = baseSleepTime.multiply(Math.pow(2, attemptCount));
    sleepTime = maxSleepTime != null && sleepTime.compareTo(maxSleepTime) > 0 ? maxSleepTime : sleepTime;
    return sleepTime.multiply(ThreadLocalRandom.current().nextDouble() + 0.5);
  }

  @Override
  public Action handleAttemptFailure(Event event) {
    TimeDuration sleepTime = getSleepTime(event.getAttemptCount());
    return event.getAttemptCount() < maxAttempts ? () -> sleepTime : NO_RETRY_ACTION;
  }
}
