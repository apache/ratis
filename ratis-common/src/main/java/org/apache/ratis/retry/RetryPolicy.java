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
 * Policy abstract for retrying.
 */
@FunctionalInterface
public interface RetryPolicy {
  Action NO_RETRY_ACTION = new Action() {
    @Override
    public boolean shouldRetry() {
      return false;
    }
    @Override
    public TimeDuration getSleepTime() {
      return TimeDuration.ZERO;
    }
  };

  Action RETRY_WITHOUT_SLEEP_ACTION = () -> TimeDuration.ZERO;

  /** The action it should take. */
  @FunctionalInterface
  interface Action {
    /** @return true if it has to make another attempt; otherwise, return false. */
    default boolean shouldRetry() {
      return true;
    }

    /** @return the sleep time period before the next attempt. */
    TimeDuration getSleepTime();
  }

  /** The event triggered the failure. */
  @FunctionalInterface
  interface Event {
    /** @return the number of attempts tried so far. */
    int getAttemptCount();

    /** @return the number of attempts for the event cause. */
    default int getCauseCount() {
      return 0;
    }

    default Throwable getCause() {
      return null;
    }
  }

  /**
   * Determines whether it is supposed to retry after the operation has failed.
   *
   * @param event The failed event.
   * @return the action it should take.
   */
  Action handleAttemptFailure(Event event);
}
