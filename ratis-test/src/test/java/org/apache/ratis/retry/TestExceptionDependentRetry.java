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

import org.apache.ratis.client.ClientRetryEvent;
import org.apache.ratis.protocol.TimeoutIOException;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

/**
 * Class to test {@link ExceptionDependentRetry}.
 */
public class TestExceptionDependentRetry {

  @Test
  public void testExceptionDependentRetrySuccess() {
    ExceptionDependentRetry.Builder builder =
        ExceptionDependentRetry.newBuilder();

    int ioExceptionRetries = 1;
    int timeoutExceptionRetries = 2;
    int defaultExceptionRetries = 5;

    long ioExceptionSleepTime = 1;
    long timeoutExceptionSleepTime = 4;
    long defaultExceptionSleepTime = 10;
    builder.setDefaultPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(defaultExceptionRetries,
        TimeDuration.valueOf(defaultExceptionSleepTime, TimeUnit.SECONDS)));
    builder.setExceptionToPolicy(IOException.class,
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(ioExceptionRetries,
            TimeDuration.valueOf(ioExceptionSleepTime, TimeUnit.SECONDS)));
    builder.setExceptionToPolicy(TimeoutIOException.class,
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(timeoutExceptionRetries,
            TimeDuration.valueOf(timeoutExceptionSleepTime, TimeUnit.SECONDS)));


    ExceptionDependentRetry exceptionDependentRetry = builder.build();

    testException(ioExceptionRetries, ioExceptionSleepTime,
        exceptionDependentRetry, new IOException());
    testException(timeoutExceptionRetries, timeoutExceptionSleepTime,
        exceptionDependentRetry, new TimeoutIOException("time out"));

    // now try with an exception which is not there in the map.
    testException(defaultExceptionRetries, defaultExceptionSleepTime,
        exceptionDependentRetry, new TimeoutException());

  }

  @Test
  public void testExceptionDependentRetryFailureWithExceptionDuplicate() {

    try {
      ExceptionDependentRetry.Builder builder =
          ExceptionDependentRetry.newBuilder();
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      fail("testExceptionDependentRetryFailure failed");
    } catch (Exception ex) {
      Assert.assertEquals(IllegalStateException.class, ex.getClass());
    }

  }

  @Test
  public void testExceptionDependentRetryFailureWithExceptionMappedToNull() {
    try {
      ExceptionDependentRetry.Builder builder =
          ExceptionDependentRetry.newBuilder();
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      builder.setExceptionToPolicy(IOException.class, null);
      fail("testExceptionDependentRetryFailure failed");
    } catch (Exception ex) {
      Assert.assertEquals(IllegalStateException.class, ex.getClass());
    }
  }

  @Test
  public void testExceptionDependentRetryFailureWithNoDefault() {

    try {
      ExceptionDependentRetry.Builder builder =
          ExceptionDependentRetry.newBuilder();
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      builder.build();
      fail("testExceptionDependentRetryFailureWithNoDefault failed");
    } catch (Exception ex) {
      Assert.assertEquals(IllegalStateException.class, ex.getClass());
    }

    try {
      ExceptionDependentRetry.Builder builder =
          ExceptionDependentRetry.newBuilder();
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      builder.setDefaultPolicy(null);
      fail("testExceptionDependentRetryFailureWithNoDefault failed");
    } catch (Exception ex) {
      Assert.assertEquals(IllegalStateException.class, ex.getClass());
    }
  }

  private void testException(int retries, long sleepTime,
      ExceptionDependentRetry exceptionDependentRetry, Exception exception) {
    for (int i = 0; i < retries + 1; i++) {
      RetryPolicy.Action action =
          exceptionDependentRetry.handleAttemptFailure(new ClientRetryEvent(i,
              null, exception));

      final boolean expected = i < retries;
      Assert.assertEquals(expected, action.shouldRetry());
      if (expected) {
        Assert.assertEquals(sleepTime, action.getSleepTime().getDuration());
      } else {
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }
    }
  }
}
