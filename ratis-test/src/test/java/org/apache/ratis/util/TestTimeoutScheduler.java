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
package org.apache.ratis.util;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TestTimeoutScheduler extends BaseTest {
  {
    Log4jUtils.setLogLevel(TimeoutScheduler.LOG, Level.ALL);
  }

  static class ErrorHandler implements Consumer<RuntimeException> {
    private final AtomicBoolean hasError = new AtomicBoolean(false);

    @Override
    public void accept(RuntimeException e) {
      hasError.set(true);
      TimeoutScheduler.LOG.error("Failed", e);
    }

    void assertNoError() {
      Assert.assertFalse(hasError.get());
    }
  }

  @Test(timeout = 1000)
  public void testSingleTask() throws Exception {
    final TimeoutScheduler scheduler = TimeoutScheduler.newInstance();
    final TimeDuration grace = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    scheduler.setGracePeriod(grace);
    Assert.assertFalse(scheduler.hasScheduler());

    final ErrorHandler errorHandler = new ErrorHandler();

    final AtomicBoolean fired = new AtomicBoolean(false);
    scheduler.onTimeout(TimeDuration.valueOf(250, TimeUnit.MILLISECONDS), () -> {
      Assert.assertFalse(fired.get());
      fired.set(true);
    }, errorHandler);
    Assert.assertTrue(scheduler.hasScheduler());

    Thread.sleep(100);
    Assert.assertFalse(fired.get());
    Assert.assertTrue(scheduler.hasScheduler());

    Thread.sleep(100);
    Assert.assertFalse(fired.get());
    Assert.assertTrue(scheduler.hasScheduler());

    Thread.sleep(100);
    Assert.assertTrue(fired.get());
    Assert.assertTrue(scheduler.hasScheduler());

    Thread.sleep(100);
    Assert.assertTrue(fired.get());
    Assert.assertFalse(scheduler.hasScheduler());

    errorHandler.assertNoError();
    scheduler.setGracePeriod(grace);
  }

  @Test(timeout = 1000)
  public void testMultipleTasks() throws Exception {
    final TimeoutScheduler scheduler = TimeoutScheduler.newInstance();
    final TimeDuration grace = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    scheduler.setGracePeriod(grace);
    Assert.assertFalse(scheduler.hasScheduler());

    final ErrorHandler errorHandler = new ErrorHandler();

    final AtomicBoolean[] fired = new AtomicBoolean[3];
    for(int i = 0; i < fired.length; i++) {
      final AtomicBoolean f = fired[i] = new AtomicBoolean(false);
      scheduler.onTimeout(TimeDuration.valueOf(100*i + 50, TimeUnit.MILLISECONDS), () -> {
        Assert.assertFalse(f.get());
        f.set(true);
      }, errorHandler);
      Assert.assertTrue(scheduler.hasScheduler());
    }

    Thread.sleep(100);
    Assert.assertTrue(fired[0].get());
    Assert.assertFalse(fired[1].get());
    Assert.assertFalse(fired[2].get());
    Assert.assertTrue(scheduler.hasScheduler());

    Thread.sleep(100);
    Assert.assertTrue(fired[0].get());
    Assert.assertTrue(fired[1].get());
    Assert.assertFalse(fired[2].get());
    Assert.assertTrue(scheduler.hasScheduler());

    Thread.sleep(100);
    Assert.assertTrue(fired[0].get());
    Assert.assertTrue(fired[1].get());
    Assert.assertTrue(fired[2].get());
    Assert.assertTrue(scheduler.hasScheduler());

    Thread.sleep(100);
    Assert.assertTrue(fired[0].get());
    Assert.assertTrue(fired[1].get());
    Assert.assertTrue(fired[2].get());
    Assert.assertFalse(scheduler.hasScheduler());

    errorHandler.assertNoError();
  }

  @Test(timeout = 1000)
  public void testExtendingGracePeriod() throws Exception {
    final TimeoutScheduler scheduler = TimeoutScheduler.newInstance();
    final TimeDuration grace = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    scheduler.setGracePeriod(grace);
    Assert.assertFalse(scheduler.hasScheduler());

    final ErrorHandler errorHandler = new ErrorHandler();

    {
      final AtomicBoolean fired = new AtomicBoolean(false);
      scheduler.onTimeout(TimeDuration.valueOf(150, TimeUnit.MILLISECONDS), () -> {
        Assert.assertFalse(fired.get());
        fired.set(true);
      }, errorHandler);
      Assert.assertTrue(scheduler.hasScheduler());

      Thread.sleep(100);
      Assert.assertFalse(fired.get());
      Assert.assertTrue(scheduler.hasScheduler());

      Thread.sleep(100);
      Assert.assertTrue(fired.get());
      Assert.assertTrue(scheduler.hasScheduler());
    }

    {
      // submit another task during grace period
      final AtomicBoolean fired2 = new AtomicBoolean(false);
      scheduler.onTimeout(TimeDuration.valueOf(150, TimeUnit.MILLISECONDS), () -> {
        Assert.assertFalse(fired2.get());
        fired2.set(true);
      }, errorHandler);

      Thread.sleep(100);
      Assert.assertFalse(fired2.get());
      Assert.assertTrue(scheduler.hasScheduler());

      Thread.sleep(100);
      Assert.assertTrue(fired2.get());
      Assert.assertTrue(scheduler.hasScheduler());

      Thread.sleep(100);
      Assert.assertTrue(fired2.get());
      Assert.assertFalse(scheduler.hasScheduler());
    }

    errorHandler.assertNoError();
  }

  @Test(timeout = 1000)
  public void testRestartingScheduler() throws Exception {
    final TimeoutScheduler scheduler = TimeoutScheduler.newInstance();
    final TimeDuration grace = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    scheduler.setGracePeriod(grace);
    Assert.assertFalse(scheduler.hasScheduler());

    final ErrorHandler errorHandler = new ErrorHandler();

    for(int i = 0; i < 2; i++) {
      final AtomicBoolean fired = new AtomicBoolean(false);
      scheduler.onTimeout(TimeDuration.valueOf(150, TimeUnit.MILLISECONDS), () -> {
        Assert.assertFalse(fired.get());
        fired.set(true);
      }, errorHandler);
      Assert.assertTrue(scheduler.hasScheduler());

      Thread.sleep(100);
      Assert.assertFalse(fired.get());
      Assert.assertTrue(scheduler.hasScheduler());

      Thread.sleep(100);
      Assert.assertTrue(fired.get());
      Assert.assertTrue(scheduler.hasScheduler());

      Thread.sleep(100);
      Assert.assertTrue(fired.get());
      Assert.assertFalse(scheduler.hasScheduler());
    }

    errorHandler.assertNoError();
  }

  @Test(timeout = 10_000)
  public void testShutdown() throws Exception {
    final TimeoutScheduler scheduler = TimeoutScheduler.newInstance();
    Assert.assertEquals(TimeoutScheduler.DEFAULT_GRACE_PERIOD, scheduler.getGracePeriod());
    final ErrorHandler errorHandler = new ErrorHandler();

    final int numTasks = 100;
    for(int i = 0; i < numTasks; i++) {
      // long timeout
      scheduler.onTimeout(HUNDRED_MILLIS, () -> {}, errorHandler);
    }
    HUNDRED_MILLIS.sleep();
    HUNDRED_MILLIS.sleep();
    JavaUtils.attempt(() -> Assert.assertEquals(1, scheduler.getQueueSize()),
        10, HUNDRED_MILLIS, "only 1 shutdown task is scheduled", LOG);

    final TimeDuration oneMillis = TimeDuration.valueOf(1, TimeUnit.MILLISECONDS);
    for(int i = 0; i < numTasks; i++) {
      // short timeout
      scheduler.onTimeout(oneMillis, () -> {}, errorHandler);
      oneMillis.sleep();
      oneMillis.sleep();
    }
    HUNDRED_MILLIS.sleep();
    JavaUtils.attempt(() -> Assert.assertEquals(1, scheduler.getQueueSize()),
        10, HUNDRED_MILLIS, "only 1 shutdown task is scheduled", LOG);

    errorHandler.assertNoError();
  }
}
