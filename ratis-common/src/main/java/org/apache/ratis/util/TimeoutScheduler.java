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
package org.apache.ratis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class TimeoutScheduler {
  public static final Logger LOG = LoggerFactory.getLogger(TimeoutScheduler.class);

  private static final TimeDuration DEFAULT_GRACE_PERIOD = TimeDuration.valueOf(1, TimeUnit.MINUTES);

  public static TimeoutScheduler newInstance(int numThreads) {
    return new TimeoutScheduler(numThreads);
  }

  /** When there is no tasks, the time period to wait before shutting down the scheduler. */
  private final AtomicReference<TimeDuration> gracePeriod = new AtomicReference<>(DEFAULT_GRACE_PERIOD);

  /** The number of scheduled tasks. */
  private int numTasks = 0;
  /** The scheduleID for each task */
  private int scheduleID = 0;

  private final int numThreads;
  private volatile ScheduledExecutorService scheduler = null;

  private TimeoutScheduler(int numThreads) {
    this.numThreads = numThreads;
  }

  public int getNumThreads() {
    final ScheduledExecutorService s = scheduler;
    return s instanceof ScheduledThreadPoolExecutor? ((ScheduledThreadPoolExecutor)s).getCorePoolSize(): numThreads;
  }

  TimeDuration getGracePeriod() {
    return gracePeriod.get();
  }

  void setGracePeriod(TimeDuration gracePeriod) {
    this.gracePeriod.set(gracePeriod);
  }

  synchronized boolean hasScheduler() {
    return scheduler != null;
  }

  /**
   * Schedule a timeout task.
   *
   * @param timeout the timeout value.
   * @param task the task to run when timeout.
   * @param errorHandler to handle the error, if there is any.
   */
  public <THROWABLE extends Throwable> void onTimeout(
      TimeDuration timeout, CheckedRunnable<THROWABLE> task, Consumer<THROWABLE> errorHandler) {
    onTimeout(timeout, sid -> {
      LOG.debug("run a task: sid {}", sid);
      try {
        task.run();
      } catch(Throwable t) {
        errorHandler.accept(JavaUtils.cast(t));
      } finally {
        onTaskCompleted();
      }
    });
  }

  private synchronized void onTimeout(TimeDuration timeout, Consumer<Integer> toSchedule) {
    if (scheduler == null) {
      Preconditions.assertTrue(numTasks == 0);
      LOG.debug("Initialize scheduler");
      scheduler = Executors.newScheduledThreadPool(numThreads, Daemon::new);
    }
    numTasks++;
    final int sid = scheduleID++;

    LOG.debug("schedule a task: timeout {}, sid {}", timeout, sid);
    schedule(scheduler, () -> toSchedule.accept(sid), () -> "task #" + sid, timeout);
  }

  private synchronized void onTaskCompleted() {
    if (--numTasks == 0) {
      final int sid = scheduleID;
      final TimeDuration grace = getGracePeriod();
      LOG.debug("Schedule a shutdown task: grace {}, sid {}", grace, sid);
      schedule(scheduler, () -> tryShutdownScheduler(sid), () -> "shutdown task #" + sid, grace);
    }
  }

  static void schedule(ScheduledExecutorService service, Runnable task, Supplier<String> name, TimeDuration timeDuration) {
    service.schedule(LogUtils.newRunnable(LOG, task, name),
        timeDuration.getDuration(), timeDuration.getUnit());
  }

  private synchronized void tryShutdownScheduler(int sid) {
    if (sid == scheduleID) {
      // No new tasks submitted, shutdown the scheduler.
      LOG.debug("shutdown scheduler: sid {}", sid);
      scheduler.shutdown();
      scheduler = null;
    } else {
      LOG.debug("shutdown cancelled: scheduleID has changed from {} to {}", sid, scheduleID);
    }
  }

  /** When timeout, run the task.  Log the error, if there is any. */
  public void onTimeout(TimeDuration timeout, CheckedRunnable<?> task, Logger log, Supplier<String> errorMessage) {
    onTimeout(timeout, task, t -> log.error(errorMessage.get(), t));
  }
}
