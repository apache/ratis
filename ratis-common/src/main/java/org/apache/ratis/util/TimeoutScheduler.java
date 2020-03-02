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

import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class TimeoutScheduler implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(TimeoutScheduler.class);

  static final TimeDuration DEFAULT_GRACE_PERIOD = TimeDuration.valueOf(1, TimeUnit.MINUTES);

  private static final Supplier<TimeoutScheduler> INSTANCE = JavaUtils.memoize(TimeoutScheduler::new);

  public static TimeoutScheduler getInstance() {
    return INSTANCE.get();
  }

  static TimeoutScheduler newInstance() {
    return new TimeoutScheduler();
  }

  static class ShutdownTask {
    private final int sid;
    private final ScheduledFuture<?> future;

    ShutdownTask(int sid, ScheduledFuture<?> future) {
      this.sid = sid;
      this.future = future;
    }

    int getSid() {
      return sid;
    }

    void cancel() {
      future.cancel(false);
    }
  }

  private static class Scheduler {
    private final AtomicReference<ScheduledThreadPoolExecutor> executor = new AtomicReference<>();

    boolean hasExecutor() {
      return executor.get() != null;
    }

    int getQueueSize() {
      return Optional.ofNullable(executor.get())
          .map(ScheduledThreadPoolExecutor::getQueue)
          .map(Collection::size).orElse(0);
    }

    ScheduledFuture<?> schedule(Runnable task, Supplier<String> name, TimeDuration time) {
      return executor.updateAndGet(e -> Optional.ofNullable(e).orElseGet(Scheduler::newExecutor))
          .schedule(LogUtils.newRunnable(LOG, task, name), time.getDuration(), time.getUnit());
    }

    private static ScheduledThreadPoolExecutor newExecutor() {
      LOG.debug("new ScheduledThreadPoolExecutor");
      final ScheduledThreadPoolExecutor e = new ScheduledThreadPoolExecutor(1, (ThreadFactory) Daemon::new);
      e.setRemoveOnCancelPolicy(true);
      return e;
    }

    void shutdown() {
      Optional.ofNullable(executor.getAndSet(null)).ifPresent(ScheduledThreadPoolExecutor::shutdown);
    }
  }

  /** When there is no tasks, the time period to wait before shutting down the scheduler. */
  private final AtomicReference<TimeDuration> gracePeriod = new AtomicReference<>(DEFAULT_GRACE_PERIOD);

  /** The number of scheduled tasks. */
  private int numTasks = 0;
  /** The scheduleID for each task */
  private int scheduleID = 0;

  private ShutdownTask shutdownTask = null;

  private final Scheduler scheduler = new Scheduler();

  private TimeoutScheduler() {
  }

  int getQueueSize() {
    return scheduler.getQueueSize();
  }

  TimeDuration getGracePeriod() {
    return gracePeriod.get();
  }

  void setGracePeriod(TimeDuration gracePeriod) {
    this.gracePeriod.set(gracePeriod);
  }

  boolean hasScheduler() {
    return scheduler.hasExecutor();
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
    numTasks++;
    final int sid = scheduleID++;

    LOG.debug("schedule a task: timeout {}, sid {}", timeout, sid);
    scheduler.schedule(() -> toSchedule.accept(sid), () -> "task #" + sid, timeout);
  }

  private synchronized void onTaskCompleted() {
    if (--numTasks > 0) {
      return;
    }
    final int sid = scheduleID;
    if (shutdownTask != null) {
      if (shutdownTask.getSid() == sid) {
        // the shutdown task is still valid
        return;
      }
      // the shutdown task is invalid
      shutdownTask.cancel();
    }

    final TimeDuration grace = getGracePeriod();
    LOG.debug("Schedule a shutdown task: grace {}, sid {}", grace, sid);
    final ScheduledFuture<?> future = scheduler.schedule(() -> tryShutdownScheduler(sid),
        () -> "shutdown task #" + sid, grace);
    shutdownTask = new ShutdownTask(sid, future);
  }

  private synchronized void tryShutdownScheduler(int sid) {
    if (sid == scheduleID) {
      // No new tasks submitted, shutdown the scheduler.
      LOG.debug("shutdown scheduler: sid {}", sid);
      scheduler.shutdown();
    } else {
      LOG.debug("shutdown cancelled: scheduleID has changed from {} to {}", sid, scheduleID);
    }
  }

  /** When timeout, run the task.  Log the error, if there is any. */
  public void onTimeout(TimeDuration timeout, CheckedRunnable<?> task, Logger log, Supplier<String> errorMessage) {
    onTimeout(timeout, task, t -> log.error(errorMessage.get(), t));
  }

  @Override
  public synchronized void close() {
    tryShutdownScheduler(scheduleID);
  }
}
