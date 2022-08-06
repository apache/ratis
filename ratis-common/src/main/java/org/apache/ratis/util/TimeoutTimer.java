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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class TimeoutTimer implements TimeoutExecutor {
  public static final Logger LOG = LoggerFactory.getLogger(TimeoutTimer.class);

  private static final Supplier<TimeoutTimer> INSTANCE = JavaUtils.memoize(() -> new TimeoutTimer(MAXIMUM_POOL_SIZE));

  public static TimeoutTimer getInstance() {
    return INSTANCE.get();
  }

  static class Task extends TimerTask {
    private final int id;
    private final Runnable runnable;

    Task(int id, Runnable runnable) {
      this.id = id;
      this.runnable = LogUtils.newRunnable(LOG, runnable, this::toString);
    }

    @Override
    public void run() {
      LOG.debug("run {}", this);
      runnable.run();
    }

    @Override
    public String toString() {
      return "task #" + id;
    }
  }

  /** The number of scheduled tasks. */
  private final AtomicInteger numTasks = new AtomicInteger();
  /** A unique ID for each task. */
  private final AtomicInteger taskId = new AtomicInteger();

  private final List<MemoizedSupplier<Timer>> timers;

  private TimeoutTimer(int numTimers) {
    final List<MemoizedSupplier<Timer>> list = new ArrayList<>(numTimers);
    for(int i = 0; i < numTimers; i++) {
      final String name = "timer" + i;
      list.add(JavaUtils.memoize(() -> new Timer(name, true)));
    }
    this.timers = Collections.unmodifiableList(list);
  }

  @Override
  public int getTaskCount() {
    return numTasks.get();
  }

  private Timer getTimer(int tid) {
    return timers.get(Math.toIntExact(Integer.toUnsignedLong(tid) % timers.size())).get();
  }

  private void schedule(TimeDuration timeout, Runnable toSchedule) {
    final int tid = taskId.incrementAndGet();
    final int n = numTasks.incrementAndGet();
    LOG.debug("schedule a task #{} with timeout {}, numTasks={}", tid, timeout, n);
    getTimer(n).schedule(new Task(tid, toSchedule), timeout.toLong(TimeUnit.MILLISECONDS));
  }

  @Override
  public <THROWABLE extends Throwable> void onTimeout(
      TimeDuration timeout, CheckedRunnable<THROWABLE> task, Consumer<THROWABLE> errorHandler) {
    schedule(timeout, () -> {
      try {
        task.run();
      } catch(Throwable t) {
        errorHandler.accept(JavaUtils.cast(t));
      } finally {
        numTasks.decrementAndGet();
      }
    });
  }
}
