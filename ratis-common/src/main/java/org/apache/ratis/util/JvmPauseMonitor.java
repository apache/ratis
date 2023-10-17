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

import org.apache.ratis.util.function.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryManagerMXBean;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Detect pauses in JVM causing by GC or other problems in the machine.
 */
public final class JvmPauseMonitor {
  static final Logger LOG = LoggerFactory.getLogger(JvmPauseMonitor.class);
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

  static final class GcInfo {
    private final long count;
    private final long timeMs;

    private GcInfo(GarbageCollectorMXBean gcBean) {
      this(gcBean.getCollectionCount(), gcBean.getCollectionTime());
    }

    private GcInfo(long count, long timeMs) {
      this.count = count;
      this.timeMs = timeMs;
    }

    GcInfo subtract(GcInfo that) {
      return new GcInfo(this.count - that.count, this.timeMs - that.timeMs);
    }

    @Override
    public String toString() {
      return "count=" + count + " time=" + timeMs + "ms";
    }
  }

  static Map<String, GcInfo> getGcTimes() {
    return ManagementFactory.getGarbageCollectorMXBeans().stream()
        .collect(Collectors.toMap(MemoryManagerMXBean::getName, GcInfo::new));
  }

  static String toString(Map<String, GcInfo> beforeSleep, TimeDuration extraSleepTime, Map<String, GcInfo> afterSleep) {
    final StringBuilder b = new StringBuilder();
    long ms = 0;
    for(Map.Entry<String, GcInfo> before: beforeSleep.entrySet()) {
      final String name = before.getKey();
      final GcInfo after = afterSleep.get(name);
      if (after != null) {
        final GcInfo diff = after.subtract(before.getValue());
        if (diff.count != 0) {
          ms += diff.timeMs;
          b.append(System.lineSeparator()).append("GC pool '").append(name)
              .append("' had collection(s): ").append(diff);
        }
      }
    }

    final String gc = b.length() == 0? " without any GCs."
        : " with " + TimeDuration.valueOf(ms, TimeUnit.MILLISECONDS).toString(TimeUnit.SECONDS, 3)
        + " GC time." + b;
    return "Detected pause in JVM or host machine approximately "
        + extraSleepTime.toString(TimeUnit.SECONDS, 3) + gc;
  }

  /** To build {@link JvmPauseMonitor}. */
  public static class Builder {
    private Object name = "default";
    private TimeDuration sleepDeviationThreshold = TimeDuration.valueOf(300, TimeUnit.MILLISECONDS);
    private TimeDuration sleepTime = sleepDeviationThreshold;
    private CheckedConsumer<TimeDuration, IOException> handler = t -> {};

    public Builder setName(Object name) {
      this.name = name;
      return this;
    }

    public Builder setSleepTime(TimeDuration sleepTime) {
      this.sleepTime = sleepTime;
      return this;
    }

    public Builder setSleepDeviationThreshold(TimeDuration sleepDeviationThreshold) {
      this.sleepDeviationThreshold = sleepDeviationThreshold;
      return this;
    }

    public Builder setHandler(CheckedConsumer<TimeDuration, IOException> handler) {
      this.handler = handler;
      return this;
    }

    public JvmPauseMonitor build() {
      return new JvmPauseMonitor(name, sleepTime, sleepDeviationThreshold, handler);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final TimeDuration sleepTime;
  private final TimeDuration sleepDeviationThreshold;

  private final String name;
  private final AtomicReference<Thread> threadRef = new AtomicReference<>();
  private final CheckedConsumer<TimeDuration, IOException> handler;

  private JvmPauseMonitor(Object name, TimeDuration sleepTime, TimeDuration sleepDeviationThreshold,
      CheckedConsumer<TimeDuration, IOException> handler) {
    this.name = JavaUtils.getClassSimpleName(getClass()) + "-" + name;
    // use min -- if the sleep time is too long, it may not be able to detect the given deviation.
    this.sleepTime = TimeDuration.min(sleepTime, sleepDeviationThreshold);
    this.sleepDeviationThreshold = sleepDeviationThreshold;
    this.handler = handler;
  }

  private void run() {
    LOG.info("{}: Started", this);
    try {
      for (; Thread.currentThread().equals(threadRef.get()); ) {
        detectPause();
      }
    } finally {
      LOG.info("{}: Stopped", this);
    }
  }

  private void detectPause() {
    final Map<String, GcInfo> before = getGcTimes();
    final TimeDuration extraSleep;
    try {
      extraSleep = sleepTime.sleep();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return;
    }

    if (extraSleep.compareTo(sleepDeviationThreshold) > 0) {
      final Map<String, GcInfo> after = getGcTimes();
      LOG.warn("{}: {}", this, toString(before, extraSleep, after));
    }

    handle(extraSleep);
  }

  private void handle(TimeDuration extraSleep) {
    try {
      handler.accept(extraSleep);
    } catch (Throwable t) {
      LOG.error("{}: Failed to handle extra sleep {}", this, extraSleep, t);
    }
  }

  /** Start this monitor. */
  public void start() {
    final MemoizedSupplier<Thread> supplier = JavaUtils.memoize(() -> Daemon.newBuilder()
        .setName(JavaUtils.getClassSimpleName(getClass()) + THREAD_COUNT.getAndIncrement())
        .setRunnable(this::run)
        .build());
    Optional.of(threadRef.updateAndGet(previous -> Optional.ofNullable(previous).orElseGet(supplier)))
        .filter(t -> supplier.isInitialized())
        .ifPresent(Thread::start);
  }

  /** Stop this monitor. */
  public void stop() {
    final Thread previous = threadRef.getAndSet(null);
    if (previous != null) {
      previous.interrupt();
      try {
        previous.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
