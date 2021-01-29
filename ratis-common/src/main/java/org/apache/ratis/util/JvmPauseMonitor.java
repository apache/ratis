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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class JvmPauseMonitor {
  public static final Logger LOG = LoggerFactory.getLogger(JvmPauseMonitor.class);

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
    final StringBuilder b = new StringBuilder("Detected pause in JVM or host machine (eg GC): pause of approximately ")
        .append(extraSleepTime)
        .append('.');

    boolean detected = false;
    for(Map.Entry<String, GcInfo> before: beforeSleep.entrySet()) {
      final String name = before.getKey();
      final GcInfo after = afterSleep.get(name);
      if (after != null) {
        final GcInfo diff = after.subtract(before.getValue());
        if (diff.count != 0) {
          b.append(System.lineSeparator()).append("GC pool '").append(name)
              .append("' had collection(s): ").append(diff);
          detected = true;
        }
      }
    }

    if (!detected) {
      b.append(" No GCs detected.");
    }
    return b.toString();
  }

  private static final TimeDuration SLEEP_TIME = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
  private static final TimeDuration WARN_THRESHOLD = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);

  private final String name;
  private final AtomicReference<Thread> threadRef = new AtomicReference<>();
  private final CheckedConsumer<TimeDuration, IOException> handler;

  public JvmPauseMonitor(Object name, CheckedConsumer<TimeDuration, IOException> handler) {
    this.name = JavaUtils.getClassSimpleName(getClass()) + "-" + name;
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
      extraSleep = SLEEP_TIME.sleep();
    } catch (InterruptedException ie) {
      return;
    }

    if (extraSleep.compareTo(WARN_THRESHOLD) > 0) {
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
    final MemoizedSupplier<Thread> supplier = JavaUtils.memoize(() -> new Daemon(this::run));
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
