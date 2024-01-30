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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A utility to detect leaks from @{@link ReferenceCountedObject}.
 */
public final class ReferenceCountedLeakDetector {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceCountedLeakDetector.class);
  // Leak detection is turned off by default.

  private static final AtomicReference<Mode> FACTORY = new AtomicReference<>(Mode.NONE);
  private static final Supplier<LeakDetector> SUPPLIER
      = MemoizedSupplier.valueOf(() -> new LeakDetector(FACTORY.get().name()).start());

  static Factory getFactory() {
    return FACTORY.get();
  }

  public static LeakDetector getLeakDetector() {
    return SUPPLIER.get();
  }

  private ReferenceCountedLeakDetector() {
  }

  static synchronized void enable(boolean advanced) {
    FACTORY.set(advanced ? Mode.ADVANCED : Mode.SIMPLE);
  }

  interface Factory {
    <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod);
  }

  private enum Mode implements Factory {
    /** Leak detector is not enable in production to avoid performance impacts. */
    NONE {
      @Override
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new Impl<>(value, retainMethod, releaseMethod);
      }
    },
    /** Leak detector is enabled to detect leaks. This is intended to use in every tests. */
    SIMPLE {
      @Override
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new SimpleTracing<>(value, retainMethod, releaseMethod, getLeakDetector());
      }
    },
    /**
     * Leak detector is enabled to detect leaks and report object creation stacktrace as well as every retain and
     * release stacktraces. This has severe impact in performance and only used to debug specific test cases.
     */
    ADVANCED {
      @Override
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new AdvancedTracing<>(value, retainMethod, releaseMethod, getLeakDetector());
      }
    }
  }

  private static class Impl<V> implements ReferenceCountedObject<V> {
    private final AtomicInteger count;
    private final V value;
    private final Runnable retainMethod;
    private final Consumer<Boolean> releaseMethod;

    Impl(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
      this.value = value;
      this.retainMethod = retainMethod;
      this.releaseMethod = releaseMethod;
      count = new AtomicInteger();
    }

    @Override
    public V get() {
      final int previous = count.get();
      if (previous < 0) {
        throw new IllegalStateException("Failed to get: object has already been completely released.");
      } else if (previous == 0) {
        throw new IllegalStateException("Failed to get: object has not yet been retained.");
      }
      return value;
    }

    @Override
    public V retain() {
      // n <  0: exception
      // n >= 0: n++
      if (count.getAndUpdate(n -> n < 0? n : n + 1) < 0) {
        throw new IllegalStateException("Failed to retain: object has already been completely released.");
      }

      retainMethod.run();
      return value;
    }

    @Override
    public boolean release() {
      // n <= 0: exception
      // n >  1: n--
      // n == 1: n = -1
      final int previous = count.getAndUpdate(n -> n <= 1? -1: n - 1);
      if (previous < 0) {
        throw new IllegalStateException("Failed to release: object has already been completely released.");
      } else if (previous == 0) {
        throw new IllegalStateException("Failed to release: object has not yet been retained.");
      }
      final boolean completedReleased = previous == 1;
      releaseMethod.accept(completedReleased);
      return completedReleased;
    }
  }

  private static class SimpleTracing<T> extends Impl<T> {
    private final UncheckedAutoCloseable leakTracker;

    SimpleTracing(T value, Runnable retainMethod, Consumer<Boolean> releaseMethod, LeakDetector leakDetector) {
      super(value, retainMethod, releaseMethod);
      final Class<?> clazz = value.getClass();
      this.leakTracker = leakDetector.track(this,
          () -> LOG.warn("LEAK: A {} is not released properly", clazz.getName()));
    }

    @Override
    public boolean release() {
      boolean released = super.release();
      if (released) {
        leakTracker.close();
      }
      return released;
    }
  }

  private static class AdvancedTracing<T> extends Impl<T> {
    private final UncheckedAutoCloseable leakTracker;
    private final List<StackTraceElement[]> retainsTraces;
    private final List<StackTraceElement[]> releaseTraces;

    AdvancedTracing(T value, Runnable retainMethod, Consumer<Boolean> releaseMethod, LeakDetector leakDetector) {
      super(value, retainMethod, releaseMethod);

      StackTraceElement[] createStrace = Thread.currentThread().getStackTrace();
      final Class<?> clazz = value.getClass();
      final List<StackTraceElement[]> localRetainsTraces = new LinkedList<>();
      final List<StackTraceElement[]> localReleaseTraces = new LinkedList<>();

      this.leakTracker = leakDetector.track(this, () ->
          LOG.warn("LEAK: A {} is not released properly.\nCreation trace:\n{}\n" +
              "Retain traces({}):\n{}\nRelease traces({}):\n{}",
              clazz.getName(), formatStackTrace(createStrace, 3),
              localRetainsTraces.size(), formatStackTraces(localRetainsTraces, 2),
              localReleaseTraces.size(), formatStackTraces(localReleaseTraces, 2)));

      this.retainsTraces = localRetainsTraces;
      this.releaseTraces = localReleaseTraces;
    }

    @Override
    public T retain() {
      T retain = super.retain();
      retainsTraces.add(Thread.currentThread().getStackTrace());
      return retain;
    }

    @Override
    public boolean release() {
      boolean released = super.release();
      if (released) {
        leakTracker.close();
      }
      releaseTraces.add(Thread.currentThread().getStackTrace());
      return released;
    }
  }

  private static String formatStackTrace(StackTraceElement[] stackTrace, int startIdx) {
    final StringBuilder sb = new StringBuilder();
    for (int line = startIdx; line < stackTrace.length; line++) {
      sb.append(stackTrace[line]).append("\n");
    }
    return sb.toString();
  }

  private static String formatStackTraces(List<StackTraceElement[]> stackTraces, int startIdx) {
    final StringBuilder sb = new StringBuilder();
    stackTraces.forEach(stackTrace -> {
      if (sb.length() > 0) {
        sb.append("\n");
      }
      for (int line = startIdx; line < stackTrace.length; line++) {
        sb.append(stackTrace[line]).append("\n");
      }
    });
    return sb.toString();
  }
}
