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

import org.apache.ratis.util.ReferenceCountedObject.ReferenceCountedObjectImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A utility to detect leaks from @{@link ReferenceCountedObject}.
 */
final class ReferenceCountedLeakDetector {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceCountedLeakDetector.class);
  // Leak detection is turned off by default.

  private static LeakDetectionMode leakDetectionFactory = LeakDetectionMode.NONE;
  private static LeakDetector leakDetector = null;

  private ReferenceCountedLeakDetector() {
  }

  static synchronized void enableLeakDetector(boolean advanced) {
    leakDetector = new LeakDetector("ReferenceCountedObject");
    leakDetectionFactory = advanced ? LeakDetectionMode.ADVANCED : LeakDetectionMode.SIMPLE;
  }

  static LeakDetectionFactory getLeakDetectionFactory() {
    return leakDetectionFactory;
  }

  interface LeakDetectionFactory {
    <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod);
  }

  private enum LeakDetectionMode implements LeakDetectionFactory {
    /** Leak detector is not enable in production to avoid performance impacts. */
    NONE {
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new ReferenceCountedObjectImpl<>(value, retainMethod, releaseMethod);
      }
    },
    /** Leak detector is enabled to detect leaks. This is intended to use in every tests.
     */
    SIMPLE {
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new SimpleTracingReferenceCountedObject<>(value, retainMethod, releaseMethod, leakDetector);
      }
    },
    /** Leak detector is enabled to detect leaks and report object creation stacktrace as well as every retain and
     * release stacktraces. This has severe impact in performance and only used to debug specific test cases.
     */
    ADVANCED {
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new AdvancedTracingReferenceCountedObject<>(value, retainMethod, releaseMethod, leakDetector);
      }
    };
  }

  private static class SimpleTracingReferenceCountedObject<T> extends ReferenceCountedObjectImpl<T> {
    private UncheckedAutoCloseable leakTracker;

    public SimpleTracingReferenceCountedObject(T value, Runnable retainMethod, Consumer<Boolean> releaseMethod,
        LeakDetector leakDetector) {
      super(value, retainMethod, releaseMethod);
      final Class<?> clazz = value.getClass();
      this.leakTracker = leakDetector.track(this, () -> {
        LOG.warn("LEAK: A {} is not released properly", clazz.getName());
      });
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

  private static class AdvancedTracingReferenceCountedObject<T> extends ReferenceCountedObjectImpl<T> {
    private UncheckedAutoCloseable leakTracker;
    private final List<StackTraceElement[]> retainsTraces;
    private final List<StackTraceElement[]> releaseTraces;

    public AdvancedTracingReferenceCountedObject(T value, Runnable retainMethod, Consumer<Boolean> releaseMethod,
        LeakDetector leakDetector) {
      super(value, retainMethod, releaseMethod);

      StackTraceElement[] createStrace = Thread.currentThread().getStackTrace();
      final Class<?> clazz = value.getClass();
      final List<StackTraceElement[]> localRetainsTraces = new LinkedList<>();
      final List<StackTraceElement[]> localReleaseTraces = new LinkedList<>();

      this.leakTracker = leakDetector.track(this, () -> {
        LOG.warn("LEAK: A {} is not released properly.\nCreation trace\n{}\nRetain traces:\n{}\nRelease traces\n{}",
            clazz.getName(), formatStackTrace(createStrace, 3), formatStackTraces(localRetainsTraces, 2),
            formatStackTraces(localReleaseTraces, 2));
      });

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
