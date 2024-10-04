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

import java.util.ArrayList;
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

  public static synchronized void enable(boolean advanced) {
    FACTORY.set(advanced ? Mode.ADVANCED : Mode.SIMPLE);
  }

  interface Factory {
    <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod);
  }

  private enum Mode implements Factory {
    /**
     * Leak detector is not enable in production to avoid performance impacts.
     */
    NONE {
      @Override
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new Impl<>(value, retainMethod, releaseMethod);
      }
    },
    /**
     * Leak detector is enabled to detect leaks. This is intended to use in every tests.
     */
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

    final int getCount() {
      return count.get();
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
      final int previous = count.getAndUpdate(n -> n <= 1 ? -1 : n - 1);
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
    private final LeakDetector leakDetector;
    private final Class<?> valueClass;

    private Runnable removeMethod = null;
    private String valueString = null;

    SimpleTracing(T value, Runnable retainMethod, Consumer<Boolean> releaseMethod, LeakDetector leakDetector) {
      super(value, retainMethod, releaseMethod);
      this.valueClass = value.getClass();
      this.leakDetector = leakDetector;
    }

    String getTraceString(int count) {
      return "(" + valueClass + ", count=" + count + ", value=" + valueString + ")";
    }

    /**
     * @return the leak message if there is a leak; return null if there is no leak.
     */
    String logLeakMessage() {
      final int count = getCount();
      if (count == 0) {
        return null;
      }
      final String message = "LEAK: " + getTraceString(count);
      LOG.warn(message);
      return message;
    }

    @Override
    public synchronized T retain() {
      if (getCount() == 0) {
        this.removeMethod = leakDetector.track(this, this::logLeakMessage);
      }
      try {
        final T value = super.retain();
        if (getCount() == 0) {
          this.valueString = value.toString();
        }
        return value;
      } catch (Exception e) {
        throw new IllegalStateException("Failed to retain: " + getTraceString(getCount()), e);
      }
    }

    @Override
    public synchronized boolean release() {
      final boolean released;
      try {
        released = super.release();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to release: " + getTraceString(getCount()), e);
      }

      if (released) {
        Preconditions.assertNotNull(removeMethod, () -> "Not yet retained (removeMethod == null): " + valueClass);
        removeMethod.run();
      }
      return released;
    }
  }

  private static class AdvancedTracing<T> extends SimpleTracing<T> {
    enum Op {CREATION, RETAIN, RELEASE}

    static class Counts {
      private final int refCount;
      private final int retainCount;
      private final int releaseCount;

      Counts() {
        this.refCount = 0;
        this.retainCount = 0;
        this.releaseCount = 0;
      }

      Counts(Op op, Counts previous) {
        if (op == Op.RETAIN) {
          this.refCount = previous.refCount + 1;
          this.retainCount = previous.retainCount + 1;
          this.releaseCount = previous.releaseCount;
        } else if (op == Op.RELEASE) {
          this.refCount = previous.refCount - 1;
          this.retainCount = previous.retainCount;
          this.releaseCount = previous.releaseCount + 1;
        } else {
          throw new IllegalStateException("Unexpected op: " + op);
        }
      }

      @Override
      public String toString() {
        return "refCount=" + refCount
            + ", retainCount=" + retainCount
            + ", releaseCount=" + releaseCount;
      }
    }

    static class TraceInfo {
      private final int id;
      private final Op op;
      private final int previousRefCount;
      private final Counts counts;

      private final StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
      private final int newTraceElementIndex;

      TraceInfo(int id, Op op, TraceInfo previous, int previousRefCount) {
        this.id = id;
        this.op = op;
        this.previousRefCount = previousRefCount;
        this.counts = previous == null? new Counts(): new Counts(op, previous.counts);
        this.newTraceElementIndex = previous == null? stackTraces.length - 1
            : findFirstUnequalFromTail(this.stackTraces, previous.stackTraces);
      }

      static <T> int findFirstUnequalFromTail(T[] current, T[] previous) {
        int c = current.length - 1;
        for(int p = previous.length - 1; p >= 0; p--, c--) {
          if (!previous[p].equals(current[c])) {
            return c;
          }
        }
        return -1;
      }

      private StringBuilder appendTo(StringBuilder b) {
        b.append(id).append(": ").append(op)
            .append(", previousRefCount=").append(previousRefCount)
            .append(", ").append(counts).append("\n");
        final int n = newTraceElementIndex + 1;
        int line = 3;
        for (; line <= n && line < stackTraces.length; line++) {
          b.append("    ").append(stackTraces[line]).append("\n");
        }
        if (line < stackTraces.length) {
          b.append("    ...\n");
        }
        return b;
      }

      @Override
      public String toString() {
        return appendTo(new StringBuilder()).toString();
      }
    }

    private final List<TraceInfo> traceInfos = new ArrayList<>();
    private int traceCount = 0;
    private TraceInfo previous;

    AdvancedTracing(T value, Runnable retainMethod, Consumer<Boolean> releaseMethod, LeakDetector leakDetector) {
      super(value, retainMethod, releaseMethod, leakDetector);
      addTraceInfo(Op.CREATION);
    }

    private synchronized TraceInfo addTraceInfo(Op op) {
      final TraceInfo current = new TraceInfo(traceCount++, op, previous, getCount());
      traceInfos.add(current);
      previous = current;
      return current;
    }

    @Override
    public synchronized T retain() {
      final T retained = super.retain();
      final TraceInfo info = addTraceInfo(Op.RETAIN);
      Preconditions.assertSame(getCount(), info.counts.refCount, "refCount");
      return retained;
    }

    @Override
    public synchronized boolean release() {
      final boolean released = super.release();
      final TraceInfo info = addTraceInfo(Op.RELEASE);
      final int count = getCount();
      final int expected = count == -1? 0 : count;
      Preconditions.assertSame(expected, info.counts.refCount, "refCount");
      return released;
    }

    @Override
    synchronized String getTraceString(int count) {
      return super.getTraceString(count) + formatTraceInfos(traceInfos);
    }

    private static String formatTraceInfos(List<TraceInfo> infos) {
      final StringBuilder b = new StringBuilder().append(infos.size()).append(" TraceInfo(s):\n");
      infos.forEach(info -> info.appendTo(b.append("\n")));
      return b.toString();
    }
  }
}