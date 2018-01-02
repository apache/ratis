/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ratis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * General Java utility methods.
 */
public interface JavaUtils {
  Logger LOG = LoggerFactory.getLogger(JavaUtils.class);

  /**
   * The same as {@link Class#cast(Object)} except that
   * this method returns null (but not throw {@link ClassCastException})
   * if the given object is not an instance of the given class.
   */
  static <T> T cast(Object obj, Class<T> clazz) {
    return clazz.isInstance(obj)? clazz.cast(obj): null;
  }

  /**
   * Invoke {@link Callable#call()} and, if there any,
   * wrap the checked exception by {@link RuntimeException}.
   */
  static <T> T callAsUnchecked(Callable<T> callable) {
    try {
      return callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value from the future and then consume it.
   */
  static <T> void getAndConsume(CompletableFuture<T> future, Consumer<T> consumer) {
    final T t;
    try {
      t = future.get();
    } catch (Exception ignored) {
      LOG.warn("Failed to get()", ignored);
      return;
    }
    consumer.accept(t);
  }

  /**
   * Create a memoized supplier which gets a value by invoking the initializer once
   * and then keeps returning the same value as its supplied results.
   *
   * @param initializer to supply at most one non-null value.
   * @param <T> The supplier result type.
   * @return a memoized supplier which is thread-safe.
   */
  static <T> Supplier<T> memoize(Supplier<T> initializer) {
    Objects.requireNonNull(initializer, "initializer == null");
    return new Supplier<T>() {
      private volatile T value = null;

      @Override
      public T get() {
        T v = value;
        if (v == null) {
          synchronized (this) {
            v = value;
            if (v == null) {
              v = value = Objects.requireNonNull(initializer.get(),
                  "initializer.get() returns null");
            }
          }
        }
        return v;
      }
    };
  }

  Supplier<ThreadGroup> ROOT_THREAD_GROUP = memoize(() -> {
    for (ThreadGroup g = Thread.currentThread().getThreadGroup(), p;; g = p) {
      if ((p = g.getParent()) == null) {
        return g;
      }
    }
  });

  static ThreadGroup getRootThreadGroup() {
    return ROOT_THREAD_GROUP.get();
  }

  /** Attempt to get a return value from the given supplier multiple times. */
  static <RETURN, THROWABLE extends Throwable> RETURN attempt(
      CheckedSupplier<RETURN, THROWABLE> supplier,
      int numAttempts, long sleepMs, String name, Logger log)
      throws THROWABLE, InterruptedException {
    Objects.requireNonNull(supplier, "supplier == null");
    Preconditions.assertTrue(numAttempts > 0, () -> "numAttempts = " + numAttempts + " <= 0");
    Preconditions.assertTrue(sleepMs >= 0L, () -> "sleepMs = " + sleepMs + " < 0");

    for(int i = 1; i <= numAttempts; i++) {
      try {
        return supplier.get();
      } catch (Throwable t) {
        if (i == numAttempts) {
          throw t;
        }
        if (log != null && log.isWarnEnabled()) {
          log.warn("FAILED " + name + " attempt #" + i + "/" + numAttempts
              + ": " + t + ", sleep " + sleepMs + "ms and then retry.", t);
        }
      }

      if (sleepMs > 0) {
        Thread.sleep(sleepMs);
      }
    }
    throw new IllegalStateException("BUG: this line should be unreachable.");
  }

  /** Attempt to run the given op multiple times. */
  static <THROWABLE extends Throwable> void attempt(
      CheckedRunnable<THROWABLE> op, int numAttempts, long sleepMs, String name, Logger log)
      throws THROWABLE, InterruptedException {
    attempt(CheckedSupplier.valueOf(op), numAttempts, sleepMs, name, log);
  }

  /** Attempt to wait the given condition to return true multiple times. */
  static void attempt(
      BooleanSupplier condition, int numAttempts, long sleepMs, String name, Logger log)
      throws InterruptedException {
    Objects.requireNonNull(condition, "condition == null");
    Preconditions.assertTrue(numAttempts > 0, () -> "numAttempts = " + numAttempts + " <= 0");
    Preconditions.assertTrue(sleepMs >= 0L, () -> "sleepMs = " + sleepMs + " < 0");

    for(int i = 1; i <= numAttempts; i++) {
      if (condition.getAsBoolean()) {
        return;
      }
      if (log != null && log.isWarnEnabled()) {
        log.warn("FAILED " + name + " attempt #" + i + "/" + numAttempts
            + ": sleep " + sleepMs + "ms and then retry.");
      }
      if (sleepMs > 0) {
        Thread.sleep(sleepMs);
      }
    }

    if (!condition.getAsBoolean()) {
      throw new IllegalStateException("Failed " + name + " for " + numAttempts + " attempts.");
    }
  }

  static Timer runRepeatedly(Runnable runnable, long delay, long period, TimeUnit unit) {
    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        runnable.run();
      }
    }, unit.toMillis(delay), unit.toMillis(period));

    return timer;
  }

  static void dumpAllThreads(Consumer<String> println) {
    final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo ti : threadMxBean.dumpAllThreads(true, true)) {
      println.accept(ti.toString());
    }
  }

  static <E> CompletableFuture<E> completeExceptionally(Throwable t) {
    final CompletableFuture<E> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }

  static Throwable unwrapCompletionException(Throwable t) {
    Objects.requireNonNull(t, "t == null");
    return t instanceof CompletionException && t.getCause() != null? t.getCause(): t;
  }
}
