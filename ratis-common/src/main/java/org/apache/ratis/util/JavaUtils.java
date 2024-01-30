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

import org.apache.ratis.util.function.CheckedFunction;
import org.apache.ratis.util.function.CheckedRunnable;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * General Java utility methods.
 */
public interface JavaUtils {
  Logger LOG = LoggerFactory.getLogger(JavaUtils.class);

  CompletableFuture<?>[] EMPTY_COMPLETABLE_FUTURE_ARRAY = {};

  ConcurrentMap<Class<?>, String> CLASS_SIMPLE_NAMES = new ConcurrentHashMap<>();
  static String getClassSimpleName(Class<?> clazz) {
    return CLASS_SIMPLE_NAMES.computeIfAbsent(clazz, Class::getSimpleName);
  }

  static String date() {
    return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS").format(new Date());
  }

  /**
   * The same as {@link Class#cast(Object)} except that
   * this method returns null (but not throw {@link ClassCastException})
   * if the given object is not an instance of the given class.
   */
  static <T> T cast(Object obj, Class<T> clazz) {
    return clazz.isInstance(obj)? clazz.cast(obj): null;
  }

  static <T> T cast(Object obj) {
    @SuppressWarnings("unchecked")
    final T t = (T)obj;
    return t;
  }

  static StackTraceElement getCallerStackTraceElement() {
    final StackTraceElement[] trace = Thread.currentThread().getStackTrace();
    return trace[3];
  }

  static StackTraceElement getCurrentStackTraceElement() {
    final StackTraceElement[] trace = Thread.currentThread().getStackTrace();
    return trace[2];
  }

  static <T extends Throwable> void runAsUnchecked(CheckedRunnable<T> runnable) {
    runAsUnchecked(runnable, RuntimeException::new);
  }

  static <THROWABLE extends Throwable> void runAsUnchecked(
      CheckedRunnable<THROWABLE> runnable, Function<THROWABLE, ? extends RuntimeException> converter) {
    try {
      runnable.run();
    } catch(RuntimeException | Error e) {
      throw e;
    } catch(Throwable t) {
      throw converter.apply(cast(t));
    }
  }

  /**
   * Invoke {@link Callable#call()} and, if there any,
   * wrap the checked exception by {@link RuntimeException}.
   */
  static <T> T callAsUnchecked(Callable<T> callable) {
    return callAsUnchecked(callable::call, RuntimeException::new);
  }

  static <OUTPUT, THROWABLE extends Throwable> OUTPUT callAsUnchecked(
      CheckedSupplier<OUTPUT, THROWABLE> checkedSupplier,
      Function<THROWABLE, ? extends RuntimeException> converter) {
    try {
      return checkedSupplier.get();
    } catch(RuntimeException | Error e) {
      throw e;
    } catch(Throwable t) {
      throw converter.apply(cast(t));
    }
  }

  static <T> T doPrivileged(Supplier<T> action, Function<SecurityException, T> exceptionHandler) {
    try {
      return System.getSecurityManager() == null? action.get()
          : AccessController.doPrivileged((PrivilegedAction<T>) action::get);
    } catch (SecurityException e) {
      return exceptionHandler.apply(e);
    }
  }

  static <T> T doPrivileged(Supplier<T> action, Supplier<String> name) {
    return doPrivileged(action, e -> {
      LOG.warn("Failed to " + name.get(), e);
      return null;
    });
  }

  /**
   * Similar to {@link System#getProperty(String)}
   * except that this method may invoke {@link AccessController#doPrivileged(PrivilegedAction)}
   * if there is a {@link SecurityManager}.
   *
   * @return null if either the property is not set or there is a {@link SecurityException};
   *         otherwise, return system property value.
   */
  static String getSystemProperty(final String key) {
    Preconditions.assertNotNull(key, "key");
    Preconditions.assertTrue(!key.isEmpty(), "key is empty.");
    return doPrivileged(() -> System.getProperty(key), () -> "get system property " + key);
  }

  static String getEnv(String variable) {
    final String value = System.getenv().get(variable);
    LOG.info("ENV: {} = {}", variable, value);
    return value;
  }

  /**
   * Similar to {@link System#setProperty(String, String)}
   * except that this method may invoke {@link AccessController#doPrivileged(PrivilegedAction)}
   * if there is a {@link SecurityManager}.
   * When there is a {@link SecurityException}, this becomes a NOOP.
   */
  static void setSystemProperty(String key, String value) {
    Preconditions.assertNotNull(key, "key");
    Preconditions.assertTrue(!key.isEmpty(), "key is empty.");
    Preconditions.assertNotNull(value, "value");
    doPrivileged(() -> System.setProperty(key, value), () -> "set system property " + key + " to " + value);
  }

  /**
   * Create a memoized supplier which gets a value by invoking the initializer once
   * and then keeps returning the same value as its supplied results.
   *
   * @param initializer to supply at most one non-null value.
   * @param <T> The supplier result type.
   * @return a memoized supplier which is thread-safe.
   */
  static <T> MemoizedSupplier<T> memoize(Supplier<T> initializer) {
    return MemoizedSupplier.valueOf(initializer);
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
  static <RETURN, THROWABLE extends Throwable> RETURN attemptRepeatedly(
      CheckedSupplier<RETURN, THROWABLE> supplier,
      int numAttempts, TimeDuration sleepTime, String name, Logger log)
      throws THROWABLE, InterruptedException {
    return attempt(supplier, numAttempts, sleepTime, () -> name, log);
  }

  /** Attempt to get a return value from the given supplier multiple times. */
  static <RETURN, THROWABLE extends Throwable> RETURN attempt(
      CheckedSupplier<RETURN, THROWABLE> supplier,
      int numAttempts, TimeDuration sleepTime, Supplier<?> name, Logger log)
      throws THROWABLE, InterruptedException {
    return attempt(i -> supplier.get(), numAttempts, sleepTime, name, log);
  }

  static <RETURN, THROWABLE extends Throwable> RETURN attempt(
      CheckedFunction<Integer, RETURN, THROWABLE> attemptMethod,
      int numAttempts, TimeDuration sleepTime, Supplier<?> name, Logger log)
      throws THROWABLE, InterruptedException {
    Objects.requireNonNull(attemptMethod, "attemptMethod == null");
    Preconditions.assertTrue(numAttempts > 0, () -> "numAttempts = " + numAttempts + " <= 0");
    Preconditions.assertTrue(!sleepTime.isNegative(), () -> "sleepTime = " + sleepTime + " < 0");

    for(int i = 1; i <= numAttempts; i++) {
      try {
        return attemptMethod.apply(i);
      } catch (Throwable t) {
        if (i == numAttempts) {
          throw t;
        }
        if (log != null && log.isWarnEnabled()) {
          log.warn("FAILED \"" + name.get() + "\", attempt #" + i + "/" + numAttempts
              + ", sleep " + sleepTime + " and then retry: " + t);
        }
      }

      sleepTime.sleep();
    }
    throw new IllegalStateException("BUG: this line should be unreachable.");
  }

  /** Attempt to run the given op multiple times. */
  static <THROWABLE extends Throwable> void attempt(
      CheckedRunnable<THROWABLE> runnable, int numAttempts, TimeDuration sleepTime, String name, Logger log)
      throws THROWABLE, InterruptedException {
    attemptRepeatedly(CheckedRunnable.asCheckedSupplier(runnable), numAttempts, sleepTime, name, log);
  }

  static Timer runRepeatedly(Runnable runnable, long delay, long period, TimeUnit unit) {
    final Timer timer = new Timer(true);
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

  static boolean isCompletedNormally(CompletableFuture<?> future) {
    return future.isDone() && !future.isCancelled() && !future.isCompletedExceptionally();
  }

  static Throwable unwrapCompletionException(Throwable t) {
    Objects.requireNonNull(t, "t == null");
    return t instanceof CompletionException && t.getCause() != null? t.getCause(): t;
  }

  static <T> CompletableFuture<Void> allOf(Collection<CompletableFuture<T>> futures) {
    if (futures == null || futures.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.allOf(futures.toArray(EMPTY_COMPLETABLE_FUTURE_ARRAY));
  }

  static <V> BiConsumer<V, Throwable> asBiConsumer(CompletableFuture<V> future) {
    return (v, t) -> {
      if (t != null) {
        future.completeExceptionally(t);
      } else {
        future.complete(v);
      }
    };
  }

  static <OUTPUT, THROWABLE extends Throwable> OUTPUT supplyAndWrapAsCompletionException(
      CheckedSupplier<OUTPUT, THROWABLE> supplier) {
    try {
      return supplier.get();
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new CompletionException(t);
    }
  }
}
