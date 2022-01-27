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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Utilities related to concurrent programming.
 */
public interface ConcurrentUtils {
  /**
   * Similar to {@link AtomicReference#updateAndGet(java.util.function.UnaryOperator)}
   * except that the update function is checked.
   */
  static <E, THROWABLE extends Throwable> E updateAndGet(AtomicReference<E> reference,
      CheckedFunction<E, E, THROWABLE> update) throws THROWABLE {
    final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
    final E updated = reference.updateAndGet(value -> {
      try {
        return update.apply(value);
      } catch (Error | RuntimeException e) {
        throw e;
      } catch (Throwable t) {
        throwableRef.set(t);
        return value;
      }
    });
    @SuppressWarnings("unchecked")
    final THROWABLE t = (THROWABLE) throwableRef.get();
    if (t != null) {
      throw t;
    }
    return updated;
  }

  /**
   * Creates a {@link ThreadFactory} so that the threads created by the factory are named with the given name prefix.
   *
   * @param namePrefix the prefix used in the name of the threads created.
   * @return a new {@link ThreadFactory}.
   */
  static ThreadFactory newThreadFactory(String namePrefix) {
    final AtomicInteger numThread = new AtomicInteger();
    return runnable -> {
      final int id = numThread.incrementAndGet();
      final Thread t = new Thread(runnable);
      t.setName(namePrefix + "-thread" + id);
      return t;
    };
  }

  /**
   * The same as {@link java.util.concurrent.Executors#newCachedThreadPool(ThreadFactory)}
   * except that this method takes a maximumPoolSize parameter.
   *
   * @param maximumPoolSize the maximum number of threads to allow in the pool.
   *                        When maximumPoolSize == 0, this method is the same as
   *                        {@link java.util.concurrent.Executors#newCachedThreadPool(ThreadFactory)}.
   * @return a new {@link ExecutorService}.
   */
  static ExecutorService newCachedThreadPool(int maximumPoolSize, ThreadFactory threadFactory) {
    return maximumPoolSize == 0? Executors.newCachedThreadPool(threadFactory)
        : new ThreadPoolExecutor(0, maximumPoolSize, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), threadFactory);
  }

  /**
   * Create a new {@link ExecutorService} with a maximum pool size.
   * If it is cached, this method is similar to {@link #newCachedThreadPool(int, ThreadFactory)}.
   * Otherwise, this method is similar to {@link java.util.concurrent.Executors#newFixedThreadPool(int)}.
   *
   * @param cached Use cached thread pool?  If not, use a fixed thread pool.
   * @param maximumPoolSize the maximum number of threads to allow in the pool.
   * @param namePrefix the prefix used in the name of the threads created.
   * @return a new {@link ExecutorService}.
   */
  static ExecutorService newThreadPoolWithMax(boolean cached, int maximumPoolSize, String namePrefix) {
    final ThreadFactory f = newThreadFactory(namePrefix);
    return cached ? newCachedThreadPool(maximumPoolSize, f)
        : Executors.newFixedThreadPool(maximumPoolSize, f);
  }

  /**
   * Shutdown the given executor and wait for its termination.
   *
   * @param executor The executor to be shut down.
   */
  static void shutdownAndWait(ExecutorService executor) {
    try {
      executor.shutdown();
      Preconditions.assertTrue(executor.awaitTermination(1, TimeUnit.DAYS));
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * The same as collection.parallelStream().forEach(action) except that
   * (1) this method is asynchronous, and
   * (2) an executor can be passed to this method.
   *
   * @param collection The given collection.
   * @param action To act on each element in the collection.
   * @param executor To execute the action.
   * @param <T> The element type.
   *
   * @return a {@link CompletableFuture} that is completed
   *         when the action is completed for each element in the collection.
   *
   * @see Collection#parallelStream()
   * @see java.util.stream.Stream#forEach(Consumer)
   */
  static <T> CompletableFuture<Void> parallelForEachAsync(Collection<T> collection, Consumer<? super T> action,
      Executor executor) {
    final List<CompletableFuture<T>> futures = new ArrayList<>(collection.size());
    collection.forEach(element -> {
      final CompletableFuture<T> f = new CompletableFuture<>();
      futures.add(f);
      executor.execute(() -> {
        action.accept(element);
        f.complete(element);
      });
    });
    return JavaUtils.allOf(futures);
  }
}
