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

import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A reference-counted object can be retained for later use
 * and then be released for returning the resource.
 * <p>
 * - When the object is retained, the reference count is incremented by 1.
 * <p>
 * - When the object is released, the reference count is decremented by 1.
 * <p>
 * - If the object is retained, it must be released afterward.
 *   Otherwise, the object will not be returned, and it will cause a resource leak.
 * <p>
 * - If the object is retained multiple times,
 *   it must be released the same number of times.
 * <p>
 * - If the object has been retained and then completely released (i.e. the reference count becomes 0),
 *   it must not be retained/released/accessed anymore since it may have been allocated for other use.
 *
 * @param <T> The object type.
 */
public interface ReferenceCountedObject<T> {

  /** @return the object. */
  T get();

  /**
   * Retain the object for later use.
   * The reference count will be increased by 1.
   * <p>
   * The {@link #release()} method must be invoked afterward.
   * Otherwise, the object is not returned, and it will cause a resource leak.
   *
   * @return the object.
   */
  T retain();

  /**
   * The same as {@link #retain()} except that this method returns a {@link UncheckedAutoCloseableSupplier}.
   *
   * @return a {@link UncheckedAutoCloseableSupplier}
   *         where {@link java.util.function.Supplier#get()} will return the retained object,
   *         i.e. the object returned by {@link #retain()},
   *         and calling {@link UncheckedAutoCloseable#close()} one or more times
   *         is the same as calling {@link #release()} once (idempotent).
   */
  default UncheckedAutoCloseableSupplier<T> retainAndReleaseOnClose() {
    final T retained = retain();
    final AtomicBoolean closed = new AtomicBoolean();
    return new UncheckedAutoCloseableSupplier<T>() {
      @Override
      public T get() {
        if (closed.get()) {
          throw new IllegalStateException("Already closed");
        }
        return retained;
      }

      @Override
      public void close() {
        if (closed.compareAndSet(false, true)) {
          release();
        }
      }
    };
  }

  /**
   * Release the object.
   * The reference count will be decreased by 1.
   *
   * @return true if the object is completely released (i.e. reference count becomes 0); otherwise, return false.
   */
  boolean release();

  /** The same as wrap(value, EMPTY, EMPTY), where EMPTY is an empty method. */
  static <V> ReferenceCountedObject<V> wrap(V value) {
    return wrap(value, () -> {}, ignored -> {});
  }

  static <T, V> ReferenceCountedObject<V> delegateFrom(Collection<ReferenceCountedObject<T>> fromRefs, V value) {
    return new ReferenceCountedObject<V>() {
      @Override
      public V get() {
        return value;
      }

      @Override
      public V retain() {
        fromRefs.forEach(ReferenceCountedObject::retain);
        return value;
      }

      @Override
      public boolean release() {
        boolean allReleased = true;
        for (ReferenceCountedObject<T> ref : fromRefs) {
          if (!ref.release()) {
            allReleased = false;
          }
        }
        return allReleased;
      }
    };
  }

  /**
   * @return a {@link ReferenceCountedObject} of the given value by delegating to this object.
   */
  default <V> ReferenceCountedObject<V> delegate(V value) {
    final ReferenceCountedObject<T> delegated = this;
    return new ReferenceCountedObject<V>() {
      @Override
      public V get() {
        return value;
      }

      @Override
      public V retain() {
        delegated.retain();
        return value;
      }

      @Override
      public boolean release() {
        return delegated.release();
      }
    };
  }

  /**
   * @return a {@link ReferenceCountedObject} by apply the given function to this object.
   */
  default <V> ReferenceCountedObject<V> apply(Function<T, V> function) {
    return delegate(function.apply(get()));
  }

  /**
   * Wrap the given value as a {@link ReferenceCountedObject}.
   *
   * @param value the value being wrapped.
   * @param retainMethod a method to run when {@link #retain()} is invoked.
   * @param releaseMethod a method to run when {@link #release()} is invoked,
   *                      where the method takes a boolean which is the same as the one returned by {@link #release()}.
   * @param <V> The value type.
   * @return the wrapped reference-counted object.
   */
  static <V> ReferenceCountedObject<V> wrap(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
    Objects.requireNonNull(value, "value == null");
    Objects.requireNonNull(retainMethod, "retainMethod == null");
    Objects.requireNonNull(releaseMethod, "releaseMethod == null");

    return ReferenceCountedLeakDetector.getFactory().create(value, retainMethod, releaseMethod);
  }

  /** The same as wrap(value, retainMethod, ignored -> releaseMethod.run()). */
  static <V> ReferenceCountedObject<V> wrap(V value, Runnable retainMethod, Runnable releaseMethod) {
    return wrap(value, retainMethod, ignored -> releaseMethod.run());
  }
}
