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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
 *   calling {@link #get()} will lead to an {@link IllegalStateException}.
 *   Depending on how the {@link ReferenceCountedObject} is built,
 *   calling {@link #retain()} may or may not be allowed;
 * @see Builder#setValue(Object)
 * @see Builder#setConstructor(Supplier)
 *
 * @param <T> The object type.
 */
public interface ReferenceCountedObject<T> {
  /**
   * @return the object.
   * @throws IllegalStateException when the object has not been retained.
   */
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
          throw new IllegalStateException("Failed to get: already closed");
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

  /**
   * The same as newBuilder().setValue(value).build();
   *
   * @deprecated use {@link Builder}
   */
  @Deprecated
  static <V> ReferenceCountedObject<V> wrap(V value) {
    return ReferenceCountedObject.<V>newBuilder().setValue(value).build();
  }

  /**
   * The same as newBuilder()
   *   .setValue(value)
   *   .setRetainMethod(retainMethod)
   *   .setBooleanReleaseMethod(releaseMethod)
   *   .build();
   *
   * @deprecated use {@link Builder}
   */
  @Deprecated
  static <V> ReferenceCountedObject<V> wrap(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
    return ReferenceCountedObject.<V>newBuilder()
        .setValue(value)
        .setRetainMethod(retainMethod)
        .setBooleanReleaseMethod(releaseMethod)
        .build();
  }

  static <V> Builder<V> newBuilder() {
    return new Builder<>();
  }

  /**
   * To build {@link ReferenceCountedObject},
   * where it may use either a fixed value or a constructor (but not both).
   * @see Builder#setValue(Object)
   * @see Builder#setConstructor(Supplier)
   *
   * @param <V> The type of the {@link ReferenceCountedObject} being built.
   */
  class Builder<V> {
    private V value = null;
    private Supplier<V> constructor = null;
    private Runnable retainMethod = () -> {};
    private Consumer<V> releaseMethod = v -> {};

    /**
     * Set a fixed value for the {@link ReferenceCountedObject} being built.
     * Once it has been completely released, calling {@link #retain()} is not allowed.
     *
     * @param value a fixed value.
     */
    public Builder<V> setValue(V value) {
      this.value = value;
      return this;
    }

    /**
     * Set a constructor for the {@link ReferenceCountedObject} being built.
     * The value is constructed at the first {@link #retain()} call,
     * After it has been completely released by {@link #release()},
     * a new value will be constructed when {@link #retain()} is called again.
     *
     * @param constructor to construct the object.
     */
    public Builder<V> setConstructor(Supplier<V> constructor) {
      this.constructor = constructor;
      return this;
    }

    /**
     * @param retainMethod a method to run when {@link #retain()} is invoked.
     */
    public Builder<V> setRetainMethod(Runnable retainMethod) {
      this.retainMethod = retainMethod != null ? retainMethod : () -> {};
      return this;
    }

    /**
     * @param releaseMethod a method to run when {@link #release()} is invoked,
     *                      where the method has a parameter,
     *                      where the actual parameter is the value when it is completely released;
     *                            otherwise, the actual parameter is null.
     *                      The method may clean up the value when it is completely released.
     */
    public Builder<V> setReleaseMethod(Consumer<V> releaseMethod) {
      this.releaseMethod = releaseMethod != null ? releaseMethod : ignored -> {};
      return this;
    }

    /**
     * @param booleanReleaseMethod a method to run when {@link #release()} is invoked,
     *                      where the method takes the boolean returned from {@link #release()}.
     */
    public Builder<V> setBooleanReleaseMethod(Consumer<Boolean> booleanReleaseMethod) {
      this.releaseMethod = booleanReleaseMethod != null ? v -> booleanReleaseMethod.accept(v != null) : ignored -> {};
      return this;
    }

    /**
     * @param releaseMethod a method to run when {@link #release()} is invoked,
     */
    public Builder<V> setReleaseMethod(Runnable releaseMethod) {
      this.releaseMethod = releaseMethod != null ? ignored -> releaseMethod.run() : ignored -> {};
      return this;
    }

    public ReferenceCountedObject<V> build() {
      if (value == null) {
        Objects.requireNonNull(constructor, "Both value == null and constructor == null");
        return new ConstructorWrapper<>(constructor, retainMethod, releaseMethod);
      } else {
        Preconditions.assertNull(constructor, "Both value != null and constructor != null");
        return new ValueWrapper<>(value, retainMethod, releaseMethod);
      }
    }

    /**
     * Wrap a fixed value as a {@link ReferenceCountedObject}.
     * When it is completely released, it cannot be retained again.
     *
     * @param <V> The value type.
     */
    private static final class ValueWrapper<V> implements ReferenceCountedObject<V> {
      private final V value;
      private final Runnable retainMethod;
      private final Consumer<V> releaseMethod;
      private final AtomicInteger count = new AtomicInteger();

      private ValueWrapper(V value, Runnable retainMethod, Consumer<V> releaseMethod) {
        this.value = Objects.requireNonNull(value, "value == null");
        this.retainMethod = Objects.requireNonNull(retainMethod, "retainMethod == null");
        this.releaseMethod = Objects.requireNonNull(releaseMethod, "releaseMethod == null");
      }

      @Override
      public V get() {
        final int previous = count.get();
        if (previous < 0) {
          throw new IllegalStateException("Failed to get: already completely released.");
        } else if (previous == 0) {
          throw new IllegalStateException("Failed to get: not yet retained.");
        }
        return value;
      }

      @Override
      public V retain() {
        // n <  0: exception
        // n >= 0: n++
        if (count.getAndUpdate(n -> n < 0? n : n + 1) < 0) {
          throw new IllegalStateException("Failed to retain: already completely released.");
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
          throw new IllegalStateException("Failed to release: already completely released.");
        } else if (previous == 0) {
          throw new IllegalStateException("Failed to release: not yet retained.");
        }
        final boolean completelyReleased = previous == 1;
        releaseMethod.accept(completelyReleased ? value : null);
        return completelyReleased;
      }
    }

    /**
     * Wrap a constructor as a {@link ReferenceCountedObject}.
     *
     * @see Builder#setConstructor(Supplier)
     */
    private static final class ConstructorWrapper<V> implements ReferenceCountedObject<V> {
      private final Supplier<V> constructor;
      private final Runnable retainMethod;
      private final Consumer<V> releaseMethod;
      private ValueWrapper<V> valueWrapper;

      private ConstructorWrapper(Supplier<V> constructor, Runnable retainMethod, Consumer<V> releaseMethod) {
        this.constructor = Objects.requireNonNull(constructor, "constructor == null");
        this.retainMethod = Objects.requireNonNull(retainMethod, "retainMethod == null");
        this.releaseMethod = Objects.requireNonNull(releaseMethod, "releaseMethod == null");
      }

      @Override
      public synchronized V get() {
        if (valueWrapper == null) {
          throw new IllegalStateException("Failed to get: not yet retained.");
        }
        return valueWrapper.get();
      }

      @Override
      public synchronized V retain() {
        if (valueWrapper == null) {
          valueWrapper = new ValueWrapper<>(constructor.get(), retainMethod, releaseMethod);
        }
        return valueWrapper.retain();
      }

      @Override
      public synchronized boolean release() {
        if (valueWrapper == null) {
          throw new IllegalStateException("Failed to release: not yet retained.");
        }
        if (valueWrapper.release()) {
          valueWrapper = null;
          return true;
        }
        return false;
      }
    }
  }

  /**
   * The same as newBuilder()
   *   .setValue(value)
   *   .setRetainMethod(retainMethod)
   *   .setReleaseMethod(releaseMethod)
   *   .build();
   *
   * @deprecated use {@link Builder}
   */
  @Deprecated
  static <V> ReferenceCountedObject<V> wrap(V value, Runnable retainMethod, Runnable releaseMethod) {
    return ReferenceCountedObject.<V>newBuilder()
        .setValue(value)
        .setRetainMethod(retainMethod)
        .setReleaseMethod(releaseMethod)
        .build();
  }
}
