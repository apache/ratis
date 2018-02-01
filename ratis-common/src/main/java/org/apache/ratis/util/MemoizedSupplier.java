/**
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

import java.util.Objects;
import java.util.function.Supplier;

/**
 * A memoized supplier is a {@link Supplier}
 * which gets a value by invoking its initializer once
 * and then keeps returning the same value as its supplied results.
 *
 * This class is thread safe.
 *
 * @param <T> The supplier result type.
 */
public class MemoizedSupplier<T> implements Supplier<T> {
  /**
   * @param supplier to supply at most one non-null value.
   * @return a {@link MemoizedSupplier} with the given supplier.
   */
  public static <T> MemoizedSupplier<T> valueOf(Supplier<T> supplier) {
    return supplier instanceof MemoizedSupplier ?
        (MemoizedSupplier<T>) supplier : new MemoizedSupplier<>(supplier);
  }

  private final Supplier<T> initializer;
  private volatile T value = null;

  /**
   * Create a memoized supplier.
   * @param initializer to supply at most one non-null value.
   */
  private MemoizedSupplier(Supplier<T> initializer) {
    Objects.requireNonNull(initializer, "initializer == null");
    this.initializer = initializer;
  }

  /** @return the lazily initialized object. */
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

  /** @return is the object initialized? */
  public boolean isInitialized() {
    return value != null;
  }
}
