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

import org.apache.ratis.util.function.CheckedSupplier;

import java.util.Objects;

/**
 * This is the base class for the memoized subclass such as
 * {@link MemoizedSupplier}, {@link MemoizedFunction}, {@link MemoizedCheckedSupplier}, etc,
 * The subclasses provide its own method to retrieve the value,
 * such as {@link MemoizedSupplier#get()} and {@link MemoizedFunction#apply(Object)}.
 * The subclass method returns a value by invoking its initializer once at the first call
 * and then keeps returning the same value for the subsequent calls.
 * <p>
 * All the subclasses are thread safe.
 *
 * @param <RETURN> The value type.
 * @param <THROW> The throwable type of the initializer.
 */
abstract class MemoizedBase<RETURN, THROW extends Throwable> {
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile RETURN value = null;

  final RETURN init(CheckedSupplier<RETURN, THROW> initializer) throws THROW {
    final RETURN initialized = value;
    if (initialized != null) {
      return initialized;
    }

    synchronized (this) {
      if (value == null) {
        value = initializer.get();
        Objects.requireNonNull(value, "initializer.get() returns null");
      }
      return value;
    }
  }

  /** @return is the object initialized? */
  public final boolean isInitialized() {
    return value != null;
  }

  /**
   * @return the value, which must be already initialized.
   * @throws NullPointerException if the value is uninitialized.
   */
  public RETURN getInitializedValue() {
    return Objects.requireNonNull(value, "Uninitialized: value == null");
  }

  @Override
  public String toString() {
    return value != null ? "Memoized:" + value : "Uninitialized";
  }
}
