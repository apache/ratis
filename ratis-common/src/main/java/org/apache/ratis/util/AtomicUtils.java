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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utilities related to atomic operations.
 */
public interface AtomicUtils {

  /**
   * Updates a AtomicLong which is supposed to maintain the minimum values. This method is not
   * synchronized but is thread-safe.
   */
  static void updateMin(AtomicLong min, long value) {
    while (true) {
      long cur = min.get();
      if (value >= cur) {
        break;
      }

      if (min.compareAndSet(cur, value)) {
        break;
      }
    }
  }

  /**
   * Updates a AtomicLong which is supposed to maintain the maximum values. This method is not
   * synchronized but is thread-safe.
   */
  static void updateMax(AtomicLong max, long value) {
    while (true) {
      long cur = max.get();
      if (value <= cur) {
        break;
      }

      if (max.compareAndSet(cur, value)) {
        break;
      }
    }
  }

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
}
