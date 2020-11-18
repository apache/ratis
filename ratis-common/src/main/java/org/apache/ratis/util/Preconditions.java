/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public interface Preconditions {
  /**
   * Assert if the given value is true.
   * @param value the value to be asserted.
   * @throws IllegalStateException if the given value is false.
   */
  static void assertTrue(boolean value) {
    if (!value) {
      throw new IllegalStateException();
    }
  }

  /**
   * Assert if the given value is true.
   * @param value the value to be asserted.
   * @param message The exception message.
   * @throws IllegalStateException with the given message if the given value is false.
   */
  static void assertTrue(boolean value, Object message) {
    if (!value) {
      throw new IllegalStateException(String.valueOf(message));
    }
  }

  /**
   * Assert if the given value is true.
   * @param value the value to be asserted.
   * @param format exception message format.
   * @param args exception message arguments.
   * @throws IllegalStateException if the given value is false.
   * The exception message is constructed by {@link String#format(String, Object...)}
   * with the given format and arguments.
   */
  static void assertTrue(boolean value, String format, Object... args) {
    if (!value) {
      throw new IllegalStateException(String.format(format, args));
    }
  }

  /**
   * Assert if the given value is true.
   * @param value the value to be asserted.
   * @param message The exception message supplier.
   * @throws IllegalStateException with the given message if the given value is false.
   */
  static void assertTrue(boolean value, Supplier<Object> message) {
    if (!value) {
      throw new IllegalStateException(String.valueOf(message.get()));
    }
  }

  static void assertSame(long expected, long computed, String name) {
    assertTrue(expected == computed,
        () -> name + ": expected == " + expected + " but computed == " + computed);
  }

  static void assertNull(Object object, Supplier<String> message) {
    assertTrue(object == null, message);
  }

  static void assertNull(Object object, String name) {
    assertNull(object, () -> name + " is expected to be null but "
        + name + " = " + object + " != null, class = " + object.getClass());
  }

  static <T> T assertNotNull(T object, Supplier<String> message) {
    assertTrue(object != null, message);
    return object;
  }

  static <T> T assertNotNull(T object, String name) {
    return assertNotNull(object, () -> name + " is expected to not be null but "
        + name + " = " + object + " == null, class = " + object.getClass());
  }

  static <T> T assertInstanceOf(Object object, Class<T> clazz) {
    assertTrue(clazz.isInstance(object),
        () -> "Required instance of " + clazz + " but object.getClass() is " + object.getClass());
    return clazz.cast(object);
  }

  static <T> void assertUnique(Iterable<T> first) {
    assertUnique(first, Collections.emptyList());
  }

  static <T> void assertUnique(Iterable<T> original, Iterable<T> toBeAdded) {
    final Set<T> set = new HashSet<>();
    for(T t : original) {
      assertTrue(set.add(t), () -> "Found duplicated element " + t + " in " + original);
    }
    for(T t : toBeAdded) {
      assertTrue(set.add(t), () -> "Found duplicated element " + t + " when adding " + toBeAdded + " to " + original);
    }
  }
}
