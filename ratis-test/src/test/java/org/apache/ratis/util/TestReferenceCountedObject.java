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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestReferenceCountedObject {
  static void assertValues(
      AtomicInteger retained, int expectedRetained,
      AtomicInteger released, int expectedReleased) {
    Assertions.assertEquals(expectedRetained, retained.get(), "retained");
    Assertions.assertEquals(expectedReleased, released.get(), "retained");
  }

  static void assertRelease(ReferenceCountedObject<?> ref,
      AtomicInteger retained, int expectedRetained,
      AtomicInteger released, int expectedReleased) {
    final boolean completelyReleased = ref.release();
    assertValues(retained, expectedRetained, released, expectedReleased);
    Assertions.assertEquals(expectedRetained == expectedReleased, completelyReleased);
  }

  static void runTestWrapper(String value, ReferenceCountedObject<String> ref,
      AtomicInteger retained, AtomicInteger released) {
    assertValues(retained, 0, released, 0);
    try {
      ref.get();
      Assertions.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }
    assertValues(retained, 0, released, 0);

    Assertions.assertEquals(value, ref.retain());
    assertValues(retained, 1, released, 0);

    try(UncheckedAutoCloseableSupplier<String> auto = ref.retainAndReleaseOnClose()) {
      final String got = auto.get();
      Assertions.assertEquals(value, got);
      Assertions.assertSame(got, auto.get()); // it should return the same object.
      assertValues(retained, 2, released, 0);
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }
    assertValues(retained, 2, released, 1);

    final UncheckedAutoCloseableSupplier<String> notClosing = ref.retainAndReleaseOnClose();
    Assertions.assertEquals(value, notClosing.get());
    assertValues(retained, 3, released, 1);
    assertRelease(ref, retained, 3, released, 2);

    final UncheckedAutoCloseableSupplier<String> auto = ref.retainAndReleaseOnClose();
    Assertions.assertEquals(value, auto.get());
    assertValues(retained, 4, released, 2);
    auto.close();
    assertValues(retained, 4, released, 3);
    auto.close();  // close() is idempotent.
    assertValues(retained, 4, released, 3);

    // completely released
    assertRelease(ref, retained, 4, released, 4);

    try {
      ref.get();
      Assertions.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    assertValues(retained, 4, released, 4);
  }

  @Test
  @Timeout(value = 1)
  public void testValueWrapper() {
    final String value = "testValue";
    final AtomicInteger retained = new AtomicInteger();
    final AtomicInteger released = new AtomicInteger();
    final ReferenceCountedObject<String> ref = ReferenceCountedObject.<String>newBuilder()
        .setValue(value)
        .setRetainMethod(retained::getAndIncrement)
        .setReleaseMethod(released::getAndIncrement)
        .build();

    runTestWrapper(value, ref, retained, released);

    // for the ValueWrapper, it cannot be retained/released after it is completely released
    try {
      ref.retain();
      Assertions.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try(UncheckedAutoCloseable ignore = ref.retainAndReleaseOnClose()) {
      Assertions.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try {
      ref.release();
      Assertions.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }
  }

  @Test
  @Timeout(value = 1)
  public void testConstructorWrapper() {
    final String prefix = "constructor";
    final AtomicInteger valueCount = new AtomicInteger();
    final AtomicInteger retained = new AtomicInteger();
    final AtomicInteger released = new AtomicInteger();
    final ReferenceCountedObject<String> ref = ReferenceCountedObject.<String>newBuilder()
        .setConstructor(() -> prefix + valueCount.getAndIncrement())
        .setRetainMethod(retained::getAndIncrement)
        .setReleaseMethod(released::getAndIncrement)
        .build();
    Assertions.assertEquals(0, valueCount.get());
    runTestWrapper(prefix + valueCount, ref, retained, released);
    Assertions.assertEquals(1, valueCount.get());

    // for the ConstructorWrapper, it can be retained/released after it is completely released
    retained.set(0);
    released.set(0);
    runTestWrapper(prefix + valueCount, ref, retained, released);
    Assertions.assertEquals(2, valueCount.get());
  }

  @Test
  @Timeout(value = 1)
  public void testBuilder() {
    // Do not set value and constructor
    assertThrows(NullPointerException.class, () -> ReferenceCountedObject.newBuilder().build());

    // Set both value and constructor
    assertThrows(IllegalStateException.class, () -> ReferenceCountedObject.newBuilder()
        .setValue("")
        .setConstructor(() -> "")
        .build());
  }

  @Test
  @Timeout(value = 1)
  public void testReleaseWithoutRetaining() {
    final ReferenceCountedObject<Object> ref = ReferenceCountedObject.newBuilder().setValue("").build();

    try {
      ref.release();
      Assertions.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try {
      ref.get();
      Assertions.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try {
      ref.retain();
      Assertions.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try(UncheckedAutoCloseable ignore = ref.retainAndReleaseOnClose()) {
      Assertions.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }
  }
}
