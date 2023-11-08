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
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TestReferenceCountedObject {
  static void assertValues(
      AtomicInteger retained, int expectedRetained,
      AtomicInteger released, int expectedReleased) {
    Assert.assertEquals("retained", expectedRetained, retained.get());
    Assert.assertEquals("released", expectedReleased, released.get());
  }

  static void assertRelease(ReferenceCountedObject<?> ref,
      AtomicInteger retained, int expectedRetained,
      AtomicInteger released, int expectedReleased) {
    final boolean returned = ref.release();
    assertValues(retained, expectedRetained, released, expectedReleased);
    Assert.assertEquals(expectedRetained == expectedReleased, returned);
  }

  @Test(timeout = 1000)
  public void testWrap() {
    final String value = "testWrap";
    final AtomicInteger retained = new AtomicInteger();
    final AtomicInteger released = new AtomicInteger();
    final ReferenceCountedObject<String> ref = ReferenceCountedObject.wrap(
        value, retained::getAndIncrement, released::getAndIncrement);

    assertValues(retained, 0, released, 0);
    try {
      ref.get();
      Assert.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }
    assertValues(retained, 0, released, 0);

    Assert.assertEquals(value, ref.retain());
    assertValues(retained, 1, released, 0);

    try(UncheckedAutoCloseableSupplier<String> auto = ref.retainAndReleaseOnClose()) {
      final String got = auto.get();
      Assert.assertEquals(value, got);
      Assert.assertSame(got, auto.get()); // it should return the same object.
      assertValues(retained, 2, released, 0);
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }
    assertValues(retained, 2, released, 1);

    final UncheckedAutoCloseableSupplier<String> notClosing = ref.retainAndReleaseOnClose();
    Assert.assertEquals(value, notClosing.get());
    assertValues(retained, 3, released, 1);
    assertRelease(ref, retained, 3, released, 2);

    final UncheckedAutoCloseableSupplier<String> auto = ref.retainAndReleaseOnClose();
    Assert.assertEquals(value, auto.get());
    assertValues(retained, 4, released, 2);
    auto.close();
    assertValues(retained, 4, released, 3);
    auto.close();  // close() is idempotent.
    assertValues(retained, 4, released, 3);

    // completely released
    assertRelease(ref, retained, 4, released, 4);

    try {
      ref.get();
      Assert.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try {
      ref.retain();
      Assert.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try(UncheckedAutoCloseable ignore = ref.retainAndReleaseOnClose()) {
      Assert.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try {
      ref.release();
      Assert.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }
  }

  @Test(timeout = 1000)
  public void testReleaseWithoutRetaining() {
    final ReferenceCountedObject<String> ref = ReferenceCountedObject.wrap("");

    try {
      ref.release();
      Assert.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try {
      ref.get();
      Assert.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try {
      ref.retain();
      Assert.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }

    try(UncheckedAutoCloseable ignore = ref.retainAndReleaseOnClose()) {
      Assert.fail();
    } catch (IllegalStateException e) {
      e.printStackTrace(System.out);
    }
  }
}
