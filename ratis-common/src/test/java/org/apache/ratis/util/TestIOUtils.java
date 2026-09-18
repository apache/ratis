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

import org.apache.ratis.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;

/** Test methods of {@link IOUtils}. */
public class TestIOUtils extends BaseTest {

  @Test
  public void testReadObjectAllowsThrowable() {
    final IOException original = new IOException("outer", new RuntimeException("inner"));
    final byte[] bytes = IOUtils.object2Bytes(original);
    final Throwable roundTrip = IOUtils.bytes2Object(bytes, Throwable.class);
    Assertions.assertEquals(IOException.class, roundTrip.getClass());
    Assertions.assertEquals("outer", roundTrip.getMessage());
    Assertions.assertNotNull(roundTrip.getCause());
    Assertions.assertEquals("inner", roundTrip.getCause().getMessage());
  }

  @Test
  public void testReadObjectAllowsStackTraceElements() {
    final StackTraceElement[] original = new Throwable().getStackTrace();
    final byte[] bytes = IOUtils.object2Bytes(original);
    final StackTraceElement[] roundTrip = IOUtils.bytes2Object(bytes, StackTraceElement[].class);
    Assertions.assertArrayEquals(original, roundTrip);
  }

  @Test
  public void testReadObjectRejectsDisallowedClass() throws IOException {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(new BigDecimal("3.14"));
    }
    final byte[] bytes = bout.toByteArray();

    final IllegalStateException ise = Assertions.assertThrows(IllegalStateException.class,
        () -> IOUtils.bytes2Object(bytes, Integer.class));
    // The cause should be an InvalidClassException raised by the filter.
    Throwable cause = ise.getCause();
    Assertions.assertNotNull(cause, "expected a cause on " + ise);
    Assertions.assertEquals(InvalidClassException.class, cause.getClass(),
        "unexpected cause: " + cause);
  }
}
