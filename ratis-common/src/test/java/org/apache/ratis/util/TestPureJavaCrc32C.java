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
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/** Testing {@link PureJavaCrc32C}. */
public class TestPureJavaCrc32C extends BaseTest {
  static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

  @Test
  public void testByteBuffer() {
    for(int length = 1; length < 1 << 20; length <<= 2) {
      runTestByteBuffer(length - 1);
      runTestByteBuffer(length);
      runTestByteBuffer(length + 1);
    }
  }

  /** Verify if the CRC computed by {@link ByteBuffer}s is the same as the CRC computed by arrays. */
  static void runTestByteBuffer(int length) {
    final byte[] array = new byte[length];
    RANDOM.nextBytes(array);
    final ByteBuffer buffer = ByteBuffer.wrap(array);

    final PureJavaCrc32C arrayCrc = new PureJavaCrc32C();
    final PureJavaCrc32C bufferCrc = new PureJavaCrc32C();
    for (int off = 0; off < array.length; ) {
      final int len = RANDOM.nextInt(array.length - off) + 1;
      arrayCrc.update(array, off, len);

      buffer.position(off).limit(off + len);
      bufferCrc.update(buffer);

      Assert.assertEquals(arrayCrc.getValue(), bufferCrc.getValue());
      off += len;
    }
  }
}
