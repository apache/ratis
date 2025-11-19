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
package org.apache.ratis.io;

import org.apache.ratis.util.StringUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestMD5 {

  @Test
  void testMD5Hash() {
    final byte[] digest = new byte[MD5Hash.MD5_LENGTH];
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    
    for(int i = 0; i < 1000; i++) {
      random.nextBytes(digest);
      final MD5Hash md5 = MD5Hash.newInstance(digest);

      // test hashCode
      final int expectedHashCode = oldQuarterDigest(digest);
      assertEquals(expectedHashCode, md5.hashCode());

      // test toString
      final String expectedString = StringUtils.bytes2HexString(digest);
      assertEquals(expectedString, md5.toString());
      assertEquals(expectedString, MD5Hash.digestToString(digest));

      // test newInstance(String)
      assertEquals(md5, MD5Hash.newInstance(expectedString.toLowerCase()));
      assertEquals(md5, MD5Hash.newInstance(expectedString.toUpperCase()));

      // test getDigest
      final ByteBuffer expectedByteBuffer = ByteBuffer.wrap(digest);
      assertEquals(expectedByteBuffer, md5.getDigest());
    }
  }

  /**
   * Return a 32-bit digest of the MD5.
   * @return the first 4 bytes of the md5
   */
  private static int oldQuarterDigest(byte[] digest) {
    int value = 0;
    for (int i = 0; i < 4; i++) {
      value |= ((digest[i] & 0xff) << (8*(3-i)));
    }
    return value;
  }
}