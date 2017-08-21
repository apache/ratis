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

import org.junit.Test;

import static org.apache.ratis.util.TraditionalBinaryPrefix.long2String;
import static org.apache.ratis.util.TraditionalBinaryPrefix.string2long;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestTraditionalBinaryPrefix {
  @Test(timeout = 1000)
  public void testTraditionalBinaryPrefix() throws Exception {
    //test string2long(..)
    String[] symbol = {"k", "m", "g", "t", "p", "e"};
    long m = 1024;
    for(String s : symbol) {
      assertEquals(0, string2long(0 + s));
      assertEquals(m, string2long(1 + s));
      m *= 1024;
    }

    assertEquals(0L, string2long("0"));
    assertEquals(1024L, string2long("1k"));
    assertEquals(-1024L, string2long("-1k"));
    assertEquals(1259520L, string2long("1230K"));
    assertEquals(-1259520L, string2long("-1230K"));
    assertEquals(104857600L, string2long("100m"));
    assertEquals(-104857600L, string2long("-100M"));
    assertEquals(956703965184L, string2long("891g"));
    assertEquals(-956703965184L, string2long("-891G"));
    assertEquals(501377302265856L, string2long("456t"));
    assertEquals(-501377302265856L, string2long("-456T"));
    assertEquals(11258999068426240L, string2long("10p"));
    assertEquals(-11258999068426240L, string2long("-10P"));
    assertEquals(1152921504606846976L, string2long("1e"));
    assertEquals(-1152921504606846976L, string2long("-1E"));

    String tooLargeNumStr = "10e";
    try {
      string2long(tooLargeNumStr);
      fail("Test passed for a number " + tooLargeNumStr + " too large");
    } catch (IllegalArgumentException e) {
      assertEquals(tooLargeNumStr + " does not fit in a Long", e.getMessage());
    }

    String tooSmallNumStr = "-10e";
    try {
      string2long(tooSmallNumStr);
      fail("Test passed for a number " + tooSmallNumStr + " too small");
    } catch (IllegalArgumentException e) {
      assertEquals(tooSmallNumStr + " does not fit in a Long", e.getMessage());
    }

    String invalidFormatNumStr = "10kb";
    char invalidPrefix = 'b';
    try {
      string2long(invalidFormatNumStr);
      fail("Test passed for a number " + invalidFormatNumStr
          + " has invalid format");
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid size prefix '" + invalidPrefix + "' in '"
              + invalidFormatNumStr
              + "'. Allowed prefixes are k, m, g, t, p, e (case insensitive)",
          e.getMessage());
    }

    //test long2string(..)
    assertEquals("0", long2String(0, null, 2));
    for(int decimalPlace = 0; decimalPlace < 2; decimalPlace++) {
      for(int n = 1; n < TraditionalBinaryPrefix.KILO.getValue(); n++) {
        assertEquals(n + "", long2String(n, null, decimalPlace));
        assertEquals(-n + "", long2String(-n, null, decimalPlace));
      }
      assertEquals("1 K", long2String(1L << 10, null, decimalPlace));
      assertEquals("-1 K", long2String(-1L << 10, null, decimalPlace));
    }

    assertEquals("8.00 E", long2String(Long.MAX_VALUE, null, 2));
    assertEquals("8.00 E", long2String(Long.MAX_VALUE - 1, null, 2));
    assertEquals("-8 E", long2String(Long.MIN_VALUE, null, 2));
    assertEquals("-8.00 E", long2String(Long.MIN_VALUE + 1, null, 2));

    final String[] zeros = {" ", ".0 ", ".00 "};
    for(int decimalPlace = 0; decimalPlace < zeros.length; decimalPlace++) {
      final String trailingZeros = zeros[decimalPlace];

      for(int e = 11; e < Long.SIZE - 1; e++) {
        final TraditionalBinaryPrefix p
            = TraditionalBinaryPrefix.values()[e/10 - 1];

        { // n = 2^e
          final long n = 1L << e;
          final String expected = (n/p.getValue()) + " " + p.getSymbol();
          assertEquals("n=" + n, expected, long2String(n, null, 2));
        }

        { // n = 2^e + 1
          final long n = (1L << e) + 1;
          final String expected = (n/p.getValue()) + trailingZeros + p.getSymbol();
          assertEquals("n=" + n, expected, long2String(n, null, decimalPlace));
        }

        { // n = 2^e - 1
          final long n = (1L << e) - 1;
          final String expected = ((n+1)/p.getValue()) + trailingZeros + p.getSymbol();
          assertEquals("n=" + n, expected, long2String(n, null, decimalPlace));
        }
      }
    }

    assertEquals("1.50 K", long2String(3L << 9, null, 2));
    assertEquals("1.5 K", long2String(3L << 9, null, 1));
    assertEquals("1.50 M", long2String(3L << 19, null, 2));
    assertEquals("2 M", long2String(3L << 19, null, 0));
    assertEquals("3 G", long2String(3L << 30, null, 2));

    assertEquals("0 B", byteDescription(0));
    assertEquals("-100 B", byteDescription(-100));
    assertEquals("1 KB", byteDescription(1024));
    assertEquals("1.50 KB", byteDescription(3L << 9));
    assertEquals("1.50 MB", byteDescription(3L << 19));
    assertEquals("3 GB", byteDescription(3L << 30));
  }

  private static String byteDescription(long len) {
    return long2String(len, "B", 2);
  }
}
