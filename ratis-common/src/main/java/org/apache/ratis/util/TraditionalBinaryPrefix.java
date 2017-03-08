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

/**
 * The traditional binary prefixes, kilo, mega, ..., exa,
 * which can be represented by a 64-bit integer.
 * {@link TraditionalBinaryPrefix} symbols are case insensitive.
 */
public enum TraditionalBinaryPrefix {
  KILO(10),
  MEGA(KILO.bitShift + 10),
  GIGA(MEGA.bitShift + 10),
  TERA(GIGA.bitShift + 10),
  PETA(TERA.bitShift + 10),
  EXA (PETA.bitShift + 10);

  private final long value;
  private final char symbol;
  private final int bitShift;
  private final long bitMask;

  TraditionalBinaryPrefix(int bitShift) {
    this.bitShift = bitShift;
    this.value = 1L << bitShift;
    this.bitMask = this.value - 1L;
    this.symbol = toString().charAt(0);
  }

  public long getValue() {
    return value;
  }

  public char getSymbol() {
    return symbol;
  }

  public long toLong(long n) {
    final long shifted = n << bitShift;
    if (n != shifted >>> bitShift) {
      throw new ArithmeticException("Long overflow: " + toString(n)
          + " cannot be assigned to a long.");
    }
    return shifted;
  }

  public String toString(long n) {
    return n + String.valueOf(symbol);
  }

  /**
   * @return The object corresponding to the symbol.
   */
  public static TraditionalBinaryPrefix valueOf(char symbol) {
    symbol = Character.toUpperCase(symbol);
    for(TraditionalBinaryPrefix prefix : TraditionalBinaryPrefix.values()) {
      if (symbol == prefix.symbol) {
        return prefix;
      }
    }
    throw new IllegalArgumentException("Unknown symbol '" + symbol + "'");
  }

  /**
   * Convert a string to long.
   * The input string is first be trimmed
   * and then it is parsed with traditional binary prefix.
   *
   * For example,
   * "-1230k" will be converted to -1230 * 1024 = -1259520;
   * "891g" will be converted to 891 * 1024^3 = 956703965184;
   *
   * @param s input string
   * @return a long value represented by the input string.
   */
  public static long string2long(String s) {
    s = s.trim();
    final int lastpos = s.length() - 1;
    final char lastchar = s.charAt(lastpos);
    if (Character.isDigit(lastchar))
      return Long.parseLong(s);
    else {
      long p;
      try {
        p = TraditionalBinaryPrefix.valueOf(lastchar).value;
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid size prefix '" + lastchar
            + "' in '" + s + "'. Allowed prefixes are k, m, g, t, p, e (case insensitive)");
      }
      long num = Long.parseLong(s.substring(0, lastpos).trim());
      if (num > Long.MAX_VALUE/p || num < Long.MIN_VALUE/p) {
        throw new IllegalArgumentException(s + " does not fit in a Long");
      }
      return num * p;
    }
  }

  /**
   * Convert a long integer to a string with traditional binary prefix.
   *
   * @param n the value to be converted
   * @param unit The unit, e.g. "B" for bytes.
   * @param decimalPlaces The number of decimal places.
   * @return a string with traditional binary prefix.
   */
  public static String long2String(long n, String unit, int decimalPlaces) {
    if (unit == null) {
      unit = "";
    }
    //take care a special case
    if (n == Long.MIN_VALUE) {
      return "-8 " + EXA.symbol + unit;
    }

    final StringBuilder b = new StringBuilder();
    //take care negative numbers
    if (n < 0) {
      b.append('-');
      n = -n;
    }
    if (n < KILO.value) {
      //no prefix
      b.append(n);
      return (unit.isEmpty()? b: b.append(" ").append(unit)).toString();
    } else {
      //find traditional binary prefix
      int i = 0;
      for(; i < values().length && n >= values()[i].value; i++);
      TraditionalBinaryPrefix prefix = values()[i - 1];

      if ((n & prefix.bitMask) == 0) {
        //exact division
        b.append(n >> prefix.bitShift);
      } else {
        final String  format = "%." + decimalPlaces + "f";
        String s = StringUtils.format(format, n/(double)prefix.value);
        //check a special rounding up case
        if (s.startsWith("1024")) {
          prefix = values()[i];
          s = StringUtils.format(format, n/(double)prefix.value);
        }
        b.append(s);
      }
      return b.append(' ').append(prefix.symbol).append(unit).toString();
    }
  }
}
