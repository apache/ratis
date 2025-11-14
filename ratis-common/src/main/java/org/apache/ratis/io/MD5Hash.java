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

import java.util.Arrays;

/**
 * A MD5 hash value.
 * <p>
 * This is a value-based class.
 */
public final class MD5Hash {
  public static final int MD5_LEN = 16;

  private final byte[] digest;

  private MD5Hash(byte[] digest) {
    this.digest = digest;
  }

  /** Constructs an MD5Hash with a specified value. */
  public static MD5Hash newInstance(byte[] digest) {
    if (digest.length != MD5_LEN) {
      throw new IllegalArgumentException("Wrong length: " + digest.length);
    }
    return new MD5Hash(digest.clone());
  }

  /** Returns the digest bytes. */
  public byte[] getDigest() {
    return digest.clone();
  }

  /** Returns true iff <code>o</code> is an MD5Hash whose digest contains the
   * same values.  */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MD5Hash)) {
      return false;
    }
    MD5Hash other = (MD5Hash)o;
    return Arrays.equals(this.digest, other.digest);
  }

  /** Returns a hash code value for this object.
   * Only uses the first 4 bytes, since md5s are evenly distributed.
   */
  @Override
  public int hashCode() {
    return ((digest[0] & 0xFF) << 24)
        |  ((digest[1] & 0xFF) << 16)
        |  ((digest[2] & 0xFF) << 8)
        |   (digest[3] & 0xFF);
  }

  private static final char[] HEX_DIGITS =
      {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

  /** Returns a string representation of this object. */
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder(MD5_LEN*2);
    for (int i = 0; i < MD5_LEN; i++) {
      int b = digest[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }
    return buf.toString();
  }

  public static MD5Hash newInstance(String hex) {
    if (hex.length() != MD5_LEN*2) {
      throw new IllegalArgumentException("Wrong length: " + hex.length());
    }

    final byte[] digest = new byte[MD5_LEN];
    for (int i = 0; i < MD5_LEN; i++) {
      int j = i << 1;
      digest[i] = (byte)(charToNibble(hex.charAt(j)) << 4 |
          charToNibble(hex.charAt(j+1)));
    }
    return new MD5Hash(digest);
  }

  private static int charToNibble(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    } else if (c >= 'a' && c <= 'f') {
      return 0xa + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
      return 0xA + (c - 'A');
    } else {
      throw new RuntimeException("Not a hex character: " + c);
    }
  }
}
