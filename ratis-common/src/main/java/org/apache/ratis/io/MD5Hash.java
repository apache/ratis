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

import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A MD5 hash value.
 * <p>
 * This is a value-based class.
 */
public final class MD5Hash {
  public static final int MD5_LENGTH = 16;

  /** @return an instance with the given digest in a (case-insensitive) hexadecimals. */
  public static MD5Hash newInstance(String digestHexadecimals) {
    Objects.requireNonNull(digestHexadecimals, "digestHexadecimals == null");
    Preconditions.assertSame(2 * MD5_LENGTH, digestHexadecimals.length(), "digestHexadecimals");

    final byte[] digest = new byte[MD5_LENGTH];
    for (int i = 0; i < MD5_LENGTH; i++) {
      final int j = i << 1;
      digest[i] = (byte) (charToNibble(digestHexadecimals, j) << 4 |
          charToNibble(digestHexadecimals, j + 1));
    }
    return new MD5Hash(digest);
  }

  /** @return an instance with the given digest. */
  public static MD5Hash newInstance(byte[] digest) {
    Objects.requireNonNull(digest, "digest == null");
    Preconditions.assertSame(MD5_LENGTH, digest.length, "digest");
    return new MD5Hash(digest.clone());
  }

  private final byte[] digest;
  private final Supplier<String> digestString;

  private MD5Hash(byte[] digest) {
    this.digest = digest;
    this.digestString = MemoizedSupplier.valueOf(() -> digestToString(digest));
  }

  /** @return the digest wrapped by a read-only {@link ByteBuffer}. */
  public ByteBuffer getDigest() {
    return ByteBuffer.wrap(digest).asReadOnlyBuffer();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    } else if (!(object instanceof MD5Hash)) {
      return false;
    }
    final MD5Hash that = (MD5Hash) object;
    return Arrays.equals(this.digest, that.digest);
  }

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
    return digestString.get();
  }

  static String digestToString(byte[] digest) {
    StringBuilder buf = new StringBuilder(MD5_LENGTH *2);
    for (int i = 0; i < MD5_LENGTH; i++) {
      int b = digest[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }
    return buf.toString();
  }

  private static int charToNibble(String hexadecimals, int i) {
    final char c = hexadecimals.charAt(i);
    if (c >= '0' && c <= '9') {
      return c - '0';
    } else if (c >= 'a' && c <= 'f') {
      return 0xa + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
      return 0xA + (c - 'A');
    } else {
      throw new IllegalArgumentException(
          "Found a non-hexadecimal character '" + c + "' at index " + i + " in \"" + hexadecimals + "\"");
    }
  }
}
