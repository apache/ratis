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

package org.apache.ratis.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class MD5Hash {
  public static final int MD5_LEN = 16;

  private static final ThreadLocal<MessageDigest> DIGESTER_FACTORY =
      ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  });

  private byte[] digest;

  /** Constructs an MD5Hash. */
  public MD5Hash() {
    this.digest = new byte[MD5_LEN];
  }

  /** Constructs an MD5Hash from a hex string. */
  public MD5Hash(String hex) {
    setDigest(hex);
  }

  /** Constructs an MD5Hash with a specified value. */
  public MD5Hash(byte[] digest) {
    if (digest.length != MD5_LEN)
      throw new IllegalArgumentException("Wrong length: " + digest.length);
    this.digest = digest;
  }

  public void readFields(DataInput in) throws IOException {
    in.readFully(digest);
  }

  /** Constructs, reads and returns an instance. */
  public static MD5Hash read(DataInput in) throws IOException {
    MD5Hash result = new MD5Hash();
    result.readFields(in);
    return result;
  }

  public void write(DataOutput out) throws IOException {
    out.write(digest);
  }

  /** Copy the contents of another instance into this instance. */
  public void set(MD5Hash that) {
    System.arraycopy(that.digest, 0, this.digest, 0, MD5_LEN);
  }

  /** Returns the digest bytes. */
  public byte[] getDigest() { return digest; }

  /** Construct a hash value for a byte array. */
  public static MD5Hash digest(byte[] data) {
    return digest(data, 0, data.length);
  }

  /**
   * Create a thread local MD5 digester
   */
  public static MessageDigest getDigester() {
    MessageDigest digester = DIGESTER_FACTORY.get();
    digester.reset();
    return digester;
  }

  /** Construct a hash value for the content from the InputStream. */
  public static MD5Hash digest(InputStream in) throws IOException {
    final byte[] buffer = new byte[4*1024];

    final MessageDigest digester = getDigester();
    for(int n; (n = in.read(buffer)) != -1; ) {
      digester.update(buffer, 0, n);
    }

    return new MD5Hash(digester.digest());
  }

  /** Construct a hash value for a byte array. */
  public static MD5Hash digest(byte[] data, int start, int len) {
    byte[] digest;
    MessageDigest digester = getDigester();
    digester.update(data, start, len);
    digest = digester.digest();
    return new MD5Hash(digest);
  }

  /** Construct a hash value for an array of byte array. */
  public static MD5Hash digest(byte[][] dataArr, int start, int len) {
    byte[] digest;
    MessageDigest digester = getDigester();
    for (byte[] data : dataArr) {
      digester.update(data, start, len);
    }
    digest = digester.digest();
    return new MD5Hash(digest);
  }

  /** Construct a half-sized version of this MD5.  Fits in a long **/
  public long halfDigest() {
    long value = 0;
    for (int i = 0; i < 8; i++)
      value |= ((digest[i] & 0xffL) << (8*(7-i)));
    return value;
  }

  /**
   * Return a 32-bit digest of the MD5.
   * @return the first 4 bytes of the md5
   */
  public int quarterDigest() {
    int value = 0;
    for (int i = 0; i < 4; i++)
      value |= ((digest[i] & 0xff) << (8*(3-i)));
    return value;
  }

  /** Returns true iff <code>o</code> is an MD5Hash whose digest contains the
   * same values.  */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MD5Hash))
      return false;
    MD5Hash other = (MD5Hash)o;
    return Arrays.equals(this.digest, other.digest);
  }

  /** Returns a hash code value for this object.
   * Only uses the first 4 bytes, since md5s are evenly distributed.
   */
  @Override
  public int hashCode() {
    return quarterDigest();
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

  /** Sets the digest value from a hex string. */
  public void setDigest(String hex) {
    if (hex.length() != MD5_LEN*2)
      throw new IllegalArgumentException("Wrong length: " + hex.length());
    byte[] digest = new byte[MD5_LEN];
    for (int i = 0; i < MD5_LEN; i++) {
      int j = i << 1;
      digest[i] = (byte)(charToNibble(hex.charAt(j)) << 4 |
          charToNibble(hex.charAt(j+1)));
    }
    this.digest = digest;
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
