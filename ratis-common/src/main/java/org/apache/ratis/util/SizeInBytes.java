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

import java.util.Objects;

/**
 * Size which may be constructed with a {@link TraditionalBinaryPrefix}.
 */
public class SizeInBytes {
  public static SizeInBytes valueOf(long size) {
    final String s = String.valueOf(size);
    return new SizeInBytes(size, s, s);
  }

  public static SizeInBytes valueOf(long n, TraditionalBinaryPrefix prefix) {
    final long size = Objects.requireNonNull(prefix, "prefix = null").toLong(n);
    final String input = n + " " + prefix.getSymbol();
    final String description = input + " (=" + size + ")";
    return new SizeInBytes(size, input, description);
  }

  public static SizeInBytes valueOf(String input) {
    input = Objects.requireNonNull(input, "input = null").trim();

    final int last = input.length() - 1;
    final String s = "b".equalsIgnoreCase(input.substring(last))?
        input.substring(0, last): input;
    final long size;
    try {
      size = TraditionalBinaryPrefix.string2long(s);
    } catch(NumberFormatException e) {
      throw new IllegalArgumentException("Failed to parse input " + input, e);
    }
    final String description = input.equals(String.valueOf(size))?
        input: input + " (=" + size + ")";

    return new SizeInBytes(size, input, description);
  }

  private final long size;
  private final String input;
  private final String description;

  private SizeInBytes(long size, String input, String description) {
    this.size = size;
    this.input = input;
    this.description = description;
  }

  public long getSize() {
    return size;
  }

  public int getSizeInt() {
    return Math.toIntExact(getSize());
  }

  public String getInput() {
    return input;
  }

  @Override
  public String toString() {
    return description;
  }
}
