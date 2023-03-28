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

import org.apache.ratis.thirdparty.com.google.common.collect.Interner;
import org.apache.ratis.thirdparty.com.google.common.collect.Interners;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.function.StringSupplier;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public final class StringUtils {
  private StringUtils() {
  }

  public static final String[] EMPTY_STRING_ARRAY = {};

  /** Retains a weak reference to each string instance it has interned. */
  private static final Interner<String> WEAK_INTERNER = Interners.newWeakInterner();

  static final DateTimeFormatter DATE_TIME = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss_SSS");

  public static String currentDateTime() {
    return LocalDateTime.now().format(DATE_TIME);
  }

  /**
   * Interns and returns a reference to the representative instance
   * for any of a collection of string instances that are equal to each other.
   * Retains weak reference to the instance,
   * and so does not prevent it from being garbage-collected.
   *
   * @param sample string instance to be interned
   * @return weak reference to interned string instance
   */
  public static String weakIntern(String sample) {
    return sample == null? null: WEAK_INTERNER.intern(sample);
  }

  /**
   * Splits the given comma separated {@link String}.
   * Each split value is trimmed.
   *
   * @param s a comma separated {@link String}, or possibly null.
   * @return the split strings, or an empty array if the given string is null.
   */
  public static String[] getTrimmedStrings(String s) {
    return s == null? EMPTY_STRING_ARRAY
        : (s = s.trim()).isEmpty()? EMPTY_STRING_ARRAY
        : s.split("\\s*,\\s*");
  }

  /** The same as String.format(Locale.ENGLISH, format, objects). */
  public static String format(final String format, final Object... objects) {
    return String.format(Locale.ENGLISH, format, objects);
  }

  public static String bytes2HexShortString(ByteString bytes) {
    final int size = bytes.size();
    if (size == 0) {
      return "<EMPTY>";
    } else if (size > 10) {
      // return only the first 10 bytes
      return bytes2HexString(bytes.substring(0, 10)) + "...(size=" + size + ")";
    } else {
      return bytes2HexString(bytes);
    }
  }

  public static String bytes2HexString(ByteString bytes) {
    Objects.requireNonNull(bytes, "bytes == null");
    return bytes2HexString(bytes.asReadOnlyByteBuffer());
  }

  public static String bytes2HexString(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes == null");
    return bytes2HexString(ByteBuffer.wrap(bytes));
  }

  public static String bytes2HexString(byte[] bytes, int offset, int length) {
    Objects.requireNonNull(bytes, "bytes == null");
    return bytes2HexString(ByteBuffer.wrap(bytes, offset, length));
  }

  public static String bytes2HexString(ByteBuffer bytes) {
    Objects.requireNonNull(bytes, "bytes == null");

    final StringBuilder s = new StringBuilder(2 * bytes.remaining());
    for(; bytes.remaining() > 0; ) {
      s.append(format("%02x", bytes.get()));
    }
    bytes.flip();
    return s.toString();
  }

  public static boolean string2boolean(String s, boolean defaultValue) {
    if (s == null || s.isEmpty()) {
      return defaultValue;
    }

    if ("true".equalsIgnoreCase(s)) {
      return true;
    } else if ("false".equalsIgnoreCase(s)) {
      return false;
    } else {
      return defaultValue;
    }
  }

  public static String stringifyException(Throwable e) {
    StringWriter stm = new StringWriter();
    PrintWriter wrt = new PrintWriter(stm);
    e.printStackTrace(wrt);
    wrt.close();
    return stm.toString();
  }

  public static StringSupplier stringSupplierAsObject(Supplier<String> supplier) {
    return StringSupplier.get(supplier);
  }

  public static <K, V> String map2String(Map<K, V> map) {
    if (map == null) {
      return null;
    } else if (map.isEmpty()) {
      return "<EMPTY_MAP>";
    } else {
      final StringBuilder b = new StringBuilder("{");
      map.entrySet().stream().forEach(e -> b.append("\n  ").append(e.getKey()).append(" -> ").append(e.getValue()));
      return b.append("\n}").toString();
    }
  }

  public static String completableFuture2String(CompletableFuture<?> future, boolean includeDetails) {
    if (!future.isDone()) {
      return "NOT_DONE";
    } else if (future.isCancelled()) {
      return "CANCELLED";
    } else if (future.isCompletedExceptionally()) {
      if (!includeDetails) {
        return "EXCEPTION";
      }
      return future.thenApply(Objects::toString).exceptionally(Throwable::toString).join();
    } else {
      if (!includeDetails) {
        return "COMPLETED";
      }
      return "" + future.join();
    }
  }
}
