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
package org.apache.ratis.protocol;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

/** Unique identifier implemented using {@link UUID}. */
public abstract class RaftId {
  private static final int BYTE_LENGTH = 16;

  private static void checkLength(int length, String name) {
    Preconditions.assertTrue(length == BYTE_LENGTH,
        "%s = %s != BYTE_LENGTH = %s", name, length, BYTE_LENGTH);
  }

  private static UUID toUuid(ByteString bytes) {
    Objects.requireNonNull(bytes, "bytes == null");
    checkLength(bytes.size(), "bytes.size()");
    final ByteBuffer buf = bytes.asReadOnlyByteBuffer();
    return new UUID(buf.getLong(), buf.getLong());
  }

  private static ByteString toByteString(UUID uuid) {
    Objects.requireNonNull(uuid, "uuid == null");
    final ByteBuffer buf = ByteBuffer.wrap(new byte[BYTE_LENGTH]);
    buf.putLong(uuid.getMostSignificantBits());
    buf.putLong(uuid.getLeastSignificantBits());
    return ByteString.copyFrom(buf.array());
  }

  private final UUID uuid;
  private final Supplier<ByteString> uuidBytes;
  private final Supplier<String> uuidString;

  private RaftId(UUID uuid, Supplier<ByteString> uuidBytes) {
    this.uuid = uuid;
    this.uuidBytes = uuidBytes;
    this.uuidString = JavaUtils.memoize(() -> createUuidString(uuid));
  }

  RaftId(UUID uuid) {
    this(uuid, JavaUtils.memoize(() -> toByteString(uuid)));
  }

  public RaftId(ByteString uuidBytes) {
    this(toUuid(uuidBytes), () -> uuidBytes);
  }

  /** @return the last 12 hex digits. */
  String createUuidString(UUID uuid) {
    final String s = uuid.toString().toUpperCase();
    final int i = s.lastIndexOf('-');
    return s.substring(i + 1);
  }

  public ByteString toByteString() {
    return uuidBytes.get();
  }

  @Override
  public String toString() {
    return uuidString.get();
  }

  @Override
  public boolean equals(Object other) {
    return other == this ||
        (other instanceof RaftId
            && this.getClass() == other.getClass()
            && uuid.equals(((RaftId) other).uuid));
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  public UUID getUuid() {
    return uuid;
  }
}
