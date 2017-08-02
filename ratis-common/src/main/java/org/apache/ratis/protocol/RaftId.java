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

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

public abstract class RaftId {
  public static final int BYTE_LENGTH = 16;

  static UUID toUuid(byte[] data) {
    Objects.requireNonNull(data, "data == null");
    Preconditions.assertTrue(data.length == BYTE_LENGTH,
        "data.length = %s != BYTE_LENGTH = %s", data.length, BYTE_LENGTH);
    ByteBuffer buffer = ByteBuffer.wrap(data);
    return new UUID(buffer.getLong(), buffer.getLong());
  }

  static byte[] toBytes(UUID uuid) {
    Objects.requireNonNull(uuid, "uuid == null");
    ByteBuffer buf = ByteBuffer.wrap(new byte[BYTE_LENGTH]);
    buf.putLong(uuid.getMostSignificantBits());
    buf.putLong(uuid.getLeastSignificantBits());
    return buf.array();
  }

  private final UUID uuid;
  private final byte[] uuidBytes;
  private final Supplier<String> uuidString;

  private RaftId(UUID uuid, byte[] bytes) {
    this.uuid = uuid;
    this.uuidBytes = bytes;
    this.uuidString = JavaUtils.memoize(this::createUuidString);
  }

  RaftId(UUID uuid) {
    this(uuid, toBytes(uuid));
  }

  public RaftId(byte[] uuidBytes) {
    this(toUuid(uuidBytes), uuidBytes);
  }

  String createUuidString() {
    return uuid.toString().toUpperCase();
  }

  public byte[] toBytes() {
    return uuidBytes;
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
}
