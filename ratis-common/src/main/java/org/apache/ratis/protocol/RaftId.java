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

import org.apache.ratis.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

public abstract class RaftId {
  public static final int BYTE_LENGTH = 16;

  private final UUID uuid;

  protected RaftId(UUID id) {
    this.uuid = Objects.requireNonNull(id, "id == null");
  }

  public RaftId(byte[] data) {
    Objects.requireNonNull(data, "data == null");
    Preconditions.assertTrue(data.length == BYTE_LENGTH,
        "data.length = %s != BYTE_LENGTH = %s", data.length, BYTE_LENGTH);
    ByteBuffer buffer = ByteBuffer.wrap(data);
    this.uuid = new UUID(buffer.getLong(), buffer.getLong());
  }

  public byte[] toBytes() {
    ByteBuffer buf = ByteBuffer.wrap(new byte[BYTE_LENGTH]);
    buf.putLong(uuid.getMostSignificantBits());
    buf.putLong(uuid.getLeastSignificantBits());
    return buf.array();
  }

  @Override
  public String toString() {
    return uuid.toString();
  }


  @Override
  public boolean equals(Object other) {
    return other == this ||
        (other instanceof RaftId &&
            uuid.equals(((RaftId) other).uuid));
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }
}
