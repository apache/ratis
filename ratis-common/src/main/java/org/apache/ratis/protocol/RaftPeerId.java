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

import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Id of Raft Peer which is globally unique.
 */
public class RaftPeerId {
  private static final Map<ByteString, RaftPeerId> byteStringMap = new ConcurrentHashMap<>();
  private static final Map<String, RaftPeerId> stringMap = new ConcurrentHashMap<>();

  public static RaftPeerId valueOf(ByteString id) {
    return byteStringMap.computeIfAbsent(id,
        key -> new RaftPeerId(key.toByteArray()));
  }

  public static RaftPeerId valueOf(String id) {
    return stringMap.computeIfAbsent(id, RaftPeerId::new);
  }

  public static RaftPeerId getRaftPeerId(String id) {
    return id == null || id.isEmpty() ? null : RaftPeerId.valueOf(id);
  }

  /** UTF-8 string as id */
  private final String idString;
  /** The corresponding bytes of {@link #idString}. */
  private final byte[] id;

  private RaftPeerId(String id) {
    this.idString = Objects.requireNonNull(id, "id == null");
    Preconditions.assertTrue(!id.isEmpty(), "id is an empty string.");
    this.id = id.getBytes(StandardCharsets.UTF_8);
  }

  private RaftPeerId(byte[] id) {
    this.id = Objects.requireNonNull(id, "id == null");
    Preconditions.assertTrue(id.length > 0, "id is an empty array.");
    this.idString = new String(id, StandardCharsets.UTF_8);
  }

  /**
   * @return id in byte[].
   */
  public byte[] toBytes() {
    return id;
  }

  @Override
  public String toString() {
    return idString;
  }

  @Override
  public boolean equals(Object other) {
    return other == this ||
        (other instanceof RaftPeerId &&
            Arrays.equals(id, ((RaftPeerId) other).id));
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(id);
  }
}
