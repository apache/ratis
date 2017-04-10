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
import java.util.Objects;

/**
 * Id of Raft Peer. Should be globally unique.
 */
public class RaftPeerId {
  public static RaftPeerId getRaftPeerId(String id) {
    return id == null || id.isEmpty() ? null : new RaftPeerId(id);
  }

  /** UTF-8 string as id */
  private final byte[] id;

  public RaftPeerId(String id) {
    Objects.requireNonNull(id, "id == null");
    Preconditions.assertTrue(!id.isEmpty(), "id is an empty string.");
    this.id = id.getBytes(StandardCharsets.UTF_8);
  }

  public RaftPeerId(byte[] id) {
    this.id = id;
  }

  public RaftPeerId(ByteString id) {
    this(id.toByteArray());
  }

  /**
   * @return id in byte[].
   */
  public byte[] toBytes() {
    return id;
  }

  @Override
  public String toString() {
    return new String(id, StandardCharsets.UTF_8);
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
