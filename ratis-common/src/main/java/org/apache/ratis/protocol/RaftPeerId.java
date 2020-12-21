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
package org.apache.ratis.protocol;

import org.apache.ratis.proto.RaftProtos.RaftPeerIdProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Id of Raft Peer which is globally unique.
 *
 * This is a value-based class.
 */
public final class RaftPeerId {
  private static final Map<ByteString, RaftPeerId> BYTE_STRING_MAP = new ConcurrentHashMap<>();
  private static final Map<String, RaftPeerId> STRING_MAP = new ConcurrentHashMap<>();

  public static RaftPeerId valueOf(ByteString id) {
    return BYTE_STRING_MAP.computeIfAbsent(id, RaftPeerId::new);
  }

  public static RaftPeerId valueOf(String id) {
    return STRING_MAP.computeIfAbsent(id, RaftPeerId::new);
  }

  public static RaftPeerId getRaftPeerId(String id) {
    return id == null || id.isEmpty() ? null : RaftPeerId.valueOf(id);
  }

  /** UTF-8 string as id */
  private final String idString;
  /** The corresponding bytes of {@link #idString}. */
  private final ByteString id;

  private final Supplier<RaftPeerIdProto> raftPeerIdProto;

  private RaftPeerId(String id) {
    this.idString = Objects.requireNonNull(id, "id == null");
    this.id = ByteString.copyFrom(idString, StandardCharsets.UTF_8);
    this.raftPeerIdProto = JavaUtils.memoize(this::buildRaftPeerIdProto);
  }

  private RaftPeerId(ByteString id) {
    this.id = Objects.requireNonNull(id, "id == null");
    Preconditions.assertTrue(id.size() > 0, "id is empty.");
    this.idString = id.toString(StandardCharsets.UTF_8);
    this.raftPeerIdProto = JavaUtils.memoize(this::buildRaftPeerIdProto);
  }

  private RaftPeerIdProto buildRaftPeerIdProto() {
    final RaftPeerIdProto.Builder builder = RaftPeerIdProto.newBuilder().setId(id);
    return builder.build();
  }

  public RaftPeerIdProto getRaftPeerIdProto() {
    return raftPeerIdProto.get();
  }

  /**
   * @return id in {@link ByteString}.
   */
  public ByteString toByteString() {
    return id;
  }

  @Override
  public String toString() {
    return idString;
  }

  @Override
  public boolean equals(Object other) {
    return other == this ||
        (other instanceof RaftPeerId && idString.equals(((RaftPeerId)other).idString));
  }

  @Override
  public int hashCode() {
    return idString.hashCode();
  }
}
