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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.ProtoUtils;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/** Caching the commit information. */
class CommitInfoCache {
  static Long max(Long oldCommitIndex, long newCommitIndex) {
    return oldCommitIndex == null || newCommitIndex > oldCommitIndex ? newCommitIndex : oldCommitIndex;
  }

  static BiFunction<RaftPeerId, Long, Long> remapping(long newCommitIndex) {
    return (id, oldCommitIndex) -> max(oldCommitIndex, newCommitIndex);
  }

  private final ConcurrentMap<RaftPeerId, Long> map = new ConcurrentHashMap<>();

  CommitInfoProto get(RaftPeer peer) {
    return ProtoUtils.toCommitInfoProto(peer, get(peer.getId()));
  }

  Long get(RaftPeerId id) {
    return map.get(id);
  }

  CommitInfoProto update(RaftPeer peer, long newCommitIndex) {
    Objects.requireNonNull(peer, "peer == null");
    final long computed = map.compute(peer.getId(), remapping(newCommitIndex));
    return ProtoUtils.toCommitInfoProto(peer, computed);
  }

  void update(CommitInfoProto newInfo) {
    final long newCommitIndex = newInfo.getCommitIndex();
    map.compute(RaftPeerId.valueOf(newInfo.getServer().getId()), remapping(newCommitIndex));
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":" + map;
  }
}
