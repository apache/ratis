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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Caching the commit information. */
class CommitInfoCache {
  private final ConcurrentMap<RaftPeerId, Long> map = new ConcurrentHashMap<>();

  Optional<Long> get(RaftPeerId id) {
    return Optional.ofNullable(map.get(id));
  }

  CommitInfoProto update(RaftPeer peer, long newCommitIndex) {
    Objects.requireNonNull(peer, "peer == null");
    final long updated = update(peer.getId(), newCommitIndex);
    return ProtoUtils.toCommitInfoProto(peer, updated);
  }

  long update(RaftPeerId peerId, long newCommitIndex) {
    Objects.requireNonNull(peerId, "peerId == null");
    return map.compute(peerId, (id, oldCommitIndex) -> {
      if (oldCommitIndex != null) {
        // get around BX_UNBOXING_IMMEDIATELY_REBOXED
        final long old = oldCommitIndex;
        if (old >= newCommitIndex) {
          return old;
        }
      }
      return newCommitIndex;
    });
  }

  void update(CommitInfoProto newInfo) {
    final RaftPeerId id = RaftPeerId.valueOf(newInfo.getServer().getId());
    update(id, newInfo.getCommitIndex());
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":" + map;
  }
}
