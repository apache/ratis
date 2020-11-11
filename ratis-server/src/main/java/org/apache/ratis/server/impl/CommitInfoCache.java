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

/** Caching the commit information. */
class CommitInfoCache {
  private final ConcurrentMap<RaftPeerId, CommitInfoProto> map = new ConcurrentHashMap<>();

  CommitInfoProto get(RaftPeerId id) {
    return map.get(id);
  }

  CommitInfoProto update(RaftPeer peer, long newCommitIndex) {
    Objects.requireNonNull(peer, "peer == null");
    return map.compute(peer.getId(), (id, old) ->
        old == null || newCommitIndex > old.getCommitIndex()? ProtoUtils.toCommitInfoProto(peer, newCommitIndex): old);
  }

  CommitInfoProto update(CommitInfoProto newInfo) {
    return map.compute(RaftPeerId.valueOf(newInfo.getServer().getId()),
        (id, old) -> old == null || newInfo.getCommitIndex() > old.getCommitIndex()? newInfo: old);
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":" + map.values();
  }
}
