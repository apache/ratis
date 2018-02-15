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

import java.util.*;

/**
 * Description of a raft group. It has a globally unique ID and a group of raft
 * peers.
 */
public class RaftGroup {
  private static RaftGroup EMPTY_GROUP = new RaftGroup(RaftGroupId.emptyGroupId());

  public static RaftGroup emptyGroup() {
    return EMPTY_GROUP;
  }

  /** UTF-8 string as id */
  private final RaftGroupId groupId;
  /** The group of raft peers */
  private final Map<RaftPeerId, RaftPeer> peers;

  public RaftGroup(RaftGroupId groupId) {
    this(groupId, Collections.emptyList());
  }

  public RaftGroup(RaftGroupId groupId, RaftPeer... peers) {
    this(groupId, Arrays.asList(peers));
  }

  public RaftGroup(RaftGroupId groupId, Collection<RaftPeer> peers) {
    this.groupId = Objects.requireNonNull(groupId, "groupId == null");

    if (peers == null || peers.isEmpty()) {
      this.peers = Collections.emptyMap();
    } else {
      final Map<RaftPeerId, RaftPeer> map = new HashMap<>();
      peers.stream().forEach(p -> map.put(p.getId(), p));
      this.peers = Collections.unmodifiableMap(map);
    }
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  public Collection<RaftPeer> getPeers() {
    return peers.values();
  }

  /** @return the peer with the given id if it is in this group; otherwise, return null. */
  public RaftPeer getPeer(RaftPeerId id) {
    return peers.get(Objects.requireNonNull(id, "id == null"));
  }

  @Override
  public String toString() {
    return groupId + ":" + peers.values();
  }
}
