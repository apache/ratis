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

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The peer configuration of a raft cluster.
 * <p>
 * The objects of this class are immutable.
 */
class PeerConfiguration {
  /**
   * Peers are voting members such as LEADER, CANDIDATE and FOLLOWER
   * @see org.apache.ratis.proto.RaftProtos.RaftPeerRole
   */
  private final Map<RaftPeerId, RaftPeer> peers;
  /**
   * Listeners are non-voting members.
   * @see org.apache.ratis.proto.RaftProtos.RaftPeerRole#LISTENER
   */
  private final Map<RaftPeerId, RaftPeer> listeners;

  static Map<RaftPeerId, RaftPeer> newMap(Iterable<RaftPeer> peers, String name, Map<RaftPeerId, RaftPeer> existing) {
    Objects.requireNonNull(peers, () -> name + " == null");
    final Map<RaftPeerId, RaftPeer> map = new HashMap<>();
    for(RaftPeer p : peers) {
      if (existing.containsKey(p.getId())) {
        throw new IllegalArgumentException("Failed to initialize " + name
            + ": Found " + p.getId() + " in existing peers " + existing);
      }
      final RaftPeer previous = map.putIfAbsent(p.getId(), p);
      if (previous != null) {
        throw new IllegalArgumentException("Failed to initialize " + name
            + ": Found duplicated ids " + p.getId() + " in " + peers);
      }
    }
    return Collections.unmodifiableMap(map);
  }

  PeerConfiguration(Iterable<RaftPeer> peers) {
    this(peers, Collections.emptyList());
  }

  PeerConfiguration(Iterable<RaftPeer> peers, Iterable<RaftPeer> listeners) {
    this.peers = newMap(peers, "peers", Collections.emptyMap());
    this.listeners = Optional.ofNullable(listeners)
        .map(l -> newMap(listeners, "listeners", this.peers))
        .orElseGet(Collections::emptyMap);
  }

  private Map<RaftPeerId, RaftPeer> getPeerMap(RaftPeerRole r) {
    if (r == RaftPeerRole.FOLLOWER) {
      return peers;
    } else if (r == RaftPeerRole.LISTENER) {
      return listeners;
    } else {
      throw new IllegalArgumentException("Unexpected RaftPeerRole " + r);
    }
  }

  Collection<RaftPeer> getPeers(RaftPeerRole role) {
    return Collections.unmodifiableCollection(getPeerMap(role).values());
  }

  int size() {
    return peers.size();
  }

  Stream<RaftPeerId> streamPeerIds() {
    return peers.keySet().stream();
  }

  @Override
  public String toString() {
    return "peers:" + peers.values() + "|listeners:" + listeners.values();
  }

  RaftPeer getPeer(RaftPeerId id, RaftPeerRole... roles) {
    if (roles == null || roles.length == 0) {
      return peers.get(id);
    }
    for(RaftPeerRole r : roles) {
      final RaftPeer peer = getPeerMap(r).get(id);
      if (peer != null) {
        return peer;
      }
    }
    return null;
  }

  boolean contains(RaftPeerId id) {
    return contains(id, RaftPeerRole.FOLLOWER);
  }

  boolean contains(RaftPeerId id, RaftPeerRole r) {
    return getPeerMap(r).containsKey(id);
  }

  RaftPeerRole contains(RaftPeerId id, EnumSet<RaftPeerRole> roles) {
    if (roles == null || roles.isEmpty()) {
      return peers.containsKey(id)? RaftPeerRole.FOLLOWER: null;
    }
    for(RaftPeerRole r : roles) {
      if (getPeerMap(r).containsKey(id)) {
        return r;
      }
    }
    return null;
  }

  List<RaftPeer> getOtherPeers(RaftPeerId selfId) {
    List<RaftPeer> others = new ArrayList<>();
    for (Map.Entry<RaftPeerId, RaftPeer> entry : peers.entrySet()) {
      if (!selfId.equals(entry.getValue().getId())) {
        others.add(entry.getValue());
      }
    }
    return others;
  }

  boolean hasMajority(Collection<RaftPeerId> others, RaftPeerId selfId) {
    Preconditions.assertTrue(!others.contains(selfId));
    int num = 0;
    if (contains(selfId)) {
      num++;
    }
    for (RaftPeerId other : others) {
      if (contains(other)) {
        num++;
      }
    }
    return num > size() / 2;
  }

  int getMajorityCount() {
    return size() / 2 + 1;
  }

  boolean majorityRejectVotes(Collection<RaftPeerId> rejected) {
    int num = size();
    for (RaftPeerId other : rejected) {
      if (contains(other)) {
        num --;
      }
    }
    return num <= size() / 2;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    final PeerConfiguration that = (PeerConfiguration)obj;
    return this.peers.equals(that.peers);
  }

  @Override
  public int hashCode() {
    return peers.keySet().hashCode(); // hashCode of a set is well defined in Java.
  }
}
