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
package org.apache.ratis.server.impl;

import java.util.*;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.Preconditions;

/**
 * The peer configuration of a raft cluster.
 *
 * The objects of this class are immutable.
 */
class PeerConfiguration {
  private final Map<RaftPeerId, RaftPeer> peers;

  PeerConfiguration(Iterable<RaftPeer> peers) {
    Objects.requireNonNull(peers);
    Map<RaftPeerId, RaftPeer> map = new HashMap<>();
    for(RaftPeer p : peers) {
      map.put(p.getId(), p);
    }
    this.peers = Collections.unmodifiableMap(map);
    Preconditions.assertTrue(!this.peers.isEmpty());
  }

  Collection<RaftPeer> getPeers() {
    return Collections.unmodifiableCollection(peers.values());
  }

  int size() {
    return peers.size();
  }

  @Override
  public String toString() {
    return peers.values().toString();
  }

  RaftPeer getPeer(RaftPeerId id) {
    return peers.get(id);
  }

  boolean contains(RaftPeerId id) {
    return peers.containsKey(id);
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
      if (num > size() / 2) {
        return true;
      }
    }
    return false;
  }
}
