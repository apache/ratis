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

import org.apache.ratis.proto.RaftProtos.RoutingTableProto;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A routing table is a directed acyclic graph containing exactly one primary peer such that
 * (1) the primary peer is the only starting peer, and
 * (2) all the other peers can be reached from the primary peer by exactly one path.
 */
public interface RoutingTable {
  /** @return the successor peers of the given peer. */
  Set<RaftPeerId> getSuccessors(RaftPeerId peerId);

  /** @return the proto of this {@link RoutingTable}. */
  RoutingTableProto toProto();

  /** @return a new builder to build a {@link RoutingTable}. */
  static Builder newBuilder() {
    return new Builder();
  }

  /** To build a {@link RoutingTable}. */
  final class Builder {
    private final AtomicReference<Map<RaftPeerId, Set<RaftPeerId>>> ref = new AtomicReference<>(new HashMap<>());

    private Builder() {}

    private Set<RaftPeerId> computeIfAbsent(RaftPeerId peerId) {
      return Optional.ofNullable(ref.get())
          .map(map -> map.computeIfAbsent(peerId, key -> new HashSet<>()))
          .orElseThrow(() -> new IllegalStateException("Already built"));
    }

    public Builder addSuccessor(RaftPeerId peerId, RaftPeerId successor) {
      computeIfAbsent(peerId).add(successor);
      return this;
    }

    public Builder addSuccessors(RaftPeerId peerId, Collection<RaftPeerId> successors) {
      computeIfAbsent(peerId).addAll(successors);
      return this;
    }

    public Builder addSuccessors(RaftPeerId peerId, RaftPeerId... successors) {
      return addSuccessors(peerId, Arrays.asList(successors));
    }

    public RoutingTable build() {
      return Optional.ofNullable(ref.getAndSet(null))
          .map(RoutingTable::newRoutingTable)
          .orElseThrow(() -> new IllegalStateException("RoutingTable Already built"));
    }

    static void validate(Map<RaftPeerId, Set<RaftPeerId>> map) {
      if (map != null && !map.isEmpty()) {
        new Builder.Validation(map).run();
      }
    }

    /** Validate if a map represents a valid routing table. */
    private static final class Validation {
      private final Map<RaftPeerId, Set<RaftPeerId>> map;
      private final RaftPeerId primary;
      private final Set<RaftPeerId> unreachablePeers;

      private Validation(Map<RaftPeerId, Set<RaftPeerId>> map) {
        this.map = Objects.requireNonNull(map, "map == null");

        final Set<RaftPeerId> allPeers = new HashSet<>(map.keySet());
        final Set<RaftPeerId> startingPeers = new HashSet<>(map.keySet());
        int numEdges = 0;
        for (Map.Entry<RaftPeerId, Set<RaftPeerId>> entry: map.entrySet()) {
          final Set<RaftPeerId> successors = entry.getValue();
          if (successors == null) {
            continue;
          }
          for (RaftPeerId s : successors) {
            Preconditions.assertTrue(!s.equals(entry.getKey()), () -> "Invalid routing table: the peer " + s
                + " has a self-loop, " + this);

            if (!startingPeers.remove(s)) { //the primary peer cannot be a successor
              final boolean added = allPeers.add(s); //an ending peer may not be contained as a key in the map
              Preconditions.assertTrue(added, () -> "Invalid routing table: the peer " + s
                  + " has more than one predecessors, " + this);
            }
          }
          numEdges += successors.size();
        }

        Preconditions.assertTrue(numEdges == allPeers.size() - 1,
            "Invalid routing table: #edges = %d != #vertices - 1, #vertices=%d, %s",
            numEdges, allPeers.size(), this);
        Preconditions.assertTrue(!startingPeers.isEmpty(),
            () -> "Invalid routing table: Starting peer not found, " + this);
        Preconditions.assertTrue(startingPeers.size() == 1,
            () -> "Invalid routing table: More than one starting peers: " + startingPeers + ", " + this);

        this.primary = startingPeers.iterator().next();
        this.unreachablePeers = allPeers;
      }

      private void run() {
        depthFirstSearch(primary);
        Preconditions.assertTrue(unreachablePeers.isEmpty() ,
            () -> "Invalid routing table: peer(s) " + unreachablePeers +  " are unreachable, " + this);
      }

      private void depthFirstSearch(RaftPeerId current) {
        final boolean removed = unreachablePeers.remove(current);
        Preconditions.assertTrue(removed, () -> "Invalid routing table: the peer " + current
            + " has more than one predecessors, " + this);
        for (RaftPeerId successor : get(current)) {
          depthFirstSearch(successor);
        }
      }

      private Set<RaftPeerId> get(RaftPeerId peerId) {
        return Optional.ofNullable(map.get(peerId)).orElseGet(Collections::emptySet);
      }

      @Override
      public String toString() {
        return "primary=" + primary + ", map=" + map;
      }
    }
  }

  /** @return a new {@link RoutingTable} represented by the given map. */
  static RoutingTable newRoutingTable(Map<RaftPeerId, Set<RaftPeerId>> map){
    Builder.validate(map);

    final Supplier<RoutingTableProto> proto = JavaUtils.memoize(
        () -> RoutingTableProto.newBuilder().addAllRoutes(ProtoUtils.toRouteProtos(map)).build());
    return new RoutingTable() {
      @Override
      public Set<RaftPeerId> getSuccessors(RaftPeerId peerId) {
        return Optional.ofNullable(map.get(peerId)).orElseGet(Collections::emptySet);
      }

      @Override
      public RoutingTableProto toProto() {
        return proto.get();
      }
    };
  }
}