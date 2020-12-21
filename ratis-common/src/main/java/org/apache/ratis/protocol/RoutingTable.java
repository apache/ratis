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

import org.apache.ratis.proto.RaftProtos.RoutingTableProto;
import org.apache.ratis.util.ProtoUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public interface RoutingTable {
  Set<RaftPeerId> getSuccessors(RaftPeerId peerId);

  RoutingTableProto toProto();

  static Builder newBuilder() {
    return new Builder();
  }


  class Builder {
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
  }

  static RoutingTable newRoutingTable(Map<RaftPeerId, Set<RaftPeerId>> map){
    return new RoutingTable() {
      @Override
      public Set<RaftPeerId> getSuccessors(RaftPeerId peerId) {
        return Optional.ofNullable(map.get(peerId)).orElseGet(Collections::emptySet);
      }

      @Override
      public RoutingTableProto toProto() {
        return RoutingTableProto.newBuilder().addAllRoutes(ProtoUtils.toRouteProtos(map)).build();
      }
    };
  }
}