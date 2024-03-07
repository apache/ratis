/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.netty.server;

import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelId;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Map: {@link ChannelId} -> {@link ClientInvocationId}s. */
class ChannelMap {
  private final Map<ChannelId, Map<ClientInvocationId, ClientInvocationId>> map = new ConcurrentHashMap<>();

  void add(ChannelId channelId, ClientInvocationId clientInvocationId) {
    map.computeIfAbsent(channelId, (e) -> new ConcurrentHashMap<>())
        .put(clientInvocationId, clientInvocationId);
  }

  void remove(ChannelId channelId, ClientInvocationId clientInvocationId) {
    Optional.ofNullable(map.get(channelId))
        .ifPresent((ids) -> ids.remove(clientInvocationId));
  }

  Set<ClientInvocationId> remove(ChannelId channelId) {
    return Optional.ofNullable(map.remove(channelId))
        .map(Map::keySet)
        .orElse(Collections.emptySet());
  }
}
