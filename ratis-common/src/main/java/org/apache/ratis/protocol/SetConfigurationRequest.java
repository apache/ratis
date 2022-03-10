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

import org.apache.ratis.util.Preconditions;

import java.util.Collections;
import java.util.List;

public class SetConfigurationRequest extends RaftClientRequest {
  private final List<RaftPeer> peers;
  private final List<RaftPeer> listeners;

  public SetConfigurationRequest(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId, List<RaftPeer> peers) {
    this(clientId, serverId, groupId, callId, peers, Collections.emptyList());
  }

  public SetConfigurationRequest(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId, List<RaftPeer> peers, List<RaftPeer> listeners) {
    super(clientId, serverId, groupId, callId, true, writeRequestType());
    this.peers = peers != null? Collections.unmodifiableList(peers): Collections.emptyList();
    this.listeners = listeners !=  null? Collections.unmodifiableList(listeners) : Collections.emptyList();
    Preconditions.assertUnique(this.peers);
    Preconditions.assertUnique(this.listeners);
  }

  public List<RaftPeer> getPeersInNewConf() {
    return peers;
  }

  public List<RaftPeer> getListenersInNewConf() {
    return listeners;
  }

  @Override
  public String toString() {
    return super.toString() + ", peers:" + getPeersInNewConf();
  }
}
