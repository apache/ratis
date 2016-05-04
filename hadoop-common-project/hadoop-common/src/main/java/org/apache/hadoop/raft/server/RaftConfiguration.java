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
package org.apache.hadoop.raft.server;

import org.apache.hadoop.raft.server.protocol.RaftPeer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RaftConfiguration {
  private final Map<String, RaftPeer> peers = new HashMap<>();

  public RaftConfiguration(RaftPeer[] peers) {
    for(RaftPeer p : peers) {
      this.peers.put(p.getId(), p);
    }
  }

  public Collection<RaftPeer> getPeers() {
    return peers.values();
  }

  public int getSize() {
    return peers.size();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + peers.values();
  }

  public RaftPeer getPeer(String id) {
    return peers.get(id);
  }
}
