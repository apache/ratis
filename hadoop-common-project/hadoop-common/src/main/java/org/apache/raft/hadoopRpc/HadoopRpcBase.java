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
package org.apache.raft.hadoopRpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.raft.protocol.RaftPeer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class HadoopRpcBase<PROXY> {
  private final Map<String, PROXY> peers = new HashMap<>();

  public Collection<String> getServerIds() {
    return peers.keySet();
  }

  public PROXY getServerProxy(String id) {
    return peers.get(id);
  }

  public void addPeers(Iterable<RaftPeer> newPeers, Configuration conf)
      throws IOException {
    for(RaftPeer p : newPeers) {
      peers.put(p.getId(), createProxy(p, conf));
    }
  }

  public abstract PROXY createProxy(RaftPeer peer, Configuration conf)
      throws IOException;
}
