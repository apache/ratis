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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class HadoopRpcBase<PROXY> {
  private final Map<String, PROXY> peers;
  private final Configuration conf;

  public HadoopRpcBase(Configuration conf) {
    this.conf = conf;
    peers = Collections.synchronizedMap(new HashMap<String, PROXY>());
  }

  public Collection<String> getServerIds() {
    return peers.keySet();
  }

  public PROXY getServerProxy(String id) {
    return peers.get(id);
  }

  public Configuration getConf() {
    return this.conf;
  }

  public void addPeers(Iterable<RaftPeer> newPeers)
      throws IOException {
    for(RaftPeer p : newPeers) {
      peers.put(p.getId(), createProxy(p));
    }
  }

  public abstract PROXY createProxy(RaftPeer peer) throws IOException;
}
