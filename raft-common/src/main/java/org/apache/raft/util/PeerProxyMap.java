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
package org.apache.raft.util;

import com.google.common.base.Preconditions;
import org.apache.raft.protocol.RaftPeer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A map from peer id to peer and its proxy. */
public abstract class PeerProxyMap<PROXY> {
  /** Peer and its proxy. */
  private class PeerAndProxy {
    private final RaftPeer peer;
    private volatile PROXY proxy = null;

    PeerAndProxy(RaftPeer peer) {
      this.peer = peer;
    }

    PROXY getProxy() throws IOException {
      if (proxy == null) {
        synchronized (this) {
          if (proxy == null) {
            proxy = createProxy(peer);
          }
        }
      }
      return proxy;
    }
  }

  private final Map<String, PeerAndProxy> peers
      = Collections.synchronizedMap(new HashMap<>());

  public Collection<String> getPeerIds() {
    return peers.keySet();
  }

  public PROXY getProxy(String id) throws IOException {
    // create new proxy based on new RaftConfiguration. So here the proxy
    // should be available.
    final PeerAndProxy p = Preconditions.checkNotNull(peers.get(id),
        "Server %s not found; peers=%s", id, getPeerIds());
    return p.getProxy();
  }

  public void addPeers(Iterable<RaftPeer> newPeers) {
    for(RaftPeer p : newPeers) {
      peers.put(p.getId(), new PeerAndProxy(p));
    }
  }

  public abstract PROXY createProxy(RaftPeer peer) throws IOException;
}
