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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.PeerProxyMap;

import java.io.Closeable;
import java.util.Collection;

/** An abstract {@link RaftClientRpc} implementation using {@link PeerProxyMap}. */
public abstract class RaftClientRpcWithProxy<PROXY extends Closeable>
    implements RaftClientRpc {
  private final PeerProxyMap<PROXY> proxies;

  protected RaftClientRpcWithProxy(PeerProxyMap<PROXY> proxies) {
    this.proxies = proxies;
  }

  public PeerProxyMap<PROXY> getProxies() {
    return proxies;
  }

  @Override
  public void addRaftPeers(Collection<RaftPeer> servers) {
    proxies.addRaftPeers(servers);
  }

  @Override
  public boolean handleException(RaftPeerId serverId, Throwable t, boolean reconnect) {
    return getProxies().handleException(serverId, t, reconnect);
  }

  @Override
  public void close() {
    proxies.close();
  }
}
