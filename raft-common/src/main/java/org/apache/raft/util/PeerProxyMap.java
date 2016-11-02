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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A map from peer id to peer and its proxy. */
public class PeerProxyMap<PROXY extends Closeable> implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(PeerProxyMap.class);

  /** Peer and its proxy. */
  private class PeerAndProxy implements Closeable {
    private final RaftPeer peer;
    private volatile PROXY proxy = null;
    private final LifeCycle lifeCycle;

    PeerAndProxy(RaftPeer peer) {
      this.peer = peer;
      this.lifeCycle = new LifeCycle(peer);
    }

    PROXY getProxy() throws IOException {
      if (proxy == null) {
        synchronized (this) {
          if (proxy == null) {
            lifeCycle.transition(LifeCycle.State.STARTING);
            proxy = createProxy.apply(peer);
            lifeCycle.transition(LifeCycle.State.RUNNING);
          }
        }
      }
      return proxy;
    }

    @Override
    public synchronized void close() {
      if (lifeCycle.getCurrentState() == LifeCycle.State.NEW) {
        lifeCycle.transition(LifeCycle.State.CLOSED);
      } else {
        lifeCycle.transition(LifeCycle.State.CLOSING);
        if (proxy != null) {
          try {
            proxy.close();
          } catch (IOException e) {
            LOG.warn("Failed to close proxy for peer {}, proxy class: ",
                peer, proxy.getClass());
          }
        }
        lifeCycle.transition(LifeCycle.State.CLOSED);
      }
    }
  }

  private final Map<String, PeerAndProxy> peers
      = Collections.synchronizedMap(new HashMap<>());

  private final CheckedFunction<RaftPeer, PROXY, IOException> createProxy;

  public PeerProxyMap(CheckedFunction<RaftPeer, PROXY, IOException> createProxy) {
    this.createProxy = createProxy;
  }
  public PeerProxyMap() {
    this.createProxy = this::createProxyImpl;
  }

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

  public PROXY createProxyImpl(RaftPeer peer) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    peers.values().forEach(PeerAndProxy::close);
  }
}