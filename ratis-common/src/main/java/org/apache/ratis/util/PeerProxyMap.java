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
package org.apache.ratis.util;

import org.apache.ratis.protocol.AlreadyClosedException;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

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

    RaftPeer getPeer() {
      return peer;
    }

    PROXY getProxy() throws IOException {
      if (proxy == null) {
        synchronized (this) {
          if (proxy == null) {
            final LifeCycle.State current = lifeCycle.getCurrentState();
            if (current.isClosingOrClosed()) {
              throw new AlreadyClosedException(name + " is already " + current);
            }
            lifeCycle.startAndTransition(
                () -> proxy = createProxy.apply(peer), IOException.class);
          }
        }
      }
      return proxy;
    }

    @Override
    public synchronized void close() {
      lifeCycle.checkStateAndClose(() -> {
        if (proxy != null) {
          try {
            proxy.close();
          } catch (IOException e) {
            LOG.warn("{}: Failed to close proxy for peer {}, proxy class: ",
                name, peer, proxy.getClass());
          }
        }
      });
    }
  }

  private final String name;
  private final Map<RaftPeerId, PeerAndProxy> peers = new ConcurrentHashMap<>();
  private final Object resetLock = new Object();

  private final CheckedFunction<RaftPeer, PROXY, IOException> createProxy;

  public PeerProxyMap(String name, CheckedFunction<RaftPeer, PROXY, IOException> createProxy) {
    this.name = name;
    this.createProxy = createProxy;
  }

  public PeerProxyMap(String name) {
    this.name = name;
    this.createProxy = this::createProxyImpl;
  }

  public PROXY getProxy(RaftPeerId id) throws IOException {
    PeerAndProxy p = peers.get(id);
    if (p == null) {
      synchronized (resetLock) {
        p = Objects.requireNonNull(peers.get(id),
            () -> name + ": Server " + id + " not found: peers=" + peers.keySet());
      }
    }
    return p.getProxy();
  }

  public void addPeers(Iterable<RaftPeer> newPeers) {
    for(RaftPeer p : newPeers) {
      computeIfAbsent(p);
    }
  }

  public void computeIfAbsent(RaftPeer p) {
    peers.computeIfAbsent(p.getId(), k -> new PeerAndProxy(p));
  }

  public void resetProxy(RaftPeerId id) {
    LOG.debug("{}: reset proxy for {}", name, id );
    synchronized (resetLock) {
      final PeerAndProxy pp = peers.remove(id);
      if (pp != null) {
        final RaftPeer peer = pp.getPeer();
        pp.close();
        computeIfAbsent(peer);
      }
    }
  }

  /** @return true if the given throwable is handled; otherwise, the call is an no-op, return false. */
  public boolean handleException(RaftPeerId serverId, Throwable e, boolean reconnect) {
    if (reconnect || IOUtils.shouldReconnect(e)) {
      resetProxy(serverId);
      return true;
    }
    return false;
  }

  public PROXY createProxyImpl(RaftPeer peer) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    peers.values().forEach(PeerAndProxy::close);
  }
}