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

import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.function.CheckedFunction;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** A map from peer id to peer and its proxy. */
public class PeerProxyMap<PROXY extends Closeable> implements RaftPeer.Add, Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(PeerProxyMap.class);

  /** Peer and its proxy. */
  private class PeerAndProxy {
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
                () -> proxy = createProxyImpl(peer), IOException.class);
          }
        }
      }
      return proxy;
    }

    Optional<PROXY> setNullProxyAndClose() {
      final PROXY p;
      synchronized (this) {
        p = proxy;
        lifeCycle.checkStateAndClose(() -> proxy = null);
      }
      return Optional.ofNullable(p);
    }

    @Override
    public String toString() {
      return peer.getId().toString();
    }
  }

  private final String name;
  private final Map<RaftPeerId, PeerAndProxy> peers = new ConcurrentHashMap<>();
  private final Object resetLock = new Object();

  private final CheckedFunction<RaftPeer, PROXY, IOException> createProxy;
  private final AtomicBoolean closed = new AtomicBoolean();

  public PeerProxyMap(String name, CheckedFunction<RaftPeer, PROXY, IOException> createProxy) {
    this.name = name;
    this.createProxy = createProxy;
  }

  public String getName() {
    return name;
  }

  private PROXY createProxyImpl(RaftPeer peer) throws IOException {
    if (closed.get()) {
      throw new AlreadyClosedException(name + ": Failed to create proxy for " + peer);
    }
    return createProxy.apply(peer);
  }

  public PROXY getProxy(RaftPeerId id) throws IOException {
    Objects.requireNonNull(id, "id == null");
    PeerAndProxy p = peers.get(id);
    if (p == null) {
      synchronized (resetLock) {
        p = Objects.requireNonNull(peers.get(id),
            () -> name + ": Server " + id + " not found: peers=" + peers.keySet());
      }
    }
    return p.getProxy();
  }

  @Override
  public void addRaftPeers(Collection<RaftPeer> newPeers) {
    for(RaftPeer p : newPeers) {
      computeIfAbsent(p);
    }
  }

  /**
   * This method is similar to {@link Map#computeIfAbsent(Object, java.util.function.Function)}
   * except that this method does not require a mapping function.
   *
   * @param peer the peer for retrieving/building a proxy.
   * @return a supplier of the proxy for the given peer.
   */
  public CheckedSupplier<PROXY, IOException> computeIfAbsent(RaftPeer peer) {
    final PeerAndProxy peerAndProxy = peers.computeIfAbsent(peer.getId(), k -> new PeerAndProxy(peer));
    return peerAndProxy::getProxy;
  }

  public void resetProxy(RaftPeerId id) {
    LOG.debug("{}: reset proxy for {}", name, id );
    final PeerAndProxy pp;
    Optional<PROXY> optional = Optional.empty();
    synchronized (resetLock) {
      pp = peers.remove(id);
      if (pp != null) {
        final RaftPeer peer = pp.getPeer();
        optional = pp.setNullProxyAndClose();
        computeIfAbsent(peer);
      }
    }
    // close proxy without holding the reset lock
    optional.map(proxy -> closeProxy(proxy, pp))
        .ifPresent(e -> LOG.warn("Failed to close the previous proxy", e));
  }

  /** @return true if the given throwable is handled; otherwise, the call is a no-op, return false. */
  public boolean handleException(RaftPeerId serverId, Throwable e, boolean reconnect) {
    if (reconnect || IOUtils.shouldReconnect(e)) {
      resetProxy(serverId);
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    final List<IOException> exceptions = Collections.synchronizedList(new ArrayList<>());
    ConcurrentUtils.parallelForEachAsync(peers.values(),
        pp -> pp.setNullProxyAndClose().map(proxy -> closeProxy(proxy, pp)).ifPresent(exceptions::add),
        r -> new Thread(r).start()
    ).join();

    final int size = exceptions.size();
    if (size > 0) {
      final Iterator<IOException> i = exceptions.iterator();
      final IOException e = new IOException(name + ": Failed to close proxy map", i.next());
      for(; i.hasNext(); ) {
        e.addSuppressed(i.next());
      }
      LOG.warn("{}: {} exception(s)", name, size, e);
    }
  }

  private IOException closeProxy(PROXY proxy, PeerAndProxy pp) {
    try {
      LOG.debug("{}: Closing proxy {} {} for peer {}", name, proxy.getClass().getSimpleName(), proxy, pp);
      proxy.close();
      return null;
    } catch (IOException e) {
      return new IOException(name + ": Failed to close proxy for peer " + pp + ", " + proxy.getClass(), e);
    }
  }
}