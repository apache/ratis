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
package org.apache.ratis.server;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.PeerProxyMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

/** An abstract implementation of {@link RaftServerRpc} using a {@link PeerProxyMap}. */
public abstract class RaftServerRpcWithProxy<PROXY extends Closeable, PROXIES extends PeerProxyMap<PROXY>>
    implements RaftServerRpc {
  private final Supplier<RaftPeerId> idSupplier;
  private final Supplier<LifeCycle> lifeCycleSupplier;
  private final Supplier<PROXIES> proxiesSupplier;

  protected RaftServerRpcWithProxy(Supplier<RaftPeerId> idSupplier, Function<RaftPeerId, PROXIES> proxyCreater) {
    this.idSupplier = idSupplier;
    this.lifeCycleSupplier = JavaUtils.memoize(
        () -> new LifeCycle(getId() + "-" + JavaUtils.getClassSimpleName(getClass())));
    this.proxiesSupplier = JavaUtils.memoize(() -> proxyCreater.apply(getId()));
  }

  /** @return the server id. */
  public RaftPeerId getId() {
    return idSupplier.get();
  }

  private LifeCycle getLifeCycle() {
    return lifeCycleSupplier.get();
  }

  /** @return the underlying {@link PeerProxyMap}. */
  public PROXIES getProxies() {
    return proxiesSupplier.get();
  }

  @Override
  public void addRaftPeers(Collection<RaftPeer> peers) {
    getProxies().addRaftPeers(peers);
  }

  @Override
  public void handleException(RaftPeerId serverId, Exception e, boolean reconnect) {
    getProxies().handleException(serverId, e, reconnect);
  }

  @Override
  public final void start() throws IOException {
    getLifeCycle().startAndTransition(this::startImpl, IOException.class);
  }

  /** Implementation of the {@link #start()} method. */
  protected abstract void startImpl() throws IOException;

  @Override
  public final void close() throws IOException{
    getLifeCycle().checkStateAndClose(this::closeImpl);
  }

  /** Implementation of the {@link #close()} method. */
  public void closeImpl() throws IOException {
    getProxies().close();
  }
}
