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
package org.apache.ratis.server.impl;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.CheckedFunction;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class RaftServerProxy implements RaftServer {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerProxy.class);

  /**
   * A map: {@link RaftGroupId} -> {@link RaftServerImpl} futures.
   *
   * The map is synchronized for mutations and the bulk {@link #getAll()} method
   * but the (non-bulk) {@link #get(RaftGroupId)} and {@link #containsGroup(RaftGroupId)} methods are not.
   * The thread safety and atomicity guarantees for the non-bulk methods are provided by {@link ConcurrentMap}.
   */
  class ImplMap implements Closeable {
    private final ConcurrentMap<RaftGroupId, CompletableFuture<RaftServerImpl>> map = new ConcurrentHashMap<>();
    private boolean isClosed = false;

    synchronized CompletableFuture<RaftServerImpl> addNew(RaftGroup group) {
      if (isClosed) {
        return JavaUtils.completeExceptionally(new AlreadyClosedException(
            getId() + ": Failed to add " + group + " since the server is already closed"));
      }
      if (containsGroup(group.getGroupId())) {
        return JavaUtils.completeExceptionally(new AlreadyExistsException(
            getId() + ": Failed to add " + group + " since the group already exists in the map."));
      }
      final RaftGroupId groupId = group.getGroupId();
      final CompletableFuture<RaftServerImpl> newImpl = newRaftServerImpl(group);
      final CompletableFuture<RaftServerImpl> previous = map.put(groupId, newImpl);
      Preconditions.assertNull(previous, "previous");
      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: addNew {} returns {}", getId(), group, toString(groupId, newImpl));
      }
      return newImpl;
    }

    synchronized CompletableFuture<RaftServerImpl> remove(RaftGroupId groupId) {
      final CompletableFuture<RaftServerImpl> future = map.remove(groupId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: remove {}", getId(), toString(groupId, future));
      }
      return future;
    }

    @Override
    public synchronized void close() {
      if (isClosed) {
        LOG.info("{} is already closed.", getId());
        return;
      }
      isClosed = true;
      map.values().parallelStream().map(CompletableFuture::join).forEach(RaftServerImpl::shutdown);
    }

    synchronized List<CompletableFuture<RaftServerImpl>> getAll() {
      return new ArrayList<>(map.values());
    }

    CompletableFuture<RaftServerImpl> get(RaftGroupId groupId) {
      final CompletableFuture<RaftServerImpl> i = map.get(groupId);
      if (i == null) {
        return JavaUtils.completeExceptionally(new GroupMismatchException(
            getId() + ": " + groupId + " not found."));
      }
      return i;
    }

    boolean containsGroup(RaftGroupId groupId) {
      return map.containsKey(groupId);
    }

    @Override
    public synchronized String toString() {
      if (map.isEmpty()) {
        return "<EMPTY>";
      } else if (map.size() == 1) {
        return toString(map.entrySet().iterator().next());
      }
      final StringBuilder b = new StringBuilder("[");
      map.entrySet().forEach(e -> b.append("\n  ").append(toString(e)));
      return b.append("] size=").append(map.size()).toString();
    }

    String toString(Map.Entry<RaftGroupId, CompletableFuture<RaftServerImpl>> e) {
      return toString(e.getKey(), e.getValue());
    }

    String toString(RaftGroupId groupId, CompletableFuture<RaftServerImpl> f) {
      return "" + (f != null && f.isDone()? f.join(): groupId + ":" + f);
    }
  }

  private final RaftPeerId id;
  private final RaftProperties properties;
  private final StateMachine.Registry stateMachineRegistry;
  private final LifeCycle lifeCycle;

  private final RaftServerRpc serverRpc;
  private final ServerFactory factory;

  private final ImplMap impls = new ImplMap();
  private final AtomicReference<ReinitializeRequest> reinitializeRequest = new AtomicReference<>();

  RaftServerProxy(RaftPeerId id, StateMachine.Registry stateMachineRegistry,
      RaftProperties properties, Parameters parameters) {
    this.properties = properties;
    this.stateMachineRegistry = stateMachineRegistry;

    final RpcType rpcType = RaftConfigKeys.Rpc.type(properties);
    this.factory = ServerFactory.cast(rpcType.newFactory(parameters));

    this.serverRpc = factory.newRaftServerRpc(this);
    this.id = id != null? id: RaftPeerId.valueOf(getIdStringFrom(serverRpc));
    this.lifeCycle = new LifeCycle(this.id);
  }

  private CompletableFuture<RaftServerImpl> newRaftServerImpl(RaftGroup group) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        serverRpc.addPeers(group.getPeers());
        return new RaftServerImpl(group, stateMachineRegistry.apply(group.getGroupId()), this);
      } catch(IOException e) {
        throw new CompletionException(getId() + ": Failed to initialize server for " + group, e);
      }
    });
  }

  private static String getIdStringFrom(RaftServerRpc rpc) {
    InetSocketAddress address = null;
    try {
      address = rpc.getInetSocketAddress();
    } catch(Exception e) {
      LOG.warn("Failed to get InetSocketAddress from " + rpc.getRpcType() + " rpc server", e);
    }
    return address != null? address.getHostName() + "_" + address.getPort()
        : rpc.getRpcType() + "-" + UUID.randomUUID();
  }

  @Override
  public RaftPeerId getId() {
    return id;
  }

  @Override
  public Iterable<RaftGroupId> getGroupIds() throws IOException {
    return getImpls().stream().map(RaftServerImpl::getGroupId).collect(Collectors.toList());
  }

  @Override
  public RpcType getRpcType() {
    return getFactory().getRpcType();
  }

  @Override
  public ServerFactory getFactory() {
    return factory;
  }

  @Override
  public RaftProperties getProperties() {
    return properties;
  }

  public RaftServerRpc getServerRpc() {
    return serverRpc;
  }

  public boolean containsGroup(RaftGroupId groupId) {
    return impls.containsGroup(groupId);
  }

  CompletableFuture<RaftServerImpl> addGroup(RaftGroup group) {
    return impls.addNew(group);
  }

  private CompletableFuture<RaftServerImpl> getImplFuture(RaftGroupId groupId) {
    return impls.get(groupId);
  }

  private RaftServerImpl getImpl(RaftRpcRequestProto proto) throws IOException {
    return getImpl(ProtoUtils.toRaftGroupId(proto.getRaftGroupId()));
  }

  RaftServerImpl getImpl(RaftGroupId groupId) throws IOException {
    Objects.requireNonNull(groupId, "groupId == null");
    return IOUtils.getFromFuture(getImplFuture(groupId), getId());
  }

  List<RaftServerImpl> getImpls() throws IOException {
    final List<RaftServerImpl> list = new ArrayList<>();
    for(CompletableFuture<RaftServerImpl> f : impls.getAll()) {
      list.add(IOUtils.getFromFuture(f, getId()));
    }
    return list;
  }

  @Override
  public LifeCycle.State getLifeCycleState() {
    return lifeCycle.getCurrentState();
  }

  @Override
  public void start() throws IOException {
    getImpls().parallelStream().forEach(RaftServerImpl::start);

    lifeCycle.startAndTransition(() -> {
      LOG.info("{}: start RPC server", getId());
      getServerRpc().start();
    }, IOException.class);
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> {
      LOG.info("{}: close", getId());
      impls.close();

      try {
        getServerRpc().close();
      } catch(IOException ignored) {
        LOG.warn(getId() + ": Failed to close " + getRpcType() + " server", ignored);
      }
    });
  }

  private <REPLY> CompletableFuture<REPLY> submitRequest(RaftGroupId groupId,
      CheckedFunction<RaftServerImpl, CompletableFuture<REPLY>, IOException> submitFunction) {
    return getImplFuture(groupId).thenCompose(
        impl -> JavaUtils.callAsUnchecked(() -> submitFunction.apply(impl), CompletionException::new));
  }

  @Override
  public CompletableFuture<RaftClientReply> submitClientRequestAsync(RaftClientRequest request) {
    return submitRequest(request.getRaftGroupId(), impl -> impl.submitClientRequestAsync(request));
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return getImpl(request.getRaftGroupId()).submitClientRequest(request);
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    return getImpl(request.getRaftGroupId()).setConfiguration(request);
  }

  @Override
  public RaftClientReply reinitialize(ReinitializeRequest request) throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, reinitializeAsync(request),
        e -> new RaftClientReply(request, e, null));
  }

  @Override
  public CompletableFuture<RaftClientReply> reinitializeAsync(
      ReinitializeRequest request) throws IOException {
    LOG.info("{}: reinitialize* {}", getId(), request);
    if (!reinitializeRequest.compareAndSet(null, request)) {
      throw new IOException("Another reinitialize is already in progress.");
    }
    final RaftGroupId oldGroupId = request.getRaftGroupId();
    return getImplFuture(oldGroupId)
        .thenAcceptAsync(RaftServerImpl::shutdown)
        .thenAccept(_1 -> impls.remove(oldGroupId))
        .thenCompose(_1 -> impls.addNew(request.getGroup()))
        .thenApply(newImpl -> {
          LOG.debug("{}: newImpl = {}", getId(), newImpl);
          final boolean started = newImpl.start();
          Preconditions.assertTrue(started, () -> getId()+ ": failed to start a new impl: " + newImpl);
          return new RaftClientReply(request, newImpl.getCommitInfos());
        })
        .whenComplete((_1, throwable) -> {
          if (throwable != null) {
            impls.remove(request.getGroup().getGroupId());
            LOG.warn(getId() + ": Failed reinitialize* " + request, throwable);
          }

          reinitializeRequest.set(null);
        });
  }

  @Override
  public ServerInformationReply getInfo(ServerInformationRequest request)
      throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, getInfoAsync(request),
        r -> null);
  }

  @Override
  public CompletableFuture<ServerInformationReply> getInfoAsync(
      ServerInformationRequest request) {
    return getImplFuture(request.getRaftGroupId()).thenApplyAsync(
        server -> server.getServerInformation(request));
  }

  /**
   * Handle a raft configuration change request from client.
   */
  @Override
  public CompletableFuture<RaftClientReply> setConfigurationAsync(SetConfigurationRequest request) {
    return submitRequest(request.getRaftGroupId(), impl -> impl.setConfigurationAsync(request));
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) throws IOException {
    return getImpl(request.getServerRequest()).requestVote(request);
  }

  @Override
  public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(AppendEntriesRequestProto request) {
    final RaftGroupId groupId = ProtoUtils.toRaftGroupId(request.getServerRequest().getRaftGroupId());
    return submitRequest(groupId, impl -> impl.appendEntriesAsync(request));
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) throws IOException {
    return getImpl(request.getServerRequest()).appendEntries(request);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    return getImpl(request.getServerRequest()).installSnapshot(request);
  }

  @Override
  public String toString() {
    return getId() + String.format(":%9s ", lifeCycle.getCurrentState()) + impls;
  }
}
