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
package org.apache.ratis.server.impl;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.AlreadyExistsException;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.ServerFactory;
import org.apache.ratis.util.JvmPauseMonitor;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedFunction;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RaftServerProxy implements RaftServer {
  /**
   * A map: {@link RaftGroupId} -> {@link RaftServerImpl} futures.
   *
   * The map is synchronized for mutations and the bulk {@link #getGroupIds()}/{@link #getAll()} methods
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
      LOG.info("{}: addNew {} returns {}", getId(), group, toString(groupId, newImpl));
      return newImpl;
    }

    synchronized CompletableFuture<RaftServerImpl> remove(RaftGroupId groupId) {
      if (!map.containsKey(groupId)) {
        LOG.warn("{}: does not contain group: {}", getId(), groupId);
        return null;
      }

      final CompletableFuture<RaftServerImpl> future = map.remove(groupId);
      LOG.info("{}: remove {}", getId(), toString(groupId, future));
      return future;
    }

    @Override
    public synchronized void close() {
      if (isClosed) {
        LOG.info("{} is already closed.", getId());
        return;
      }
      isClosed = true;
      map.entrySet().parallelStream().forEach(entry -> close(entry.getKey(), entry.getValue()));
    }

    private void close(RaftGroupId groupId, CompletableFuture<RaftServerImpl> future) {
      final RaftServerImpl impl;
      try {
        impl = future.join();
      } catch (Throwable t) {
        LOG.warn("{}: Failed to join the division for {}", getId(), groupId, t);
        return;
      }
      try {
        impl.close();
      } catch (Throwable t) {
        LOG.warn("{}: Failed to close the division for {}", getId(), groupId, t);
      }
    }

    synchronized List<RaftGroupId> getGroupIds() {
      return new ArrayList<>(map.keySet());
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
  private final Supplier<RaftPeer> peerSupplier = JavaUtils.memoize(this::buildRaftPeer);
  private final RaftProperties properties;
  private final StateMachine.Registry stateMachineRegistry;
  private final LifeCycle lifeCycle;

  private final RaftServerRpc serverRpc;
  private final ServerFactory factory;

  private final DataStreamServerRpc dataStreamServerRpc;

  private final ImplMap impls = new ImplMap();
  private final ExecutorService implExecutor = Executors.newSingleThreadExecutor();

  private final JvmPauseMonitor pauseMonitor;

  RaftServerProxy(RaftPeerId id, StateMachine.Registry stateMachineRegistry,
      RaftProperties properties, Parameters parameters) {
    this.properties = properties;
    this.stateMachineRegistry = stateMachineRegistry;

    final RpcType rpcType = RaftConfigKeys.Rpc.type(properties, LOG::info);
    this.factory = ServerFactory.cast(rpcType.newFactory(parameters));

    this.serverRpc = factory.newRaftServerRpc(this);

    this.id = id != null? id: RaftPeerId.valueOf(getIdStringFrom(serverRpc));
    this.lifeCycle = new LifeCycle(this.id + "-" + JavaUtils.getClassSimpleName(getClass()));

    this.dataStreamServerRpc = new DataStreamServerImpl(this, parameters).getServerRpc();

    final TimeDuration rpcSlownessTimeout = RaftServerConfigKeys.Rpc.slownessTimeout(properties);
    final TimeDuration leaderStepDownWaitTime = RaftServerConfigKeys.LeaderElection.leaderStepDownWaitTime(properties);
    this.pauseMonitor = new JvmPauseMonitor(id,
        extraSleep -> handleJvmPause(extraSleep, rpcSlownessTimeout, leaderStepDownWaitTime));
  }

  private void handleJvmPause(TimeDuration extraSleep, TimeDuration closeThreshold, TimeDuration stepDownThreshold)
      throws IOException {
    if (extraSleep.compareTo(closeThreshold) > 0) {
      close();
    } else if (extraSleep.compareTo(stepDownThreshold) > 0) {
      getImpls().forEach(RaftServerImpl::stepDownOnJvmPause);
    }
  }

  /** Check the storage dir and add groups*/
  void initGroups(RaftGroup group) {

    final Optional<RaftGroup> raftGroup = Optional.ofNullable(group);
    final Optional<RaftGroupId> raftGroupId = raftGroup.map(RaftGroup::getGroupId);

    RaftServerConfigKeys.storageDir(properties).parallelStream()
        .forEach((dir) -> Optional.ofNullable(dir.listFiles())
            .map(Arrays::stream).orElse(Stream.empty())
            .filter(File::isDirectory)
            .forEach(sub -> {
              try {
                LOG.info("{}: found a subdirectory {}", getId(), sub);
                final RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString(sub.getName()));
                if (!raftGroupId.filter(groupId::equals).isPresent()) {
                  addGroup(RaftGroup.valueOf(groupId));
                }
              } catch (Exception e) {
                LOG.warn(getId() + ": Failed to initialize the group directory "
                    + sub.getAbsolutePath() + ".  Ignoring it", e);
              }
            }));
    raftGroup.ifPresent(this::addGroup);
  }

  void addRaftPeers(Collection<RaftPeer> peers) {
    final List<RaftPeer> others = peers.stream().filter(p -> !p.getId().equals(getId())).collect(Collectors.toList());
    getServerRpc().addRaftPeers(others);
    getDataStreamServerRpc().addRaftPeers(others);
  }

  private CompletableFuture<RaftServerImpl> newRaftServerImpl(RaftGroup group) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        addRaftPeers(group.getPeers());
        return new RaftServerImpl(group, stateMachineRegistry.apply(group.getGroupId()), this);
      } catch(IOException e) {
        throw new CompletionException(getId() + ": Failed to initialize server for " + group, e);
      }
    }, implExecutor);
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
  public RaftPeer getPeer() {
    return peerSupplier.get();
  }

  private RaftPeer buildRaftPeer() {
    return RaftPeer.newBuilder()
        .setId(getId())
        .setAddress(getServerRpc().getInetSocketAddress())
        .setDataStreamAddress(getDataStreamServerRpc().getInetSocketAddress())
        .build();
  }

  @Override
  public List<RaftGroupId> getGroupIds() {
    return impls.getGroupIds();
  }

  @Override
  public Iterable<RaftGroup> getGroups() throws IOException {
    return getImpls().stream().map(RaftServerImpl::getGroup).collect(Collectors.toList());
  }

  @Override
  public ServerFactory getFactory() {
    return factory;
  }

  @Override
  public RaftProperties getProperties() {
    return properties;
  }

  @Override
  public RaftServerRpc getServerRpc() {
    return serverRpc;
  }

  @Override
  public DataStreamServerRpc getDataStreamServerRpc() {
    return dataStreamServerRpc;
  }

  private CompletableFuture<RaftServerImpl> addGroup(RaftGroup group) {
    return impls.addNew(group);
  }

  private CompletableFuture<RaftServerImpl> getImplFuture(RaftGroupId groupId) {
    return impls.get(groupId);
  }

  private RaftServerImpl getImpl(RaftRpcRequestProto proto) throws IOException {
    return getImpl(ProtoUtils.toRaftGroupId(proto.getRaftGroupId()));
  }

  private RaftServerImpl getImpl(RaftGroupId groupId) throws IOException {
    Objects.requireNonNull(groupId, "groupId == null");
    return IOUtils.getFromFuture(getImplFuture(groupId), this::getId);
  }

  List<RaftServerImpl> getImpls() throws IOException {
    final List<RaftServerImpl> list = new ArrayList<>();
    for(CompletableFuture<RaftServerImpl> f : impls.getAll()) {
      list.add(IOUtils.getFromFuture(f, this::getId));
    }
    return list;
  }

  @Override
  public Division getDivision(RaftGroupId groupId) throws IOException {
    return getImpl(groupId);
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
      getDataStreamServerRpc().start();
    }, IOException.class);
    pauseMonitor.start();
  }

  @Override
  public void close() {
    try {
      implExecutor.shutdown();
      implExecutor.awaitTermination(1, TimeUnit.DAYS);
    } catch (Exception e) {
      LOG.warn(getId() + ": Failed to shutdown " + getRpcType() + " server");
    }

    lifeCycle.checkStateAndClose(() -> {
      LOG.info("{}: close", getId());
      impls.close();

      try {
        getServerRpc().close();
      } catch(IOException ignored) {
        LOG.warn(getId() + ": Failed to close " + getRpcType() + " server", ignored);
      }

      try {
        getDataStreamServerRpc().close();
      } catch (IOException ignored) {
        LOG.warn(getId() + ": Failed to close " + SupportedDataStreamType.NETTY + " server", ignored);
      }
    });
    pauseMonitor.stop();
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
  public RaftClientReply groupManagement(GroupManagementRequest request) throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, groupManagementAsync(request),
        e -> RaftClientReply.newBuilder()
            .setRequest(request)
            .setException(e)
            .build());
  }

  @Override
  public CompletableFuture<RaftClientReply> groupManagementAsync(GroupManagementRequest request) {
    final RaftGroupId groupId = request.getRaftGroupId();
    if (groupId == null) {
      return JavaUtils.completeExceptionally(new GroupMismatchException(
          getId() + ": Request group id == null"));
    }
    final GroupManagementRequest.Add add = request.getAdd();
    if (add != null) {
      return groupAddAsync(request, add.getGroup());
    }
    final GroupManagementRequest.Remove remove = request.getRemove();
    if (remove != null) {
      return groupRemoveAsync(request, remove.getGroupId(),
          remove.isDeleteDirectory(), remove.isRenameDirectory());
    }
    return JavaUtils.completeExceptionally(new UnsupportedOperationException(
        getId() + ": Request not supported " + request));
  }

  private CompletableFuture<RaftClientReply> groupAddAsync(GroupManagementRequest request, RaftGroup newGroup) {
    if (!request.getRaftGroupId().equals(newGroup.getGroupId())) {
      return JavaUtils.completeExceptionally(new GroupMismatchException(
          getId() + ": Request group id (" + request.getRaftGroupId() + ") does not match the new group " + newGroup));
    }
    return impls.addNew(newGroup)
        .thenApplyAsync(newImpl -> {
          LOG.debug("{}: newImpl = {}", getId(), newImpl);
          final boolean started = newImpl.start();
          Preconditions.assertTrue(started, () -> getId()+ ": failed to start a new impl: " + newImpl);
          return newImpl.newSuccessReply(request);
        }, implExecutor)
        .whenComplete((raftClientReply, throwable) -> {
          if (throwable != null) {
            if (!(throwable.getCause() instanceof AlreadyExistsException)) {
              impls.remove(newGroup.getGroupId());
              LOG.warn(getId() + ": Failed groupAdd* " + request, throwable);
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug(getId() + ": Failed groupAdd* " + request, throwable);
              }
            }
          }
        });
  }

  private CompletableFuture<RaftClientReply> groupRemoveAsync(
      RaftClientRequest request, RaftGroupId groupId, boolean deleteDirectory,
      boolean renameDirectory) {
    if (!request.getRaftGroupId().equals(groupId)) {
      return JavaUtils.completeExceptionally(new GroupMismatchException(
          getId() + ": Request group id (" + request.getRaftGroupId() + ") does not match the given group id " +
              groupId));
    }
    final CompletableFuture<RaftServerImpl> f = impls.remove(groupId);
    if (f == null) {
      return JavaUtils.completeExceptionally(new GroupMismatchException(
          getId() + ": Group " + groupId + " not found."));
    }
    return f.thenApply(impl -> {
      impl.groupRemove(deleteDirectory, renameDirectory);
      return impl.newSuccessReply(request);
    });
  }

  @Override
  public GroupListReply getGroupList(GroupListRequest request) {
    return new GroupListReply(request, getGroupIds());
  }

  @Override
  public CompletableFuture<GroupListReply> getGroupListAsync(GroupListRequest request) {
    return CompletableFuture.completedFuture(getGroupList(request));
  }

  @Override
  public GroupInfoReply getGroupInfo(GroupInfoRequest request) throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, getGroupInfoAsync(request), r -> null);
  }

  @Override
  public CompletableFuture<GroupInfoReply> getGroupInfoAsync(GroupInfoRequest request) {
    return getImplFuture(request.getRaftGroupId()).thenApplyAsync(
        server -> server.getGroupInfo(request));
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
