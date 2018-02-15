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
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class RaftServerProxy implements RaftServer {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerProxy.class);

  private final RaftPeerId id;
  private final StateMachine stateMachine;
  private final RaftProperties properties;

  private final RaftServerRpc serverRpc;
  private final ServerFactory factory;

  private volatile CompletableFuture<RaftServerImpl> impl;
  private final AtomicReference<ReinitializeRequest> reinitializeRequest = new AtomicReference<>();

  RaftServerProxy(RaftPeerId id, StateMachine stateMachine,
      RaftGroup group, RaftProperties properties, Parameters parameters)
      throws IOException {
    this.properties = properties;
    this.stateMachine = stateMachine;

    final RpcType rpcType = RaftConfigKeys.Rpc.type(properties);
    this.factory = ServerFactory.cast(rpcType.newFactory(parameters));

    this.serverRpc = factory.newRaftServerRpc(this);
    this.id = id != null? id: RaftPeerId.valueOf(getIdStringFrom(serverRpc));
    try {
      this.impl = CompletableFuture.completedFuture(initImpl(group));
    } catch (IOException ioe) {
      try {
        serverRpc.close();
      } catch (IOException closeIoe) {
        ioe.addSuppressed(closeIoe);
      } finally {
        throw ioe;
      }
    }
  }

  private RaftServerImpl initImpl(RaftGroup group) throws IOException {
    return new RaftServerImpl(id, group, this, properties);
  }

  private static String getIdStringFrom(RaftServerRpc rpc) {
    final InetSocketAddress address = rpc.getInetSocketAddress();
    return address != null? address.getHostName() + "_" + address.getPort()
        : rpc.getRpcType() + "-" + UUID.randomUUID();
  }

  @Override
  public RaftPeerId getId() {
    return id;
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
  public StateMachine getStateMachine() {
    return stateMachine;
  }

  @Override
  public RaftProperties getProperties() {
    return properties;
  }

  public RaftServerRpc getServerRpc() {
    return serverRpc;
  }

  public RaftServerImpl getImpl() throws IOException {
    try {
      return impl.get();
    } catch (InterruptedException e) {
      throw IOUtils.toInterruptedIOException(getId() + ": getImpl interrupted.", e);
    } catch (ExecutionException e) {
      throw IOUtils.asIOException(e);
    }
  }

  @Override
  public void start() {
    JavaUtils.getAndConsume(impl, RaftServerImpl::start);
    getServerRpc().start();
  }

  @Override
  public void close() {
    JavaUtils.getAndConsume(impl, RaftServerImpl::shutdown);
    try {
      getServerRpc().close();
    } catch (IOException ignored) {
      LOG.warn("Failed to close RPC server for " + getId(), ignored);
    }
  }

  @Override
  public CompletableFuture<RaftClientReply> submitClientRequestAsync(
      RaftClientRequest request) throws IOException {
    return getImpl().submitClientRequestAsync(request);
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return getImpl().submitClientRequest(request);
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    return getImpl().setConfiguration(request);
  }

  @Override
  public RaftClientReply reinitialize(ReinitializeRequest request) throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, reinitializeAsync(request),
        e -> new RaftClientReply(request, e, null));
  }

  @Override
  public CompletableFuture<RaftClientReply> reinitializeAsync(
      ReinitializeRequest request) throws IOException {
    LOG.info("{}: reinitializeAsync {}", getId(), request);
    getImpl().assertGroup(request.getRequestorId(), request.getRaftGroupId());
    if (!reinitializeRequest.compareAndSet(null, request)) {
      throw new IOException("Another reinitialize is already in progress.");
    }

    return CompletableFuture.supplyAsync(() -> {
      try {
        final CompletableFuture<RaftServerImpl> oldImpl = impl;
        impl = new CompletableFuture<>();
        JavaUtils.getAndConsume(oldImpl, RaftServerImpl::shutdown);

        final RaftServerImpl newImpl;
        try {
          newImpl = initImpl(request.getGroup());
        } catch (IOException ioe) {
          final RaftException re = new RaftException(
              "Failed to reinitialize, request=" + request, ioe);
          impl.completeExceptionally(new IOException(
              "Server " + getId() + " is not initialized.", re));
          return new RaftClientReply(request, re, null);
        }

        getServerRpc().addPeers(request.getGroup().getPeers());
        newImpl.start();
        impl.complete(newImpl);
        return new RaftClientReply(request, newImpl.getCommitInfos());
      } finally {
        reinitializeRequest.set(null);
      }
    });
  }

  @Override
  public ServerInformationReply getInfo(ServerInformatonRequest request)
      throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, getInfoAsync(request),
        r -> null);
  }

  @Override
  public CompletableFuture<ServerInformationReply> getInfoAsync(
      ServerInformatonRequest request) {
    return impl.thenApply(server -> server.getServerInformation(request));
  }

  /**
   * Handle a raft configuration change request from client.
   */
  @Override
  public CompletableFuture<RaftClientReply> setConfigurationAsync(
      SetConfigurationRequest request) throws IOException {
    return getImpl().setConfigurationAsync(request);
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto r)
      throws IOException {
    return getImpl().requestVote(r);
  }

  @Override
  public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(
      AppendEntriesRequestProto r) throws IOException {
    return getImpl().appendEntriesAsync(r);
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto r)
      throws IOException {
    return getImpl().appendEntries(r);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    return getImpl().installSnapshot(request);
  }

  @Override
  public String toString() {
    try {
      return getImpl().toString();
    } catch (IOException ignored) {
      return getClass().getSimpleName() + ":" + getId();
    }
  }
}
