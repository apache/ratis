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
package org.apache.ratis.grpc.server;

import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.client.GrpcClientProtocolService;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.RaftServerRpcWithProxy;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;

/** A grpc implementation of {@link RaftServerRpc}. */
public class GrpcService extends RaftServerRpcWithProxy<GrpcServerProtocolClient, PeerProxyMap<GrpcServerProtocolClient>> {
  static final Logger LOG = LoggerFactory.getLogger(GrpcService.class);
  public static final String GRPC_SEND_SERVER_REQUEST =
      GrpcService.class.getSimpleName() + ".sendRequest";

  public static class Builder extends RaftServerRpc.Builder<Builder, GrpcService> {
    private Builder() {}

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public GrpcService build() {
      return new GrpcService(getServer());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final Server server;
  private final Supplier<InetSocketAddress> addressSupplier;

  private GrpcService(RaftServer server) {
    this(server, server::getId,
        GrpcConfigKeys.Server.port(server.getProperties()),
        GrpcConfigKeys.messageSizeMax(server.getProperties(), LOG::info),
        RaftServerConfigKeys.Log.Appender.bufferCapacity(server.getProperties()),
        GrpcConfigKeys.flowControlWindow(server.getProperties(), LOG::info),
        RaftServerConfigKeys.Rpc.requestTimeout(server.getProperties()));
  }
  private GrpcService(RaftServer raftServer, Supplier<RaftPeerId> idSupplier, int port,
      SizeInBytes grpcMessageSizeMax, SizeInBytes appenderBufferSize,
      SizeInBytes flowControlWindow, TimeDuration requestTimeoutDuration) {
    super(idSupplier, id -> new PeerProxyMap<>(id.toString(),
        p -> new GrpcServerProtocolClient(p, flowControlWindow.getSizeInt(), requestTimeoutDuration)));
    if (appenderBufferSize.getSize() > grpcMessageSizeMax.getSize()) {
      throw new IllegalArgumentException("Illegal configuration: "
          + RaftServerConfigKeys.Log.Appender.BUFFER_CAPACITY_KEY + " = " + appenderBufferSize
          + " > " + GrpcConfigKeys.MESSAGE_SIZE_MAX_KEY + " = " + grpcMessageSizeMax);
    }

    server = NettyServerBuilder.forPort(port)
        .maxInboundMessageSize(grpcMessageSizeMax.getSizeInt())
        .flowControlWindow(flowControlWindow.getSizeInt())
        .addService(new GrpcServerProtocolService(idSupplier, raftServer))
        .addService(new GrpcClientProtocolService(idSupplier, raftServer))
        .addService(new GrpcAdminProtocolService(raftServer))
        .build();
    addressSupplier = JavaUtils.memoize(() -> new InetSocketAddress(port != 0? port: server.getPort()));
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.GRPC;
  }

  @Override
  public void startImpl() {
    try {
      server.start();
    } catch (IOException e) {
      ExitUtils.terminate(1, "Failed to start Grpc server", e, LOG);
    }
    LOG.info("{}: {} started, listening on {}", getId(), getClass().getSimpleName(), getInetSocketAddress());
  }

  @Override
  public void closeImpl() throws IOException {
    final String name = getId() + ": shutdown server with port " + server.getPort();
    LOG.info("{} now", name);
    final Server s = server.shutdownNow();
    super.closeImpl();
    try {
      s.awaitTermination();
    } catch(InterruptedException e) {
      throw IOUtils.toInterruptedIOException(name + " failed", e);
    }
    LOG.info("{} successfully", name);
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return addressSupplier.get();
  }

  @Override
  public AppendEntriesReplyProto appendEntries(
      AppendEntriesRequestProto request) throws IOException {
    throw new UnsupportedOperationException(
        "Blocking AppendEntries call is not supported");
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    throw new UnsupportedOperationException(
        "Blocking InstallSnapshot call is not supported");
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, getId(),
        null, request);

    final RaftPeerId target = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
    return getProxies().getProxy(target).requestVote(request);
  }
}
