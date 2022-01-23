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
package org.apache.ratis.grpc.server;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.grpc.metrics.intercept.server.MetricServerInterceptor;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptors;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;

import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider.OPENSSL;

/** A grpc implementation of {@link org.apache.ratis.server.RaftServerRpc}. */
public final class GrpcService extends RaftServerRpcWithProxy<GrpcServerProtocolClient,
    PeerProxyMap<GrpcServerProtocolClient>> {
  static final Logger LOG = LoggerFactory.getLogger(GrpcService.class);
  public static final String GRPC_SEND_SERVER_REQUEST =
      JavaUtils.getClassSimpleName(GrpcService.class) + ".sendRequest";

  public static final class Builder {
    private RaftServer server;
    private GrpcTlsConfig tlsConfig;
    private GrpcTlsConfig adminTlsConfig;
    private GrpcTlsConfig clientTlsConfig;
    private GrpcTlsConfig serverTlsConfig;

    private Builder() {}

    public Builder setServer(RaftServer raftServer) {
      this.server = raftServer;
      return this;
    }

    public GrpcService build() {
      return new GrpcService(server, adminTlsConfig, clientTlsConfig, serverTlsConfig);
    }

    public Builder setTlsConfig(GrpcTlsConfig tlsConfig) {
      this.tlsConfig = tlsConfig;
      return this;
    }

    public Builder setAdminTlsConfig(GrpcTlsConfig config) {
      this.adminTlsConfig = config;
      return this;
    }

    public Builder setClientTlsConfig(GrpcTlsConfig config) {
      this.clientTlsConfig = config;
      return this;
    }

    public Builder setServerTlsConfig(GrpcTlsConfig config) {
      this.serverTlsConfig = config;
      return this;
    }

    public GrpcTlsConfig getTlsConfig() {
      return tlsConfig;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final List<Server> servers = new ArrayList<>(3);
  private final Supplier<InetSocketAddress> addressSupplier;
  private final Supplier<InetSocketAddress> clientServerAddressSupplier;
  private final Supplier<InetSocketAddress> adminServerAddressSupplier;

  private final ExecutorService executor;
  private final GrpcClientProtocolService clientProtocolService;

  private final MetricServerInterceptor serverInterceptor;

  public MetricServerInterceptor getServerInterceptor() {
    return serverInterceptor;
  }

  private GrpcService(RaftServer server,
      GrpcTlsConfig adminTlsConfig, GrpcTlsConfig clientTlsConfig, GrpcTlsConfig serverTlsConfig) {
    this(server, server::getId,
        GrpcConfigKeys.Admin.port(server.getProperties()), adminTlsConfig,
        GrpcConfigKeys.Client.port(server.getProperties()), clientTlsConfig,
        GrpcConfigKeys.Server.port(server.getProperties()), serverTlsConfig,
        GrpcConfigKeys.messageSizeMax(server.getProperties(), LOG::info),
        RaftServerConfigKeys.Log.Appender.bufferByteLimit(server.getProperties()),
        GrpcConfigKeys.flowControlWindow(server.getProperties(), LOG::info),
        RaftServerConfigKeys.Rpc.requestTimeout(server.getProperties()));
  }

  @SuppressWarnings("checkstyle:ParameterNumber") // private constructor
  private GrpcService(RaftServer raftServer, Supplier<RaftPeerId> idSupplier,
      int adminPort, GrpcTlsConfig adminTlsConfig,
      int clientPort, GrpcTlsConfig clientTlsConfig,
      int serverPort, GrpcTlsConfig serverTlsConfig,
      SizeInBytes grpcMessageSizeMax, SizeInBytes appenderBufferSize,
      SizeInBytes flowControlWindow,TimeDuration requestTimeoutDuration) {
    super(idSupplier, id -> new PeerProxyMap<>(id.toString(),
        p -> new GrpcServerProtocolClient(p, flowControlWindow.getSizeInt(),
            requestTimeoutDuration, serverTlsConfig)));
    if (appenderBufferSize.getSize() > grpcMessageSizeMax.getSize()) {
      throw new IllegalArgumentException("Illegal configuration: "
          + RaftServerConfigKeys.Log.Appender.BUFFER_BYTE_LIMIT_KEY + " = " + appenderBufferSize
          + " > " + GrpcConfigKeys.MESSAGE_SIZE_MAX_KEY + " = " + grpcMessageSizeMax);
    }

    final RaftProperties properties = raftServer.getProperties();
    this.executor = ConcurrentUtils.newThreadPoolWithMax(
        GrpcConfigKeys.Server.asyncRequestThreadPoolCached(properties),
        GrpcConfigKeys.Server.asyncRequestThreadPoolSize(properties),
        getId() + "-request-");
    this.clientProtocolService = new GrpcClientProtocolService(idSupplier, raftServer, executor);

    this.serverInterceptor = new MetricServerInterceptor(
        idSupplier,
        JavaUtils.getClassSimpleName(getClass()) + "_" + serverPort
    );

    final boolean separateAdminServer = adminPort != serverPort && adminPort > 0;
    final boolean separateClientServer = clientPort != serverPort && clientPort > 0;

    final NettyServerBuilder serverBuilder =
        startBuildingNettyServer(serverPort, serverTlsConfig, grpcMessageSizeMax, flowControlWindow);
    serverBuilder.addService(ServerInterceptors.intercept(
        new GrpcServerProtocolService(idSupplier, raftServer), serverInterceptor));
    if (!separateAdminServer) {
      addAdminService(raftServer, serverBuilder);
    }
    if (!separateClientServer) {
      addClientService(serverBuilder);
    }

    final Server server = serverBuilder.build();
    servers.add(server);
    addressSupplier = newAddressSupplier(serverPort, server);

    if (separateAdminServer) {
      final NettyServerBuilder builder =
          startBuildingNettyServer(adminPort, adminTlsConfig, grpcMessageSizeMax, flowControlWindow);
      addAdminService(raftServer, builder);
      final Server adminServer = builder.build();
      servers.add(adminServer);
      adminServerAddressSupplier = newAddressSupplier(adminPort, adminServer);
    } else {
      adminServerAddressSupplier = addressSupplier;
    }

    if (separateClientServer) {
      final NettyServerBuilder builder =
          startBuildingNettyServer(clientPort, clientTlsConfig, grpcMessageSizeMax, flowControlWindow);
      addClientService(builder);
      final Server clientServer = builder.build();
      servers.add(clientServer);
      clientServerAddressSupplier = newAddressSupplier(clientPort, clientServer);
    } else {
      clientServerAddressSupplier = addressSupplier;
    }
  }

  private MemoizedSupplier<InetSocketAddress> newAddressSupplier(int port, Server server) {
    return JavaUtils.memoize(() -> new InetSocketAddress(port != 0 ? port : server.getPort()));
  }

  private void addClientService(NettyServerBuilder builder) {
    builder.addService(ServerInterceptors.intercept(clientProtocolService, serverInterceptor));
  }

  private void addAdminService(RaftServer raftServer, NettyServerBuilder nettyServerBuilder) {
    nettyServerBuilder.addService(ServerInterceptors.intercept(
          new GrpcAdminProtocolService(raftServer),
          serverInterceptor));
  }

  private static NettyServerBuilder startBuildingNettyServer(int port, GrpcTlsConfig tlsConfig,
      SizeInBytes grpcMessageSizeMax, SizeInBytes flowControlWindow) {
    NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(port)
        .withChildOption(ChannelOption.SO_REUSEADDR, true)
        .maxInboundMessageSize(grpcMessageSizeMax.getSizeInt())
        .flowControlWindow(flowControlWindow.getSizeInt());

    if (tlsConfig != null) {
      SslContextBuilder sslContextBuilder =
          tlsConfig.isFileBasedConfig()?
              SslContextBuilder.forServer(tlsConfig.getCertChainFile(),
                  tlsConfig.getPrivateKeyFile()):
              SslContextBuilder.forServer(tlsConfig.getPrivateKey(),
                  tlsConfig.getCertChain());
      if (tlsConfig.getMtlsEnabled()) {
        sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
        if (tlsConfig.isFileBasedConfig()) {
          sslContextBuilder.trustManager(tlsConfig.getTrustStoreFile());
        } else {
            sslContextBuilder.trustManager(tlsConfig.getTrustStore());
        }
      }
      sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder, OPENSSL);
      try {
        nettyServerBuilder.sslContext(sslContextBuilder.build());
      } catch (Exception ex) {
        throw new IllegalArgumentException("Failed to build SslContext, tlsConfig=" + tlsConfig, ex);
      }
    }
    return nettyServerBuilder;
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.GRPC;
  }

  @Override
  public void startImpl() {
    for (Server server : servers) {
      try {
        server.start();
      } catch (IOException e) {
        ExitUtils.terminate(1, "Failed to start Grpc server", e, LOG);
      }
      LOG.info("{}: {} started, listening on {}",
          getId(), JavaUtils.getClassSimpleName(getClass()), server.getPort());
    }
  }

  @Override
  public void closeImpl() throws IOException {
    for (Server server : servers) {
      final String name = getId() + ": shutdown server with port " + server.getPort();
      LOG.info("{} now", name);
      final Server s = server.shutdownNow();
      super.closeImpl();
      try {
        s.awaitTermination();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw IOUtils.toInterruptedIOException(name + " failed", e);
      }
      LOG.info("{} successfully", name);
    }

    serverInterceptor.close();
    ConcurrentUtils.shutdownAndWait(executor);
  }

  @Override
  public void notifyNotLeader(RaftGroupId groupId) {
    clientProtocolService.closeAllOrderedRequestStreamObservers(groupId);
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return addressSupplier.get();
  }

  @Override
  public InetSocketAddress getClientServerAddress() {
    return clientServerAddressSupplier.get();
  }

  @Override
  public InetSocketAddress getAdminServerAddress() {
    return adminServerAddressSupplier.get();
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) {
    throw new UnsupportedOperationException(
        "Blocking " + JavaUtils.getCurrentStackTraceElement().getMethodName() + " call is not supported");
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) {
    throw new UnsupportedOperationException(
        "Blocking " + JavaUtils.getCurrentStackTraceElement().getMethodName() + " call is not supported");
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, getId(),
        null, request);

    final RaftPeerId target = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
    return getProxies().getProxy(target).requestVote(request);
  }

  @Override
  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, getId(), null, request);

    final RaftPeerId target = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
    return getProxies().getProxy(target).startLeaderElection(request);
  }

}
