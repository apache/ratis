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
package org.apache.ratis.grpc;

import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.client.GrpcClientRpc;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.grpc.server.GrpcServicesImpl;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.ServerFactory;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class GrpcFactory implements ServerFactory, ClientFactory {

  public static final Logger LOG = LoggerFactory.getLogger(GrpcFactory.class);

  static final String USE_CACHE_FOR_ALL_THREADS_NAME = "useCacheForAllThreads";
  static final String USE_CACHE_FOR_ALL_THREADS_KEY = "org.apache.ratis.thirdparty.io.netty.allocator."
      + USE_CACHE_FOR_ALL_THREADS_NAME;
  static {
    // see org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator.DEFAULT_USE_CACHE_FOR_ALL_THREADS
    final String value = JavaUtils.getSystemProperty(USE_CACHE_FOR_ALL_THREADS_KEY);
    if (value == null) {
      // Set the property to false, when it is not set.
      JavaUtils.setSystemProperty(USE_CACHE_FOR_ALL_THREADS_KEY, Boolean.FALSE.toString());
    }
  }

  static boolean checkPooledByteBufAllocatorUseCacheForAllThreads(Consumer<String> log) {
    final boolean value = PooledByteBufAllocator.defaultUseCacheForAllThreads();
    if (value) {
      log.accept("PERFORMANCE WARNING: " + USE_CACHE_FOR_ALL_THREADS_NAME + " is " + true
          + " that may cause Netty to create a lot garbage objects and, as a result, trigger GC.\n"
          + "\tIt is recommended to disable " + USE_CACHE_FOR_ALL_THREADS_NAME
          + " by setting -D" + USE_CACHE_FOR_ALL_THREADS_KEY + "=" + false + " in command line.");
    }
    return value;
  }

  static final BiFunction<GrpcTlsConfig, SslContext, SslContext> BUILD_SSL_CONTEXT_FOR_SERVER
      = (tlsConf, defaultContext) -> tlsConf == null ? defaultContext : GrpcUtil.buildSslContextForServer(tlsConf);

  static final BiFunction<GrpcTlsConfig, SslContext, SslContext> BUILD_SSL_CONTEXT_FOR_CLIENT
      = (tlsConf, defaultContext) -> tlsConf == null ? defaultContext : GrpcUtil.buildSslContextForClient(tlsConf);

  static final class SslContexts {
    private final SslContext adminSslContext;
    private final SslContext clientSslContext;
    private final SslContext serverSslContext;

    private SslContexts(GrpcTlsConfig tlsConfig, GrpcTlsConfig adminTlsConfig,
        GrpcTlsConfig clientTlsConfig, GrpcTlsConfig serverTlsConfig,
        BiFunction<GrpcTlsConfig, SslContext, SslContext> buildMethod) {
      final SslContext defaultSslContext = buildMethod.apply(tlsConfig, null);
      this.adminSslContext = buildMethod.apply(adminTlsConfig, defaultSslContext);
      this.clientSslContext = buildMethod.apply(clientTlsConfig, defaultSslContext);
      this.serverSslContext = buildMethod.apply(serverTlsConfig, defaultSslContext);
    }
  }

  private final GrpcServices.Customizer servicesCustomizer;

  private final Supplier<SslContexts> forServerSupplier;
  private final Supplier<SslContexts> forClientSupplier;

  public GrpcFactory(Parameters parameters) {
    this(GrpcConfigKeys.Server.servicesCustomizer(parameters),
        GrpcConfigKeys.TLS.conf(parameters),
        GrpcConfigKeys.Admin.tlsConf(parameters),
        GrpcConfigKeys.Client.tlsConf(parameters),
        GrpcConfigKeys.Server.tlsConf(parameters)
    );
  }

  private GrpcFactory(GrpcServices.Customizer servicesCustomizer,
      GrpcTlsConfig tlsConfig, GrpcTlsConfig adminTlsConfig,
      GrpcTlsConfig clientTlsConfig, GrpcTlsConfig serverTlsConfig) {
    this.servicesCustomizer = servicesCustomizer;

    this.forServerSupplier = MemoizedSupplier.valueOf(() -> new SslContexts(
        tlsConfig, adminTlsConfig, clientTlsConfig, serverTlsConfig, BUILD_SSL_CONTEXT_FOR_SERVER));
    this.forClientSupplier = MemoizedSupplier.valueOf(() -> new SslContexts(
        tlsConfig, adminTlsConfig, clientTlsConfig, serverTlsConfig, BUILD_SSL_CONTEXT_FOR_CLIENT));
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.GRPC;
  }

  @Override
  public LogAppender newLogAppender(RaftServer.Division server, LeaderState state, FollowerInfo f) {
    return new GrpcLogAppender(server, state, f);
  }

  @Override
  public GrpcServices newRaftServerRpc(RaftServer server) {
    checkPooledByteBufAllocatorUseCacheForAllThreads(LOG::info);

    final SslContexts forServer = forServerSupplier.get();
    final SslContexts forClient = forClientSupplier.get();
    return GrpcServicesImpl.newBuilder()
        .setServer(server)
        .setCustomizer(servicesCustomizer)
        .setAdminSslContext(forServer.adminSslContext)
        .setServerSslContextForServer(forServer.serverSslContext)
        .setServerSslContextForClient(forClient.serverSslContext)
        .setClientSslContext(forServer.clientSslContext)
        .build();
  }

  @Override
  public GrpcClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties) {
    checkPooledByteBufAllocatorUseCacheForAllThreads(LOG::debug);

    final SslContexts forClient = forClientSupplier.get();
    return new GrpcClientRpc(clientId, properties, forClient.adminSslContext, forClient.clientSslContext);
  }
}
