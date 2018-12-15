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
package org.apache.ratis.grpc;

import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.client.GrpcClientRpc;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.grpc.server.GrpcService;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.*;

public class GrpcFactory implements ServerFactory, ClientFactory {
  private final GrpcTlsConfig tlsConfig;

  public static Parameters newRaftParameters(GrpcTlsConfig conf) {
    final Parameters p = new Parameters();
    GrpcConfigKeys.TLS.setConf(p, conf);
    return p;
  }

  public GrpcFactory(Parameters parameters) {
    this(GrpcConfigKeys.TLS.getConf(parameters));
  }

  public GrpcFactory(GrpcTlsConfig tlsConfig) {
    this.tlsConfig = tlsConfig;
  }

  public GrpcTlsConfig getTlsConfig() {
    return tlsConfig;
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.GRPC;
  }

  @Override
  public LogAppender newLogAppender(RaftServerImpl server, LeaderState state,
                                    FollowerInfo f) {
    return new GrpcLogAppender(server, state, f);
  }

  @Override
  public GrpcService newRaftServerRpc(RaftServer server) {
    return GrpcService.newBuilder()
        .setServer(server)
        .setTlsConfig(tlsConfig)
        .build();
  }

  @Override
  public GrpcClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties) {
    return new GrpcClientRpc(clientId, properties, getTlsConfig());
  }
}
