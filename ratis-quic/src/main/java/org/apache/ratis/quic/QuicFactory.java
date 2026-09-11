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
package org.apache.ratis.quic;

import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.quic.client.QuicClientRpc;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.ServerFactory;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.leader.LogAppenderWithHeartbeatThread;

/**
 * Entry point for the QUIC transport.
 *
 * <p>Registered in {@link org.apache.ratis.rpc.SupportedRpcType#QUIC} so that
 * the standard Ratis bootstrapping code instantiates this class via reflection
 * when {@code raft.rpc.type = QUIC} is set in {@link RaftProperties}.
 *
 * <p>{@link Parameters} is accepted for API compatibility with the
 * {@link org.apache.ratis.rpc.RpcFactory} contract but is currently unused;
 * all QUIC configuration is carried in {@link RaftProperties} via
 * {@link QuicConfigKeys}.
 */
public class QuicFactory implements ServerFactory, ClientFactory {

  @SuppressWarnings("unused") // instantiated reflectively by SupportedRpcType
  public QuicFactory(Parameters parameters) {
    // Parameters not needed; QUIC config lives in RaftProperties.
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.QUIC;
  }

  @Override
  public QuicRpcService newRaftServerRpc(RaftServer server) {
    return QuicRpcService.newBuilder().setServer(server).build();
  }

  @Override
  public QuicClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties) {
    return new QuicClientRpc(clientId, properties);
  }

  /** Optional heartbeat thread (raft.server.log.appender.heartbeat.thread, HB-THREAD-CHANGES.md):
   *  heartbeats are sent next to an in-flight AppendEntries/InstallSnapshot instead of after its
   *  reply. Default false keeps the stock appender: one request in flight per follower. */
  @Override
  public LogAppender newLogAppender(RaftServer.Division server, LeaderState state, FollowerInfo f) {
    final RaftProperties properties = server.getRaftServer().getProperties();
    return RaftServerConfigKeys.Log.Appender.heartbeatThread(properties)
        ? new LogAppenderWithHeartbeatThread(server, state, f)
        : LogAppender.newLogAppenderDefault(server, state, f);
  }
}
