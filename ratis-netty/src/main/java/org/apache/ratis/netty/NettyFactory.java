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
package org.apache.ratis.netty;

import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.client.NettyClientRpc;
import org.apache.ratis.netty.server.NettyRpcService;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.ServerFactory;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.leader.LogAppenderWithHeartbeatThread;

public class NettyFactory implements ServerFactory, ClientFactory {
  private final Parameters parameters;

  public NettyFactory(Parameters parameters) {
    this.parameters = parameters;
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.NETTY;
  }

  @Override
  public NettyRpcService newRaftServerRpc(RaftServer server) {
    return NettyRpcService.newBuilder().setServer(server).setParameters(parameters).build();
  }

  @Override
  public NettyClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties) {
    return new NettyClientRpc(clientId, properties, parameters);
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
