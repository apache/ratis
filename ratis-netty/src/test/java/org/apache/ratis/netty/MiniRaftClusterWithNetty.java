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
package org.apache.ratis.netty;

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RpcType;
import org.apache.ratis.client.RaftClientRequestSender;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.client.NettyClientRequestSender;
import org.apache.ratis.netty.server.NettyRpcService;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.server.impl.RaftServerImpl;

public class MiniRaftClusterWithNetty extends MiniRaftCluster.RpcBase {
  public static final Factory<MiniRaftClusterWithNetty> FACTORY
      = new Factory<MiniRaftClusterWithNetty>() {
    @Override
    public MiniRaftClusterWithNetty newCluster(
        String[] ids, RaftProperties prop, boolean formatted) {
      RaftConfigKeys.Rpc.setType(prop::setEnum, RpcType.NETTY);
      return new MiniRaftClusterWithNetty(ids, prop, formatted);
    }
  };

  public static final DelayLocalExecutionInjection sendServerRequest
      = new DelayLocalExecutionInjection(NettyRpcService.SEND_SERVER_REQUEST);

  private MiniRaftClusterWithNetty(
      String[] ids, RaftProperties properties, boolean formatted) {
    super(ids, properties, formatted);
  }

  @Override
  protected RaftServerImpl newRaftServer(RaftPeerId id, boolean format) {
    final RaftServerImpl s = super.newRaftServer(id, format);
    NettyConfigKeys.Server.setPort(s.getProperties()::setInt, getPort(s));
    return s;
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return new NettyClientRequestSender(getPeers());
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequest,
        leaderId, delayMs, getMaxTimeout());
  }
}
