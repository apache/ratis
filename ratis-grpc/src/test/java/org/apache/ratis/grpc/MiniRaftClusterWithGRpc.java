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

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.impl.*;
import org.apache.ratis.statemachine.StateMachine;

import java.io.IOException;

public class MiniRaftClusterWithGRpc extends MiniRaftCluster.RpcBase {
  public static final Factory<MiniRaftClusterWithGRpc> FACTORY
      = new Factory<MiniRaftClusterWithGRpc>() {
    @Override
    public MiniRaftClusterWithGRpc newCluster(
        String[] ids, RaftProperties prop) {
      RaftConfigKeys.Rpc.setType(prop, SupportedRpcType.GRPC);
      return new MiniRaftClusterWithGRpc(ids, prop);
    }
  };

  public static final DelayLocalExecutionInjection sendServerRequestInjection =
      new DelayLocalExecutionInjection(RaftGRpcService.GRPC_SEND_SERVER_REQUEST);

  private MiniRaftClusterWithGRpc(String[] ids, RaftProperties properties) {
    super(ids, properties, null);
  }

  @Override
  protected RaftServerImpl newRaftServer(
      RaftPeerId id, StateMachine stateMachine, RaftConfiguration conf,
      RaftProperties properties) throws IOException {
    GrpcConfigKeys.Server.setPort(properties, getPort(id, conf));
    return ServerImplUtils.newRaftServer(id, stateMachine, conf, properties, null);
  }

  @Override
  protected void startServer(RaftServerImpl server, boolean startService) {
    final String id = server.getId().toString();
    if (startService) {
      server.start();
      BlockRequestHandlingInjection.getInstance().unblockReplier(id);
    } else {
      BlockRequestHandlingInjection.getInstance().blockReplier(id);
    }
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequestInjection,
        leaderId, delayMs, getMaxTimeout());
  }
}
