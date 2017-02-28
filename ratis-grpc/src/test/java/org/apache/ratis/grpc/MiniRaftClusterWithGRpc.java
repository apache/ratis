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
import org.apache.ratis.client.RaftClientRequestSender;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.client.RaftClientSenderWithGrpc;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.server.impl.RaftServerImpl;

import java.util.Collection;

public class MiniRaftClusterWithGRpc extends MiniRaftCluster.RpcBase {
  public static final Factory<MiniRaftClusterWithGRpc> FACTORY
      = new Factory<MiniRaftClusterWithGRpc>() {
    @Override
    public MiniRaftClusterWithGRpc newCluster(
        String[] ids, RaftProperties prop, boolean formatted) {
      RaftConfigKeys.Rpc.setType(prop::set, SupportedRpcType.GRPC);
      return new MiniRaftClusterWithGRpc(ids, prop, formatted);
    }
  };

  public static final DelayLocalExecutionInjection sendServerRequestInjection =
      new DelayLocalExecutionInjection(RaftGRpcService.GRPC_SEND_SERVER_REQUEST);

  private MiniRaftClusterWithGRpc(String[] ids, RaftProperties properties,
      boolean formatted) {
    super(ids, properties, formatted);
  }

  @Override
  protected RaftServerImpl newRaftServer(RaftPeerId id, boolean format) {
    final RaftServerImpl s = super.newRaftServer(id, format);
    s.getProperties().setInt(
        RaftGrpcConfigKeys.RAFT_GRPC_SERVER_PORT_KEY, getPort(s));
    return s;
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return new RaftClientSenderWithGrpc(getPeers());
  }

  @Override
  protected Collection<RaftPeer> addNewPeers(
      Collection<RaftServerImpl> newServers, boolean startService) {
    final Collection<RaftPeer> peers = toRaftPeers(newServers);
    for (RaftPeer p: peers) {
      final RaftServerImpl server = servers.get(p.getId());
      if (!startService) {
        BlockRequestHandlingInjection.getInstance().blockReplier(server.getId().toString());
      } else {
        server.start();
      }
    }
    return peers;
  }

  @Override
  public void startServer(RaftPeerId id) {
    super.startServer(id);
    BlockRequestHandlingInjection.getInstance().unblockReplier(id.toString());
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequestInjection,
        leaderId, delayMs, getMaxTimeout());
  }
}
