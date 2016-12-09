/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.raft.netty.client;

import org.apache.raft.client.impl.ClientProtoUtils;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.netty.NettyRpcProxy;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.raft.shaded.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.raft.shaded.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.raft.shaded.proto.netty.NettyProtos.RaftNettyServerRequestProto;

import java.io.IOException;

public class NettyClientRequestSender implements RaftClientRequestSender {
  private final NettyRpcProxy.PeerMap proxies = new NettyRpcProxy.PeerMap();

  public NettyClientRequestSender(Iterable<RaftPeer> servers) {
    addServers(servers);
  }

  @Override
  public RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
    final String serverId = request.getReplierId();
    final NettyRpcProxy proxy = proxies.getProxy(serverId);

    final RaftNettyServerRequestProto.Builder b = RaftNettyServerRequestProto.newBuilder();
    final RaftRpcRequestProto rpcRequest;
    if (request instanceof SetConfigurationRequest) {
      final SetConfigurationRequestProto proto = ClientProtoUtils.toSetConfigurationRequestProto(
          (SetConfigurationRequest)request);
      b.setSetConfigurationRequest(proto);
      rpcRequest = proto.getRpcRequest();
    } else {
      final RaftClientRequestProto proto = ClientProtoUtils.toRaftClientRequestProto(request);
      b.setRaftClientRequest(proto);
      rpcRequest = proto.getRpcRequest();
    }
    return ClientProtoUtils.toRaftClientReply(
        proxy.send(rpcRequest, b.build()).getRaftClientReply());
  }

  @Override
  public void addServers(Iterable<RaftPeer> servers) {
    proxies.addPeers(servers);
  }

  @Override
  public void close() {
    proxies.close();
  }
}
