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
package org.apache.ratis.netty.client;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.RaftClientRpcWithProxy;
import org.apache.ratis.netty.NettyRpcProxy;
import org.apache.ratis.protocol.*;
import org.apache.ratis.shaded.proto.RaftProtos;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.ReinitializeRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.shaded.proto.netty.NettyProtos.RaftNettyServerRequestProto;

import java.io.IOException;

public class NettyClientRpc extends RaftClientRpcWithProxy<NettyRpcProxy> {
  public NettyClientRpc() {
    super(new NettyRpcProxy.PeerMap());
  }

  @Override
  public RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
    final RaftPeerId serverId = request.getServerId();
    final NettyRpcProxy proxy = getProxies().getProxy(serverId);

    final RaftNettyServerRequestProto.Builder b = RaftNettyServerRequestProto.newBuilder();
    final RaftRpcRequestProto rpcRequest;
    if (request instanceof ReinitializeRequest) {
      final ReinitializeRequestProto proto = ClientProtoUtils.toReinitializeRequestProto(
          (ReinitializeRequest)request);
      b.setReinitializeRequest(proto);
      rpcRequest = proto.getRpcRequest();
    } else if (request instanceof SetConfigurationRequest) {
      final SetConfigurationRequestProto proto = ClientProtoUtils.toSetConfigurationRequestProto(
          (SetConfigurationRequest)request);
      b.setSetConfigurationRequest(proto);
      rpcRequest = proto.getRpcRequest();
    } else if (request instanceof ServerInformatonRequest) {
      final RaftProtos.ServerInformationRequestProto proto = ClientProtoUtils.toServerInformationRequestProto(
          (ServerInformatonRequest)request);
      b.setServerInformationRequest(proto);
      rpcRequest = proto.getRpcRequest();
    } else {
      final RaftClientRequestProto proto = ClientProtoUtils.toRaftClientRequestProto(request);
      b.setRaftClientRequest(proto);
      rpcRequest = proto.getRpcRequest();
    }
    if (request instanceof ServerInformatonRequest) {
      return ClientProtoUtils.toServerInformationReply(
          proxy.send(rpcRequest, b.build()).getServerInfoReply());
    } else {
      return ClientProtoUtils.toRaftClientReply(
          proxy.send(rpcRequest, b.build()).getRaftClientReply());
    }
  }
}
