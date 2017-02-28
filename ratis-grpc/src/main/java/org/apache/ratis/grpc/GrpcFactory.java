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
import org.apache.ratis.grpc.client.RaftClientSenderWithGrpc;
import org.apache.ratis.grpc.server.GRpcLogAppender;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.impl.*;

public class GrpcFactory implements ServerFactory, ClientFactory {
  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.GRPC;
  }

  @Override
  public LogAppender newLogAppender(RaftServerImpl server, LeaderState state,
                                    FollowerInfo f) {
    return new GRpcLogAppender(server, state, f);
  }

  @Override
  public RaftGRpcService newRaftServerRpc(RaftServerImpl server) {
    return RaftGRpcService.newBuilder()
        .setServer(server)
        .build();
  }

  @Override
  public RaftClientSenderWithGrpc newRaftClientRequestSender() {
    return new RaftClientSenderWithGrpc();
  }
}
