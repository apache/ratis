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
package org.apache.ratis.grpc.server;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.grpc.RaftGrpcUtil;
import org.apache.ratis.protocol.AdminAsynchronousProtocol;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.ServerInformationRequest;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.GroupManagementRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.ServerInformationReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.ServerInformationRequestProto;
import org.apache.ratis.shaded.proto.grpc.AdminProtocolServiceGrpc.AdminProtocolServiceImplBase;

public class AdminProtocolService extends AdminProtocolServiceImplBase {
  private final AdminAsynchronousProtocol protocol;

  public AdminProtocolService(AdminAsynchronousProtocol protocol) {
    this.protocol = protocol;
  }

  @Override
  public void groupManagement(GroupManagementRequestProto proto, StreamObserver<RaftClientReplyProto> responseObserver) {
    final GroupManagementRequest request = ClientProtoUtils.toGroupManagementRequest(proto);
    RaftGrpcUtil.asyncCall(responseObserver, () -> protocol.groupManagementAsync(request),
        ClientProtoUtils::toRaftClientReplyProto);
  }

  @Override
  public void serverInformation(ServerInformationRequestProto proto,
      StreamObserver<ServerInformationReplyProto> responseObserver) {
    final ServerInformationRequest request = ClientProtoUtils.toServerInformationRequest(proto);
    RaftGrpcUtil.asyncCall(responseObserver, () -> protocol.getInfoAsync(request),
        ClientProtoUtils::toServerInformationReplyProto);
  }
}
