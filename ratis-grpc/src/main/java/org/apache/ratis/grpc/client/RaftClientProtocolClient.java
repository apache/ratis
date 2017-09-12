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
package org.apache.ratis.grpc.client;

import org.apache.ratis.grpc.RaftGrpcUtil;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.shaded.io.grpc.ManagedChannel;
import org.apache.ratis.shaded.io.grpc.ManagedChannelBuilder;
import org.apache.ratis.shaded.io.grpc.StatusRuntimeException;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos.ServerInformationRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.ServerInformationReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.ReinitializeRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.shaded.proto.grpc.AdminProtocolServiceGrpc;
import org.apache.ratis.shaded.proto.grpc.AdminProtocolServiceGrpc.AdminProtocolServiceBlockingStub;
import org.apache.ratis.shaded.proto.grpc.RaftClientProtocolServiceGrpc;
import org.apache.ratis.shaded.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceBlockingStub;
import org.apache.ratis.shaded.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceStub;
import org.apache.ratis.util.CheckedSupplier;

import java.io.Closeable;
import java.io.IOException;

public class RaftClientProtocolClient implements Closeable {
  private final RaftPeer target;
  private final ManagedChannel channel;
  private final RaftClientProtocolServiceBlockingStub blockingStub;
  private final RaftClientProtocolServiceStub asyncStub;
  private final AdminProtocolServiceBlockingStub adminBlockingStub;

  public RaftClientProtocolClient(RaftPeer target) {
    this.target = target;
    channel = ManagedChannelBuilder.forTarget(target.getAddress())
        .usePlaintext(true).build();
    blockingStub = RaftClientProtocolServiceGrpc.newBlockingStub(channel);
    asyncStub = RaftClientProtocolServiceGrpc.newStub(channel);
    adminBlockingStub = AdminProtocolServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public void close() {
    channel.shutdownNow();
  }

  RaftClientReplyProto reinitialize(
      ReinitializeRequestProto request) throws IOException {
    return blockingCall(() -> adminBlockingStub.reinitialize(request));
  }

  ServerInformationReplyProto serverInformation(
      ServerInformationRequestProto request) throws IOException {
    return adminBlockingStub.serverInformation(request);
  }

  RaftClientReplyProto setConfiguration(
      SetConfigurationRequestProto request) throws IOException {
    return blockingCall(() -> blockingStub.setConfiguration(request));
  }

  private static RaftClientReplyProto blockingCall(
      CheckedSupplier<RaftClientReplyProto, StatusRuntimeException> supplier
      ) throws IOException {
    try {
      return supplier.get();
    } catch (StatusRuntimeException e) {
      throw RaftGrpcUtil.unwrapException(e);
    }
  }

  StreamObserver<RaftClientRequestProto> append(
      StreamObserver<RaftClientReplyProto> responseHandler) {
    return asyncStub.append(responseHandler);
  }

  public RaftPeer getTarget() {
    return target;
  }
}
