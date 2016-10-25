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
package org.apache.raft.grpc.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.raft.grpc.proto.RaftClientProtocolServiceGrpc;
import org.apache.raft.grpc.proto.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceBlockingStub;
import org.apache.raft.grpc.proto.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceStub;
import org.apache.raft.proto.RaftProtos.RaftClientReplyProto;
import org.apache.raft.proto.RaftProtos.RaftClientRequestProto;
import org.apache.raft.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.raft.protocol.RaftPeer;

public class RaftClientProtocolClient {
  private final ManagedChannel channel;
  private final RaftClientProtocolServiceBlockingStub blockingStub;
  private final RaftClientProtocolServiceStub asyncStub;

  public RaftClientProtocolClient(RaftPeer target) {
    channel = ManagedChannelBuilder.forTarget(target.getAddress())
        .usePlaintext(true).build();
    blockingStub = RaftClientProtocolServiceGrpc.newBlockingStub(channel);
    asyncStub = RaftClientProtocolServiceGrpc.newStub(channel);
  }

  public void shutdown() {
    channel.shutdownNow();
  }

  public RaftClientReplyProto setConfiguration(
      SetConfigurationRequestProto request) {
    return blockingStub.setConfiguration(request);
  }

  StreamObserver<RaftClientRequestProto> append(
      StreamObserver<RaftClientReplyProto> responseHandler) {
    return asyncStub.append(responseHandler);
  }
}
