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

import org.apache.ratis.shaded.io.grpc.ManagedChannel;
import org.apache.ratis.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.shaded.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.shaded.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceBlockingStub;
import org.apache.ratis.shaded.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceStub;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

/**
 * This is a RaftClient implementation that supports streaming data to the raft
 * ring. The stream implementation utilizes gRPC.
 */
public class RaftServerProtocolClient {
  private final ManagedChannel channel;
  private TimeDuration timeout = TimeDuration.valueOf(3, TimeUnit.SECONDS);
  private final RaftServerProtocolServiceBlockingStub blockingStub;
  private final RaftServerProtocolServiceStub asyncStub;

  public RaftServerProtocolClient(RaftPeer target, int flowControlWindow) {
    channel = NettyChannelBuilder.forTarget(target.getAddress())
        .usePlaintext(true).flowControlWindow(flowControlWindow)
        .build();
    blockingStub = RaftServerProtocolServiceGrpc.newBlockingStub(channel);
    asyncStub = RaftServerProtocolServiceGrpc.newStub(channel);
  }

  public void shutdown() {
    channel.shutdownNow();
  }

  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) {
    // the StatusRuntimeException will be handled by the caller
    TimeUnit unit = timeout.getUnit();
    RequestVoteReplyProto r= blockingStub.withDeadlineAfter(timeout.toInt(unit), unit).requestVote(request);
    return r;
  }

  StreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseHandler) {
    return asyncStub.appendEntries(responseHandler);
  }

  StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      StreamObserver<InstallSnapshotReplyProto> responseHandler) {
    TimeUnit unit = timeout.getUnit();
    return asyncStub.withDeadlineAfter(timeout.toInt(unit), unit).installSnapshot(responseHandler);
  }
}
