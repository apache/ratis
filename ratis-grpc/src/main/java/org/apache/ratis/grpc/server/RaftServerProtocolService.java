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

import org.apache.ratis.grpc.RaftGrpcUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.shaded.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceImplBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftServerProtocolService extends RaftServerProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerProtocolService.class);

  private final RaftPeerId id;
  private final RaftServerProtocol server;

  public RaftServerProtocolService(RaftPeerId id, RaftServerProtocol server) {
    this.id = id;
    this.server = server;
  }

  @Override
  public void requestVote(RequestVoteRequestProto request,
      StreamObserver<RequestVoteReplyProto> responseObserver) {
    try {
      final RequestVoteReplyProto reply = server.requestVote(request);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Throwable e) {
      LOG.info("{} got exception when handling requestVote {}: {}",
          id, request.getServerRequest(), e);
      responseObserver.onError(RaftGrpcUtil.wrapException(e));
    }
  }

  @Override
  public StreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseObserver) {
    return new StreamObserver<AppendEntriesRequestProto>() {
      @Override
      public void onNext(AppendEntriesRequestProto request) {
        try {
          final AppendEntriesReplyProto reply = server.appendEntries(request);
          responseObserver.onNext(reply);
        } catch (Throwable e) {
          LOG.info("{} got exception when handling appendEntries {}: {}",
              id, request.getServerRequest(), e);
          responseObserver.onError(RaftGrpcUtil.wrapException(e));
        }
      }

      @Override
      public void onError(Throwable t) {
        // for now we just log a msg
        LOG.info("{}: appendEntries on error. Exception: {}", id, t);
      }

      @Override
      public void onCompleted() {
        LOG.info("{}: appendEntries completed", id);
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      StreamObserver<InstallSnapshotReplyProto> responseObserver) {
    return new StreamObserver<InstallSnapshotRequestProto>() {
      @Override
      public void onNext(InstallSnapshotRequestProto request) {
        try {
          final InstallSnapshotReplyProto reply = server.installSnapshot(request);
          responseObserver.onNext(reply);
        } catch (Throwable e) {
          LOG.info("{} got exception when handling installSnapshot {}: {}",
              id, request.getServerRequest(), e);
          responseObserver.onError(RaftGrpcUtil.wrapException(e));
        }
      }

      @Override
      public void onError(Throwable t) {
        LOG.info("{}: installSnapshot on error. Exception: {}", id, t);
      }

      @Override
      public void onCompleted() {
        LOG.info("{}: installSnapshot completed", id);
        responseObserver.onCompleted();
      }
    };
  }
}
