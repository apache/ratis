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
package org.apache.raft.grpc.server;

import org.apache.raft.grpc.RaftGrpcUtil;
import org.apache.raft.server.impl.RequestDispatcher;
import org.apache.raft.shaded.io.grpc.stub.StreamObserver;
import org.apache.raft.shaded.proto.RaftProtos.*;
import org.apache.raft.shaded.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceImplBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftServerProtocolService extends RaftServerProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerProtocolService.class);

  private final String id;
  private final RequestDispatcher dispatcher;

  public RaftServerProtocolService(String id, RequestDispatcher dispatcher) {
    this.id = id;
    this.dispatcher = dispatcher;
  }

  @Override
  public void requestVote(RequestVoteRequestProto request,
      StreamObserver<RequestVoteReplyProto> responseObserver) {
    try {
      final RequestVoteReplyProto reply = dispatcher.requestVote(request);
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
          final AppendEntriesReplyProto reply = dispatcher.appendEntries(request);
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
          final InstallSnapshotReplyProto reply =
              dispatcher.installSnapshot(request);
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
