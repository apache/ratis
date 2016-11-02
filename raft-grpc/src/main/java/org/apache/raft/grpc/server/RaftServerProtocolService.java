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

import io.grpc.stub.StreamObserver;
import org.apache.raft.grpc.proto.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceImplBase;
import org.apache.raft.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.raft.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.raft.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.raft.server.RequestDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftServerProtocolService extends RaftServerProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(RaftServerProtocolService.class);
  private final RequestDispatcher dispatcher;

  public RaftServerProtocolService(RequestDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public void requestVote(RequestVoteRequestProto request,
      StreamObserver<RequestVoteReplyProto> responseObserver) {
    try {
      final RequestVoteReplyProto reply = dispatcher.requestVote(request);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.info(dispatcher.getRaftServer().getId() +
          " got exception when handling requestVote " + request, e);
      responseObserver.onError(e);
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
        } catch (Exception e) {
          responseObserver.onError(e);
        }
      }

      @Override
      public void onError(Throwable t) {
        // for now we just log a msg
        LOG.info("{}: appendEntries on error. Exception: {}",
            dispatcher.getRaftServer().getId(), t);
      }

      @Override
      public void onCompleted() {
        LOG.info("{}: appendEntries completed",
            dispatcher.getRaftServer().getId());
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
        } catch (Exception e) {
          responseObserver.onError(e);
        }
      }

      @Override
      public void onError(Throwable t) {
        // TODO clean up partial downloaded snapshots
        LOG.info("{}: installSnapshot on error. Exception: {}",
            dispatcher.getRaftServer().getId(), t);
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }
}
