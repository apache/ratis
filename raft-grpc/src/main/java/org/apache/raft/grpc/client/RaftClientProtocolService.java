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

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.apache.raft.client.ClientProtoUtils;
import org.apache.raft.grpc.RaftGrpcUtil;
import org.apache.raft.grpc.proto.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceImplBase;
import org.apache.raft.proto.RaftProtos.RaftClientReplyProto;
import org.apache.raft.proto.RaftProtos.RaftClientRequestProto;
import org.apache.raft.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.server.RequestDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RaftClientProtocolService extends RaftClientProtocolServiceImplBase {
  static final Logger LOG = LoggerFactory.getLogger(RaftClientProtocolService.class);

  private static class PendingAppend implements Comparable<PendingAppend> {
    private final long seqNum;
    private volatile RaftClientReply reply;

    PendingAppend(long seqNum) {
      this.seqNum = seqNum;
    }

    boolean isReady() {
      return reply != null || this == COMPLETED;
    }

    void setReply(RaftClientReply reply) {
      this.reply = reply;
    }

    @Override
    public int compareTo(PendingAppend p) {
      return seqNum == p.seqNum ? 0 : (seqNum < p.seqNum ? -1 : 1);
    }

    @Override
    public String toString() {
      return seqNum + ", reply:" + (reply == null ? "null" : reply.toString());
    }
  }
  private static final PendingAppend COMPLETED = new PendingAppend(Long.MAX_VALUE);

  private final RequestDispatcher dispatcher;

  public RaftClientProtocolService(RequestDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public void setConfiguration(SetConfigurationRequestProto request,
      StreamObserver<RaftClientReplyProto> responseObserver) {
    try {
      CompletableFuture<RaftClientReply> future = dispatcher.setConfiguration(
          ClientProtoUtils.toSetConfigurationRequest(request));
      future.whenCompleteAsync((reply, exception) -> {
        if (exception != null) {
          responseObserver.onError(RaftGrpcUtil.wrapException(exception));
        } else {
          responseObserver.onNext(ClientProtoUtils.toRaftClientReplyProto(reply));
          responseObserver.onCompleted();
        }
      });
    } catch (Exception e) {
      responseObserver.onError(RaftGrpcUtil.wrapException(e));
    }
  }

  @Override
  public StreamObserver<RaftClientRequestProto> append(
      StreamObserver<RaftClientReplyProto> responseObserver) {
    return new AppendRequestStreamObserver(responseObserver);
  }

  private class AppendRequestStreamObserver implements
      StreamObserver<RaftClientRequestProto> {
    private final List<PendingAppend> pendingList = new LinkedList<>();
    private final StreamObserver<RaftClientReplyProto> responseObserver;

    AppendRequestStreamObserver(StreamObserver<RaftClientReplyProto> ro) {
      this.responseObserver = ro;
    }

    @Override
    public void onNext(RaftClientRequestProto request) {
      try {
        PendingAppend p = new PendingAppend(request.getRpcRequest().getSeqNum());
        synchronized (pendingList) {
          pendingList.add(p);
        }

        CompletableFuture<RaftClientReply> future = dispatcher
            .handleClientRequest(ClientProtoUtils.toRaftClientRequest(request));
        future.whenCompleteAsync((reply, exception) -> {
          if (exception != null) {
            // TODO: the exception may be from either raft or state machine.
            // Currently we skip all the following responses when getting an
            // exception from the state machine.
            responseObserver.onError(RaftGrpcUtil.wrapException(exception));
          } else {
            final long replySeq = reply.getSeqNum();
            synchronized (pendingList) {
              Preconditions.checkState(!pendingList.isEmpty(),
                  "PendingList is empty when handling onNext for seqNum %s",
                  replySeq);
              final long headSeqNum = pendingList.get(0).seqNum;
              // we assume the seqNum is consecutive for a stream RPC call
              final PendingAppend pendingForReply = pendingList.get(
                  (int) (replySeq - headSeqNum));
              Preconditions.checkState(pendingForReply != null &&
                      pendingForReply.seqNum == replySeq,
                  "pending for reply is: %s, the pending list: %s",
                  pendingForReply, pendingList);
              pendingForReply.setReply(reply);

              if (headSeqNum == replySeq) {
                Collection<PendingAppend> readySet = new ArrayList<>();
                // if this is head, we send back all the ready responses
                Iterator<PendingAppend> iter = pendingList.iterator();
                PendingAppend pending;
                while (iter.hasNext() && ((pending = iter.next()).isReady())) {
                  readySet.add(pending);
                  iter.remove();
                }
                sendReadyReplies(readySet);
              }
            }
          }
        });
      } catch (Throwable e) {
        LOG.info("{} got exception when handling client append request {}: {}",
            dispatcher.getRaftServer().getId(), request.getRpcRequest(), e);
        responseObserver.onError(RaftGrpcUtil.wrapException(e));
      }
    }

    private void sendReadyReplies(Collection<PendingAppend> readySet) {
      readySet.forEach(ready -> {
        Preconditions.checkState(ready.isReady());
        if (ready == COMPLETED) {
          responseObserver.onCompleted();
        } else {
          responseObserver.onNext(
              ClientProtoUtils.toRaftClientReplyProto(ready.reply));
        }
      });
    }

    @Override
    public void onError(Throwable t) {
      // for now we just log a msg
      LOG.warn("{} onError: client Append cancelled",
          dispatcher.getRaftServer().getId(), t);
      synchronized (pendingList) {
        pendingList.clear();
      }
    }

    @Override
    public void onCompleted() {
      synchronized (pendingList) {
        if (pendingList.isEmpty()) {
          responseObserver.onCompleted();
        } else {
          pendingList.add(COMPLETED);
        }
      }
    }
  }
}
