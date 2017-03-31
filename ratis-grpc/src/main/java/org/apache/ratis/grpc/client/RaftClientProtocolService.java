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

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.shaded.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceImplBase;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.grpc.RaftGrpcUtil;
import org.apache.ratis.protocol.RaftClientAsynchronousProtocol;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class RaftClientProtocolService extends RaftClientProtocolServiceImplBase {
  static final Logger LOG = LoggerFactory.getLogger(RaftClientProtocolService.class);

  private static class PendingAppend implements Comparable<PendingAppend> {
    private final long callId;
    private volatile RaftClientReply reply;

    PendingAppend(long callId) {
      this.callId = callId;
    }

    boolean isReady() {
      return reply != null || this == COMPLETED;
    }

    void setReply(RaftClientReply reply) {
      this.reply = reply;
    }

    @Override
    public int compareTo(PendingAppend p) {
      return callId == p.callId ? 0 : (callId < p.callId ? -1 : 1);
    }

    @Override
    public String toString() {
      return callId + ", reply:" + (reply == null ? "null" : reply.toString());
    }
  }
  private static final PendingAppend COMPLETED = new PendingAppend(Long.MAX_VALUE);

  private final RaftPeerId id;
  private final RaftClientAsynchronousProtocol protocol;

  public RaftClientProtocolService(RaftPeerId id, RaftClientAsynchronousProtocol protocol) {
    this.id = id;
    this.protocol = protocol;
  }

  @Override
  public void setConfiguration(SetConfigurationRequestProto request,
      StreamObserver<RaftClientReplyProto> responseObserver) {
    try {
      CompletableFuture<RaftClientReply> future = protocol.setConfigurationAsync(
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
        PendingAppend p = new PendingAppend(request.getRpcRequest().getCallId());
        synchronized (pendingList) {
          pendingList.add(p);
        }

        CompletableFuture<RaftClientReply> future = protocol.submitClientRequestAsync(
            ClientProtoUtils.toRaftClientRequest(request));
        future.whenCompleteAsync((reply, exception) -> {
          if (exception != null) {
            // TODO: the exception may be from either raft or state machine.
            // Currently we skip all the following responses when getting an
            // exception from the state machine.
            responseObserver.onError(RaftGrpcUtil.wrapException(exception));
          } else {
            final long replySeq = reply.getCallId();
            synchronized (pendingList) {
              Preconditions.assertTrue(!pendingList.isEmpty(),
                  "PendingList is empty when handling onNext for callId %s",
                  replySeq);
              final long headSeqNum = pendingList.get(0).callId;
              // we assume the callId is consecutive for a stream RPC call
              final PendingAppend pendingForReply = pendingList.get(
                  (int) (replySeq - headSeqNum));
              Preconditions.assertTrue(pendingForReply != null &&
                      pendingForReply.callId == replySeq,
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
            id, request.getRpcRequest(), e);
        responseObserver.onError(RaftGrpcUtil.wrapException(e));
      }
    }

    private void sendReadyReplies(Collection<PendingAppend> readySet) {
      readySet.forEach(ready -> {
        Preconditions.assertTrue(ready.isReady());
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
      LOG.warn("{} onError: client Append cancelled", id, t);
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
