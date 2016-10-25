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
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos.RaftClientReplyProto;
import org.apache.raft.proto.RaftProtos.RaftClientRequestProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import static org.apache.raft.grpc.RaftGrpcConfigKeys.RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_DEFAULT;
import static org.apache.raft.grpc.RaftGrpcConfigKeys.RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_KEY;

class AppendStreamer implements Closeable {
  private final RaftClientProtocolClient proxy;
  private volatile StreamObserver<RaftClientRequestProto> requestObserver;
  private final Queue<RaftClientRequestProto> pendingRequests;
  private final int maxPendingNum;
  private volatile boolean running = true;

  AppendStreamer(RaftClientProtocolClient proxy, RaftProperties prop) {
    this.proxy = proxy;
    this.maxPendingNum = prop.getInt(
        RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_KEY,
        RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_DEFAULT);
    this.pendingRequests = new LinkedList<>();
    requestObserver = proxy.append(new ResponseHandler());
  }

  private void checkState() throws IOException {
    if (!running) {
      throw new IOException("The AppendStreamer has been closed");
    }
  }

  synchronized void write(RaftClientRequestProto request)
      throws IOException {
    checkState();
    Preconditions.checkArgument(!request.getReadOnly());
    while (pendingRequests.size() >= maxPendingNum) {
      try {
        wait();
      } catch (InterruptedException ignored) {
      }
    }
    requestObserver.onNext(request);
    pendingRequests.offer(request);
  }

  synchronized void flush() throws IOException {
    checkState();
    if (pendingRequests.isEmpty()) {
      return;
    }
    // wait for the pending Q to become empty
    while (!pendingRequests.isEmpty()) {
      try {
        wait();
      } catch (InterruptedException ignored) {
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (!running) {
      return;
    }
    requestObserver.onCompleted();

    flush();
    running = false;
  }

  /** the response handler for stream RPC */
  private class ResponseHandler implements StreamObserver<RaftClientReplyProto> {
    @Override
    public void onNext(RaftClientReplyProto reply) {
      synchronized (this) {
        RaftClientRequestProto pending = Preconditions.checkNotNull(
            pendingRequests.peek());
        Preconditions.checkState(pending.getRpcRequest().getSeqNum() ==
            reply.getRpcReply().getSeqNum());
        if (reply.getRpcReply().getSuccess()) {
          pendingRequests.poll();
        }
        // TODO if not success
        AppendStreamer.this.notify();
      }
    }

    @Override
    public void onError(Throwable t) {
      // TODO retry on NotLeaderException/IOException
    }

    @Override
    public void onCompleted() {
      Preconditions.checkState(pendingRequests.isEmpty());
      // TODO set Exception for close
    }
  }
}
