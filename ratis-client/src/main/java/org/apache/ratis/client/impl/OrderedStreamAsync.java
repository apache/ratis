/*
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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.LongFunction;

public class OrderedStreamAsync {
  public static final Logger LOG = LoggerFactory.getLogger(OrderedStreamAsync.class);

  static class DataStreamWindowRequest extends DataStreamRequestByteBuffer
      implements SlidingWindow.ClientSideRequest<DataStreamReply> {
    private final long seqNum;
    private final CompletableFuture<DataStreamReply> replyFuture = new CompletableFuture<>();

    DataStreamWindowRequest(Type type, long streamId, long offset, ByteBuffer data, long seqNum) {
      super(type, streamId, offset, data);
      this.seqNum = seqNum;
    }

    @Override
    public void setFirstRequest() {
    }

    @Override
    public long getSeqNum() {
      return seqNum;
    }

    @Override
    public void setReply(DataStreamReply dataStreamReply) {
      replyFuture.complete(dataStreamReply);
    }

    @Override
    public boolean hasReply() {
      return replyFuture.isDone();
    }

    @Override
    public void fail(Throwable e) {
      replyFuture.completeExceptionally(e);
    }

    public CompletableFuture<DataStreamReply> getReplyFuture(){
      return replyFuture;
    }
  }

  private final DataStreamClientRpc dataStreamClientRpc;
  private final SlidingWindow.Client<DataStreamWindowRequest, DataStreamReply> slidingWindow;
  private final Semaphore requestSemaphore;

  OrderedStreamAsync(ClientId clientId, DataStreamClientRpc dataStreamClientRpc, RaftProperties properties){
    this.dataStreamClientRpc = dataStreamClientRpc;
    this.slidingWindow = new SlidingWindow.Client<>(clientId);
    this.requestSemaphore = new Semaphore(RaftClientConfigKeys.DataStream.outstandingRequestsMax(properties));
  }

  CompletableFuture<DataStreamReply> sendRequest(long streamId, long offset, ByteBuffer data, Type type){
    final int length = data.remaining();
    try {
      requestSemaphore.acquire();
    } catch (InterruptedException e){
      return JavaUtils.completeExceptionally(IOUtils.toInterruptedIOException(
          "Interrupted when sending streamId=" + streamId + ", offset= " + offset + ", length=" + length, e));
    }
    final LongFunction<DataStreamWindowRequest> constructor
        = seqNum -> new DataStreamWindowRequest(type, streamId, offset, data.slice(), seqNum);
    return slidingWindow.submitNewRequest(constructor, this::sendRequestToNetwork).
           getReplyFuture().whenComplete((r, e) -> requestSemaphore.release());
  }

  private void sendRequestToNetwork(DataStreamWindowRequest request){
    CompletableFuture<DataStreamReply> f = request.getReplyFuture();
    if(f.isDone()) {
      return;
    }
    if(slidingWindow.isFirst(request.getSeqNum())){
      request.setFirstRequest();
    }
    final CompletableFuture<DataStreamReply> requestFuture = dataStreamClientRpc.streamAsync(request);
    requestFuture.thenApply(reply -> {
      slidingWindow.receiveReply(
          request.getSeqNum(), reply, this::sendRequestToNetwork);
      return reply;
    }).thenAccept(reply -> {
      if (f.isDone()) {
        return;
      }
      f.complete(reply);
    });
  }
}
