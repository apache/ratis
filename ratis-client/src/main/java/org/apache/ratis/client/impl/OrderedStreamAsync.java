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
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.DataStreamRequestClient;
import org.apache.ratis.protocol.RaftClientRequest;
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

  public static class DataStreamWindowRequest implements DataStreamRequest,
      SlidingWindow.ClientSideRequest<DataStreamReply> {
    private final long streamId;
    private final long messageId;
    private final long seqNum;
    private final ByteBuffer data;
    private boolean isFirst = false;
    private CompletableFuture<DataStreamReply> replyFuture = new CompletableFuture<>();

    public DataStreamRequestClient newDataStreamRequest(){
      return new DataStreamRequestClient(streamId, messageId, data.slice());
    }

    @Override
    public long getStreamId() {
      return streamId;
    }

    @Override
    public long getDataOffset() {
      return messageId;
    }

    @Override
    public long getDataLength() {
      return data.capacity();
    }

    @Override
    public void setFirstRequest() {
      isFirst = true;
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

    public DataStreamWindowRequest(long streamId, long messageId,
                                 long seqNum, ByteBuffer data){
      this.streamId = streamId;
      this.messageId = messageId;
      this.data = data.slice();
      this.seqNum = seqNum;
    }
  }

  private SlidingWindow.Client<DataStreamWindowRequest, DataStreamReply> slidingWindow;
  private Semaphore requestSemaphore;
  private DataStreamClientRpc dataStreamClientRpc;

  public OrderedStreamAsync(DataStreamClientRpc dataStreamClientRpc,
                            RaftProperties properties){
    this.dataStreamClientRpc = dataStreamClientRpc;
    this.requestSemaphore = new Semaphore(RaftClientConfigKeys.Async.outstandingRequestsMax(properties)*2);
    this.slidingWindow = new SlidingWindow.Client<>("sliding");
  }

  private void resetSlidingWindow(RaftClientRequest request) {
    slidingWindow.resetFirstSeqNum();
  }

  public CompletableFuture<DataStreamReply> sendRequest(long streamId,
                                                        long messageId,
                                                        ByteBuffer data){
    try {
      requestSemaphore.acquire();
    } catch (InterruptedException e){
      return JavaUtils.completeExceptionally(IOUtils.toInterruptedIOException(
          "Interrupted when sending streamId=" + streamId + ", messageId= " + messageId, e));
    }
    final LongFunction<DataStreamWindowRequest> constructor = seqNum -> new DataStreamWindowRequest(streamId,
                                                                          messageId, seqNum, data);
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
    DataStreamRequestClient rpcRequest = request.newDataStreamRequest();
    CompletableFuture<DataStreamReply> requestFuture = dataStreamClientRpc.streamAsync(rpcRequest);
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
