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
import org.apache.ratis.datastream.impl.DataStreamPacketByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestFilePositionCount;
import org.apache.ratis.io.FilePositionCount;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SlidingWindow;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.LongFunction;

public class OrderedStreamAsync {
  public static final Logger LOG = LoggerFactory.getLogger(OrderedStreamAsync.class);

  static class DataStreamWindowRequest implements SlidingWindow.ClientSideRequest<DataStreamReply> {
    private final DataStreamRequestHeader header;
    private final Object data;
    private final long seqNum;
    private final CompletableFuture<DataStreamReply> replyFuture = new CompletableFuture<>();

    DataStreamWindowRequest(DataStreamRequestHeader header, Object data, long seqNum) {
      this.header = header;
      this.data = data;
      this.seqNum = seqNum;
    }

    DataStreamRequest getDataStreamRequest() {
      if (header.getDataLength() == 0) {
        return new DataStreamRequestByteBuffer(header, DataStreamPacketByteBuffer.EMPTY_BYTE_BUFFER);
      } else if (data instanceof ByteBuffer) {
        return new DataStreamRequestByteBuffer(header, (ByteBuffer)data);
      } else if (data instanceof FilePositionCount) {
        return new DataStreamRequestFilePositionCount(header, (FilePositionCount)data);
      }
      throw new IllegalStateException("Unexpected " + data.getClass());
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

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":seqNum=" + seqNum + "," + header;
    }
  }

  private final DataStreamClientRpc dataStreamClientRpc;

  private final Semaphore requestSemaphore;
  private final TimeDuration requestTimeout;
  private final TimeoutScheduler scheduler = TimeoutScheduler.getInstance();
  private final Executor sendExecutor;

  OrderedStreamAsync(DataStreamClientRpc dataStreamClientRpc, RaftProperties properties){
    this.dataStreamClientRpc = dataStreamClientRpc;
    this.requestSemaphore = new Semaphore(RaftClientConfigKeys.DataStream.outstandingRequestsMax(properties));
    this.requestTimeout = RaftClientConfigKeys.DataStream.requestTimeout(properties);

    this.sendExecutor = ConcurrentUtils.newThreadPoolWithMax(false,
        RaftClientConfigKeys.DataStream.sendRequestWorker(properties), "stream-async-sender-");
  }

  CompletableFuture<DataStreamReply> sendRequest(DataStreamRequestHeader header, Object data,
      SlidingWindow.Client<DataStreamWindowRequest, DataStreamReply> slidingWindow) {
    try {
      requestSemaphore.acquire();
    } catch (InterruptedException e){
      return JavaUtils.completeExceptionally(IOUtils.toInterruptedIOException(
          "Interrupted when sending " + JavaUtils.getClassSimpleName(data.getClass()) + ", header= " + header, e));
    }
    final LongFunction<DataStreamWindowRequest> constructor
        = seqNum -> new DataStreamWindowRequest(header, data, seqNum);
    return slidingWindow.submitNewRequest(constructor, r -> sendRequestToNetwork(r, slidingWindow)).
           getReplyFuture().whenComplete((r, e) -> {
             if (e != null) {
               LOG.error("Failed to send request, header=" + header, e);
             }
             requestSemaphore.release();
           });
  }

  private void sendRequestToNetwork(DataStreamWindowRequest request,
      SlidingWindow.Client<DataStreamWindowRequest, DataStreamReply> slidingWindow) {
    CompletableFuture<DataStreamReply> f = request.getReplyFuture();
    if(f.isDone()) {
      return;
    }
    if(slidingWindow.isFirst(request.getSeqNum())){
      request.setFirstRequest();
    }
    final CompletableFuture<DataStreamReply> requestFuture = dataStreamClientRpc.streamAsync(
        request.getDataStreamRequest());
    long seqNum = request.getSeqNum();

    scheduleWithTimeout(request);

    requestFuture.thenApplyAsync(reply -> {
      slidingWindow.receiveReply(
          seqNum, reply, r -> sendRequestToNetwork(r, slidingWindow));
      return reply;
    }, sendExecutor).thenAcceptAsync(reply -> {
      if (f.isDone()) {
        return;
      }
      f.complete(reply);
    }, sendExecutor).exceptionally(e -> {
      f.completeExceptionally(e);
      return null;
    });
  }

  private void scheduleWithTimeout(DataStreamWindowRequest request) {
    scheduler.onTimeout(requestTimeout, () -> {
      if (!request.getReplyFuture().isDone()) {
        request.getReplyFuture().completeExceptionally(
            new TimeoutIOException("Timeout " + requestTimeout + ": Failed to send " + request));
      }
    }, LOG, () -> "Failed to completeExceptionally for " + request);
  }
}
