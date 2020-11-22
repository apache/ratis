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
package org.apache.ratis.datastream;

import org.apache.ratis.BaseTest;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientMessage;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.statemachine.StateMachine.StateMachineDataChannel;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

public interface DataStreamTestUtils {
  Logger LOG = LoggerFactory.getLogger(DataStreamTestUtils.class);

  ByteString MOCK = ByteString.copyFromUtf8("mock");
  int MODULUS = 23;

  static byte pos2byte(int pos) {
    return (byte) ('A' + pos % MODULUS);
  }

  static ByteString bytesWritten2ByteString(long bytesWritten) {
    return ByteString.copyFromUtf8("bytesWritten=" + bytesWritten);
  }

  class MultiDataStreamStateMachine extends BaseStateMachine {
    private final ConcurrentMap<ClientInvocationId, SingleDataStream> streams = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<DataStream> stream(RaftClientRequest request) {
      final SingleDataStream s = new SingleDataStream(request);
      streams.put(ClientInvocationId.valueOf(request), s);
      return CompletableFuture.completedFuture(s);
    }

    @Override
    public CompletableFuture<?> link(DataStream stream, LogEntryProto entry) {
      final SingleDataStream s = getSingleDataStream(ClientInvocationId.valueOf(entry.getStateMachineLogEntry()));
      s.setLogEntry(entry);
      return CompletableFuture.completedFuture(null);
    }

    SingleDataStream getSingleDataStream(RaftClientRequest request) {
      return getSingleDataStream(ClientInvocationId.valueOf(request));
    }

    SingleDataStream getSingleDataStream(ClientInvocationId invocationId) {
      return streams.get(invocationId);
    }
  }

  class SingleDataStream implements DataStream {
    private final RaftClientRequest writeRequest;
    private final DataChannel channel = new DataChannel();
    private volatile LogEntryProto logEntry;

    SingleDataStream(RaftClientRequest request) {
      this.writeRequest = request;
    }

    @Override
    public DataChannel getWritableByteChannel() {
      return channel;
    }

    @Override
    public CompletableFuture<?> cleanUp() {
      try {
        channel.close();
      } catch (Throwable t) {
        return JavaUtils.completeExceptionally(t);
      }
      return CompletableFuture.completedFuture(null);
    }

    void setLogEntry(LogEntryProto logEntry) {
      this.logEntry = logEntry;
    }

    LogEntryProto getLogEntry() {
      return logEntry;
    }

    RaftClientRequest getWriteRequest() {
      return writeRequest;
    }
  }

  class DataChannel implements StateMachineDataChannel {
    private volatile boolean open = true;
    private int bytesWritten = 0;
    private int forcedPosition = 0;

    int getBytesWritten() {
      return bytesWritten;
    }

    int getForcedPosition() {
      return forcedPosition;
    }

    @Override
    public void force(boolean metadata) {
      forcedPosition = bytesWritten;
    }

    @Override
    public int write(ByteBuffer src) {
      if (!open) {
        throw new IllegalStateException("Already closed");
      }
      final int remaining = src.remaining();
      for (; src.remaining() > 0; ) {
        Assert.assertEquals(pos2byte(bytesWritten), src.get());
        bytesWritten += 1;
      }
      return remaining;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;
    }
  }

  static int writeAndAssertReplies(DataStreamOutputImpl out, int bufferSize, int bufferNum) {
    final List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
    final List<Integer> sizes = new ArrayList<>();

    //send data
    final int halfBufferSize = bufferSize / 2;
    int dataSize = 0;
    for (int i = 0; i < bufferNum; i++) {
      final int size = halfBufferSize + ThreadLocalRandom.current().nextInt(halfBufferSize);
      sizes.add(size);

      final ByteBuffer bf = initBuffer(dataSize, size);
      futures.add(out.writeAsync(bf, i == bufferNum - 1));
      dataSize += size;
    }

    { // check header
      final DataStreamReply reply = out.getHeaderFuture().join();
      Assert.assertTrue(reply.isSuccess());
      Assert.assertEquals(0, reply.getBytesWritten());
      Assert.assertEquals(reply.getType(), Type.STREAM_HEADER);
    }

    // check writeAsync requests
    for (int i = 0; i < futures.size(); i++) {
      final DataStreamReply reply = futures.get(i).join();
      Assert.assertTrue(reply.isSuccess());
      Assert.assertEquals(sizes.get(i).longValue(), reply.getBytesWritten());
      Assert.assertEquals(reply.getType(), i == futures.size() - 1 ? Type.STREAM_DATA_SYNC : Type.STREAM_DATA);
    }
    return dataSize;
  }

  static CompletableFuture<RaftClientReply> writeAndCloseAndAssertReplies(
      Iterable<RaftServer> servers, DataStreamOutputImpl out, int bufferSize, int bufferNum) {
    LOG.info("start Stream{}", out.getHeader().getCallId());
    final int bytesWritten = writeAndAssertReplies(out, bufferSize, bufferNum);
    try {
      for (RaftServer s : servers) {
        assertHeader(s, out.getHeader(), bytesWritten);
      }
    } catch (Throwable e) {
      throw new CompletionException(e);
    }

    return out.closeAsync().thenCompose(reply -> assertCloseReply(out, reply, bytesWritten));
  }

  static void assertHeader(RaftServer server, RaftClientRequest header, int dataSize) throws Exception {
    // check header
    Assert.assertEquals(RaftClientRequest.dataStreamRequestType(), header.getType());

    // check stream
    final MultiDataStreamStateMachine stateMachine = (MultiDataStreamStateMachine) server.getDivision(header.getRaftGroupId()).getStateMachine();
    final SingleDataStream stream = stateMachine.getSingleDataStream(header);
    final DataChannel channel = stream.getWritableByteChannel();
    Assert.assertEquals(dataSize, channel.getBytesWritten());
    Assert.assertEquals(dataSize, channel.getForcedPosition());

    // check writeRequest
    final RaftClientRequest writeRequest = stream.getWriteRequest();
    Assert.assertEquals(RaftClientRequest.dataStreamRequestType(), writeRequest.getType());
    assertRaftClientMessage(header, writeRequest);
  }

  static CompletableFuture<RaftClientReply> assertCloseReply(DataStreamOutputImpl out, DataStreamReply dataStreamReply,
      long bytesWritten) {
    // Test close idempotent
    Assert.assertSame(dataStreamReply, out.closeAsync().join());
    BaseTest.testFailureCase("writeAsync should fail",
        () -> out.writeAsync(DataStreamRequestByteBuffer.EMPTY_BYTE_BUFFER).join(),
        CompletionException.class, (Logger) null, AlreadyClosedException.class);

    try {
      final RaftClientReply reply = ClientProtoUtils.toRaftClientReply(RaftClientReplyProto.parseFrom(
          ((DataStreamReplyByteBuffer) dataStreamReply).slice()));
      assertRaftClientMessage(out.getHeader(), reply);
      if (reply.isSuccess()) {
        final ByteString bytes = reply.getMessage().getContent();
        if (!bytes.equals(MOCK)) {
          Assert.assertEquals(bytesWritten2ByteString(bytesWritten), bytes);
        }
      }

      return CompletableFuture.completedFuture(reply);
    } catch (Throwable t) {
      return JavaUtils.completeExceptionally(t);
    }
  }

  static void assertRaftClientMessage(RaftClientMessage expected, RaftClientMessage computed) {
    Assert.assertNotNull(computed);
    Assert.assertEquals(expected.getClientId(), computed.getClientId());
    Assert.assertEquals(expected.getServerId(), computed.getServerId());
    Assert.assertEquals(expected.getRaftGroupId(), computed.getRaftGroupId());
    Assert.assertEquals(expected.getCallId(), computed.getCallId());
  }

  static ByteBuffer initBuffer(int offset, int size) {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(size);
    final int length = buffer.capacity();
    buffer.position(0).limit(length);
    for (int j = 0; j < length; j++) {
      buffer.put(pos2byte(offset + j));
    }
    buffer.flip();
    Assert.assertEquals(length, buffer.remaining());
    return buffer;
  }
}
