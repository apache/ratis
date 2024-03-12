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
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientMessage;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine.DataChannel;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ratis.server.storage.RaftStorageTestUtils.getLogUnsafe;

public interface DataStreamTestUtils {
  Logger LOG = LoggerFactory.getLogger(DataStreamTestUtils.class);

  ByteString MOCK = ByteString.copyFromUtf8("mock");
  int MODULUS = 23;

  static byte pos2byte(int pos) {
    return (byte) ('A' + pos % MODULUS);
  }

  static ByteBuffer initBuffer(int offset, int size) {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(size);
    final int length = buffer.capacity();
    buffer.position(0).limit(length);
    for (int j = 0; j < length; j++) {
      buffer.put(pos2byte(offset + j));
    }
    buffer.flip();
    Assertions.assertEquals(length, buffer.remaining());
    return buffer;
  }

  static void createFile(File f, int size) throws Exception {
    final ReadableByteChannel source = new ReadableByteChannel() {
      private int offset = 0;

      @Override
      public boolean isOpen() {
        return offset < size;
      }

      @Override
      public void close() {
        offset = size;
      }

      @Override
      public int read(ByteBuffer dst) {
        final int start = offset;
        for(; dst.remaining() > 0 && isOpen(); offset++) {
          dst.put(pos2byte(offset));
        }
        return offset - start;
      }
    };
    FileUtils.createDirectories(f.getParentFile());
    try(FileChannel out = FileUtils.newFileChannel(f, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      final long transferred = out.transferFrom(source, 0, size);
      Assertions.assertEquals(size, transferred);
    }
  }

  static ByteString bytesWritten2ByteString(long bytesWritten) {
    return ByteString.copyFromUtf8("bytesWritten=" + bytesWritten);
  }

  static RoutingTable getRoutingTableChainTopology(Iterable<RaftPeer> peers, RaftPeer primary) {
    return getRoutingTableChainTopology(CollectionUtils.as(peers, RaftPeer::getId), primary.getId());
  }

  static RoutingTable getRoutingTableChainTopology(Iterable<RaftPeerId> peers, RaftPeerId primary) {
    final RoutingTable.Builder builder = RoutingTable.newBuilder();
    RaftPeerId previous = primary;
    for (RaftPeerId peer : peers) {
      if (peer.equals(primary)) {
        continue;
      }
      builder.addSuccessor(previous, peer);
      previous = peer;
    }

    return builder.build();
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
      LOG.info("link {}", stream);
      if (stream == null) {
        return JavaUtils.completeExceptionally(new IllegalStateException("Null stream: entry=" + entry));
      }
      ((SingleDataStream)stream).setLogEntry(entry);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      final LogEntryProto entry = Objects.requireNonNull(trx.getLogEntryUnsafe());
      updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      final SingleDataStream s = getSingleDataStream(ClientInvocationId.valueOf(entry.getStateMachineLogEntry()));
      final ByteString bytesWritten = bytesWritten2ByteString(s.getDataChannel().getBytesWritten());
      return CompletableFuture.completedFuture(() -> bytesWritten);
    }

    SingleDataStream getSingleDataStream(RaftClientRequest request) {
      return getSingleDataStream(ClientInvocationId.valueOf(request));
    }

    SingleDataStream getSingleDataStream(ClientInvocationId invocationId) {
      return streams.get(invocationId);
    }

    Collection<SingleDataStream> getStreams() {
      return streams.values();
    }
  }

  class SingleDataStream implements DataStream {
    private final RaftClientRequest writeRequest;
    private final MyDataChannel channel = new MyDataChannel();
    private volatile LogEntryProto logEntry;

    SingleDataStream(RaftClientRequest request) {
      this.writeRequest = request;
    }

    @Override
    public MyDataChannel getDataChannel() {
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

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ": writeRequest=" + writeRequest
          + ", logEntry=" + LogProtoUtils.toLogEntryString(logEntry);
    }
  }

  class MyDataChannel implements DataChannel {
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
        Assertions.assertEquals(pos2byte(bytesWritten), src.get());
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
      futures.add(i == bufferNum - 1 ? out.writeAsync(bf, StandardWriteOption.FLUSH, StandardWriteOption.SYNC)
          : out.writeAsync(bf));
      dataSize += size;
    }

    { // check header
      final DataStreamReply reply = out.getHeaderFuture().join();
      assertSuccessReply(Type.STREAM_HEADER, 0, reply);
    }

    // check writeAsync requests
    for (int i = 0; i < futures.size(); i++) {
      final DataStreamReply reply = futures.get(i).join();
      final Type expectedType = Type.STREAM_DATA;
      assertSuccessReply(expectedType, sizes.get(i).longValue(), reply);
    }
    return dataSize;
  }

  static void assertSuccessReply(Type expectedType, long expectedBytesWritten, DataStreamReply reply) {
    Assertions.assertTrue(reply.isSuccess());
    Assertions.assertEquals(expectedBytesWritten, reply.getBytesWritten());
    Assertions.assertEquals(expectedType, reply.getType());
  }

  static CompletableFuture<RaftClientReply> writeAndCloseAndAssertReplies(
      Iterable<RaftServer> servers, RaftPeerId leader, DataStreamOutputImpl out, int bufferSize, int bufferNum,
      ClientId clientId, boolean stepDownLeader) {
    LOG.info("start Stream{}", out.getHeader().getCallId());
    final int bytesWritten = writeAndAssertReplies(out, bufferSize, bufferNum);
    try {
      for (RaftServer s : servers) {
        assertHeader(s, out.getHeader(), bytesWritten, stepDownLeader);
      }
    } catch (Throwable e) {
      throw new CompletionException(e);
    }
    LOG.info("Stream{}: bytesWritten={}", out.getHeader().getCallId(), bytesWritten);

    return out.closeAsync().thenCompose(
        reply -> assertCloseReply(out, reply, bytesWritten, leader, clientId, stepDownLeader));
  }

  static void assertHeader(RaftServer server, RaftClientRequest header, int dataSize, boolean stepDownLeader)
      throws Exception {
    // check header
    Assertions.assertEquals(RaftClientRequest.dataStreamRequestType(), header.getType());

    // check stream
    final MultiDataStreamStateMachine stateMachine = (MultiDataStreamStateMachine) server.getDivision(header.getRaftGroupId()).getStateMachine();
    final SingleDataStream stream = stateMachine.getSingleDataStream(header);
    final MyDataChannel channel = stream.getDataChannel();
    Assertions.assertEquals(dataSize, channel.getBytesWritten());
    Assertions.assertEquals(dataSize, channel.getForcedPosition());

    // check writeRequest
    final RaftClientRequest writeRequest = stream.getWriteRequest();
    Assertions.assertEquals(RaftClientRequest.dataStreamRequestType(), writeRequest.getType());
    assertRaftClientMessage(header, null, writeRequest, header.getClientId(), stepDownLeader);
  }

  static CompletableFuture<RaftClientReply> assertCloseReply(DataStreamOutputImpl out, DataStreamReply dataStreamReply,
      long bytesWritten, RaftPeerId leader, ClientId clientId, boolean stepDownLeader) {
    // Test close idempotent
    Assertions.assertSame(dataStreamReply, out.closeAsync().join());
    Assertions.assertEquals(dataStreamReply.getClientId(), clientId);
    BaseTest.testFailureCase("writeAsync should fail",
        () -> out.writeAsync(DataStreamRequestByteBuffer.EMPTY_BYTE_BUFFER).join(),
        CompletionException.class, (Logger) null, AlreadyClosedException.class);

    final DataStreamReplyByteBuffer buffer = (DataStreamReplyByteBuffer) dataStreamReply;
    try {
      final RaftClientReply reply = ClientProtoUtils.toRaftClientReply(buffer.slice());
      assertRaftClientMessage(out.getHeader(), leader, reply, clientId, stepDownLeader);
      if (reply.isSuccess()) {
        final ByteString bytes = reply.getMessage().getContent();
        if (!bytes.equals(MOCK)) {
          Assertions.assertEquals(bytesWritten2ByteString(bytesWritten), bytes);
        }
      }

      return CompletableFuture.completedFuture(reply);
    } catch (Throwable t) {
      return JavaUtils.completeExceptionally(t);
    }
  }

  static void assertRaftClientMessage(
      RaftClientMessage expected, RaftPeerId expectedServerId, RaftClientMessage computed, ClientId expectedClientId,
      boolean stepDownLeader) {
    Assertions.assertNotNull(computed);
    Assertions.assertEquals(expectedClientId, computed.getClientId());
    if (!stepDownLeader) {
      Assertions.assertEquals(
          Optional.ofNullable(expectedServerId).orElseGet(expected::getServerId), computed.getServerId());
    }
    Assertions.assertEquals(expected.getRaftGroupId(), computed.getRaftGroupId());
  }

  static LogEntryProto searchLogEntry(ClientInvocationId invocationId, RaftLog log) throws Exception {
    for (LogEntryHeader termIndex : log.getEntries(0, Long.MAX_VALUE)) {
      final LogEntryProto entry = getLogUnsafe(log, termIndex.getIndex());
      if (entry.hasStateMachineLogEntry()) {
        if (invocationId.match(entry.getStateMachineLogEntry())) {
          return entry;
        }
      }
    }
    return null;
  }

  static void assertLogEntry(LogEntryProto logEntry, RaftClientRequest request) {
    Assertions.assertNotNull(logEntry);
    Assertions.assertTrue(logEntry.hasStateMachineLogEntry());
    final StateMachineLogEntryProto s = logEntry.getStateMachineLogEntry();
    Assertions.assertEquals(StateMachineLogEntryProto.Type.DATASTREAM, s.getType());
    Assertions.assertEquals(request.getCallId(), s.getCallId());
    Assertions.assertEquals(request.getClientId().toByteString(), s.getClientId());
  }

  static void assertLogEntry(RaftServer.Division division, SingleDataStream stream) throws Exception {
    final RaftClientRequest request = stream.getWriteRequest();
    final LogEntryProto entryFromStream = stream.getLogEntry();
    assertLogEntry(entryFromStream, request);

    final LogEntryProto entryFromLog = searchLogEntry(ClientInvocationId.valueOf(request), division.getRaftLog());
    Assertions.assertEquals(entryFromStream, entryFromLog);
  }
}
