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
package org.apache.ratis.examples.filestore;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos;
import org.apache.ratis.proto.ExamplesProtos.DeleteReplyProto;
import org.apache.ratis.proto.ExamplesProtos.DeleteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.FileStoreRequestProto;
import org.apache.ratis.proto.ExamplesProtos.ReadRequestProto;
import org.apache.ratis.proto.ExamplesProtos.StreamWriteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.WriteRequestHeaderProto;
import org.apache.ratis.proto.ExamplesProtos.WriteRequestProto;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.ReferenceCountedObject;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class FileStoreStateMachine extends BaseStateMachine {
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  private final FileStore files;

  public FileStoreStateMachine(RaftProperties properties) {
    this.files = new FileStore(this::getId, properties);
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage)
      throws IOException {
    super.initialize(server, groupId, raftStorage);
    this.storage.init(raftStorage);
    for (Path path : files.getRoots()) {
      FileUtils.createDirectories(path);
    }
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void close() {
    files.close();
    setLastAppliedTermIndex(null);
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    final ReadRequestProto proto;
    try {
      proto = ReadRequestProto.parseFrom(request.getContent());
    } catch (InvalidProtocolBufferException e) {
      return FileStoreCommon.completeExceptionally("Failed to parse " + request, e);
    }

    final String path = proto.getPath().toStringUtf8();
    return (proto.getIsWatch()? files.watch(path)
        : files.read(path, proto.getOffset(), proto.getLength(), true))
        .thenApply(reply -> Message.valueOf(reply.toByteString()));
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
    final ByteString content = request.getMessage().getContent();
    final FileStoreRequestProto proto = FileStoreRequestProto.parseFrom(content);
    final TransactionContext.Builder b = TransactionContext.newBuilder()
        .setStateMachine(this)
        .setClientRequest(request);
    if (proto.getRequestCase() == FileStoreRequestProto.RequestCase.WRITE) {
      final WriteRequestProto write = proto.getWrite();
      final FileStoreRequestProto newProto = FileStoreRequestProto.newBuilder()
          .setWriteHeader(write.getHeader()).build();
      b.setLogData(newProto.toByteString()).setStateMachineData(write.getData())
       .setStateMachineContext(newProto);
    } else {
      b.setLogData(content)
       .setStateMachineContext(proto);
    }
    return b.build();
  }

  @Override
  public TransactionContext startTransaction(LogEntryProto entry, RaftProtos.RaftPeerRole role) {
    return TransactionContext.newBuilder()
        .setStateMachine(this)
        .setLogEntry(entry)
        .setServerRole(role)
        .setStateMachineContext(getProto(entry))
        .build();
  }

  @Override
  public CompletableFuture<Integer> write(ReferenceCountedObject<LogEntryProto> entryRef, TransactionContext context) {
    LogEntryProto entry = entryRef.retain();
    final FileStoreRequestProto proto = getProto(context, entry);
    if (proto.getRequestCase() != FileStoreRequestProto.RequestCase.WRITEHEADER) {
      return null;
    }

    final WriteRequestHeaderProto h = proto.getWriteHeader();
    final CompletableFuture<Integer> f = files.write(entry.getIndex(),
        h.getPath().toStringUtf8(), h.getClose(),  h.getSync(), h.getOffset(),
        entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData()
    ).whenComplete((r, e) -> entryRef.release());
    // sync only if closing the file
    return h.getClose() ? f: null;
  }

  static FileStoreRequestProto getProto(TransactionContext context, LogEntryProto entry) {
    if (context != null) {
      final FileStoreRequestProto proto = (FileStoreRequestProto) context.getStateMachineContext();
      if (proto != null) {
        return proto;
      }
    }
    return getProto(entry);
  }

  static FileStoreRequestProto getProto(LogEntryProto entry) {
    try {
      return FileStoreRequestProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Failed to parse data, entry=" + entry, e);
    }
  }

  @Override
  public CompletableFuture<ByteString> read(LogEntryProto entry, TransactionContext context) {
    final FileStoreRequestProto proto = getProto(context, entry);
    if (proto.getRequestCase() != FileStoreRequestProto.RequestCase.WRITEHEADER) {
      return null;
    }

    final WriteRequestHeaderProto h = proto.getWriteHeader();
    CompletableFuture<ExamplesProtos.ReadReplyProto> reply =
        files.read(h.getPath().toStringUtf8(), h.getOffset(), h.getLength(), false);

    return reply.thenApply(ExamplesProtos.ReadReplyProto::getData);
  }

  static class LocalStream implements DataStream {
    private final String name;
    private final DataChannel dataChannel;

    LocalStream(String name, DataChannel dataChannel) {
      this.name = JavaUtils.getClassSimpleName(getClass()) + "[" + name + "]";
      this.dataChannel = dataChannel;
    }

    @Override
    public DataChannel getDataChannel() {
      return dataChannel;
    }

    @Override
    public CompletableFuture<?> cleanUp() {
      return CompletableFuture.supplyAsync(() -> {
        try {
          dataChannel.close();
          return true;
        } catch (IOException e) {
          return FileStoreCommon.completeExceptionally("Failed to close data channel", e);
        }
      });
    }

    @Override
    public String toString() {
      return name;
    }
  }

  @Override
  public CompletableFuture<DataStream> stream(RaftClientRequest request) {
    final ByteString reqByteString = request.getMessage().getContent();
    final FileStoreRequestProto proto;
    try {
      proto = FileStoreRequestProto.parseFrom(reqByteString);
    } catch (InvalidProtocolBufferException e) {
      return FileStoreCommon.completeExceptionally(
          "Failed to parse stream header", e);
    }
    final String file = proto.getStream().getPath().toStringUtf8();
    return files.createDataChannel(file)
        .thenApply(channel -> new LocalStream(file, channel));
  }

  @Override
  public CompletableFuture<?> link(DataStream stream, LogEntryProto entry) {
    LOG.info("linking {} to {}", stream, LogProtoUtils.toLogEntryString(entry));
    return files.streamLink(stream);
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry();

    final long index = entry.getIndex();
    updateLastAppliedTermIndex(entry.getTerm(), index);

    final FileStoreRequestProto request = getProto(trx, entry);

    switch(request.getRequestCase()) {
      case DELETE:
        return delete(index, request.getDelete());
      case WRITEHEADER:
        return writeCommit(index, request.getWriteHeader(),
            entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData().size());
      case STREAM:
        return streamCommit(request.getStream());
      case WRITE:
        // WRITE should not happen here since
        // startTransaction converts WRITE requests to WRITEHEADER requests.
      default:
        LOG.error(getId() + ": Unexpected request case " + request.getRequestCase());
        return FileStoreCommon.completeExceptionally(index,
            "Unexpected request case " + request.getRequestCase());
    }
  }

  private CompletableFuture<Message> writeCommit(
      long index, WriteRequestHeaderProto header, int size) {
    final String path = header.getPath().toStringUtf8();
    return files.submitCommit(index, path, header.getClose(), header.getOffset(), size)
        .thenApply(reply -> Message.valueOf(reply.toByteString()));
  }

  private CompletableFuture<Message> streamCommit(StreamWriteRequestProto stream) {
    final String path = stream.getPath().toStringUtf8();
    final long size = stream.getLength();
    return files.streamCommit(path, size).thenApply(reply -> Message.valueOf(reply.toByteString()));
  }

  private CompletableFuture<Message> delete(long index, DeleteRequestProto request) {
    final String path = request.getPath().toStringUtf8();
    return files.delete(index, path).thenApply(resolved ->
        Message.valueOf(DeleteReplyProto.newBuilder().setResolvedPath(
            FileStoreCommon.toByteString(resolved)).build().toByteString(),
            () -> "Message:" + resolved));
  }
}
