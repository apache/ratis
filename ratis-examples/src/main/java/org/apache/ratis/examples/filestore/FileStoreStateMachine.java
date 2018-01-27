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
package org.apache.ratis.examples.filestore;

import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.shaded.proto.ExamplesProtos.*;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class FileStoreStateMachine extends BaseStateMachine {
  public static final Logger LOG = LoggerFactory.getLogger(FileStoreStateMachine.class);

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final AtomicReference<TermIndex> latestTermIndex = new AtomicReference<>();

  private final FileStore files;

  public FileStoreStateMachine(RaftProperties properties) {
    final File dir = ConfUtils.getFile(properties::getFile, FileStoreCommon.STATEMACHINE_DIR_KEY, null);
    Objects.requireNonNull(dir, FileStoreCommon.STATEMACHINE_DIR_KEY + " is not set.");
    this.files = new FileStore(this::getId, dir.toPath());
  }

  @Override
  public void initialize(RaftPeerId id, RaftProperties properties, RaftStorage raftStorage)
      throws IOException {
    super.initialize(id, properties, raftStorage);
    this.storage.init(raftStorage);
    FileUtils.createDirectories(files.getRoot());
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void close() {
    files.close();
    latestTermIndex.set(null);
  }

  @Override
  public CompletableFuture<RaftClientReply> query(RaftClientRequest request) {
    final ReadRequestProto proto;
    try {
      proto = ReadRequestProto.parseFrom(request.getMessage().getContent());
    } catch (InvalidProtocolBufferException e) {
      return FileStoreCommon.completeExceptionally("Failed to parse " + request, e);
    }

    final String path = proto.getPath().toStringUtf8();
    return files.read(path, proto.getOffset(), proto.getLength())
        .thenApply(reply -> new RaftClientReply(request, () -> reply.toByteString()));
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
    final ByteString content = request.getMessage().getContent();
    final FileStoreRequestProto proto = FileStoreRequestProto.parseFrom(content);
    final SMLogEntryProto log;
    if (proto.getRequestCase() == FileStoreRequestProto.RequestCase.WRITE) {
      final WriteRequestProto write = proto.getWrite();
      final FileStoreRequestProto newProto = FileStoreRequestProto.newBuilder()
          .setWriteHeader(write.getHeader()).build();
      log = SMLogEntryProto.newBuilder()
          .setData(newProto.toByteString())
          .setStateMachineData(write.getData())
          .build();
    } else {
      log = SMLogEntryProto.newBuilder().setData(content).build();
    }

    return new TransactionContextImpl(this, request, log);
  }

  @Override
  public CompletableFuture<Integer> writeStateMachineData(LogEntryProto entry) {
    final SMLogEntryProto smLog = entry.getSmLogEntry();
    final ByteString data = smLog.getData();
    final FileStoreRequestProto proto;
    try {
      proto = FileStoreRequestProto.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      return FileStoreCommon.completeExceptionally(
          entry.getIndex(), "Failed to parse data, entry=" + entry, e);
    }
    if (proto.getRequestCase() != FileStoreRequestProto.RequestCase.WRITEHEADER) {
      return null;
    }

    final WriteRequestHeaderProto h = proto.getWriteHeader();
    final CompletableFuture<Integer> f = files.write(entry.getIndex(),
        h.getPath().toStringUtf8(), h.getClose(), h.getOffset(), smLog.getStateMachineData());
    // sync only if closing the file
    return h.getClose()? f: null;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry();

    final long index = entry.getIndex();
    updateLatestTermIndex(entry.getTerm(), index);

    final SMLogEntryProto smLog = entry.getSmLogEntry();
    final FileStoreRequestProto request;
    try {
      request = FileStoreRequestProto.parseFrom(smLog.getData());
    } catch (InvalidProtocolBufferException e) {
      return FileStoreCommon.completeExceptionally(index,
          "Failed to parse SmLogEntry", e);
    }

    switch(request.getRequestCase()) {
      case DELETE:
        return delete(index, request.getDelete());
      case WRITEHEADER:
        return writeCommit(index, request.getWriteHeader(), smLog.getStateMachineData().size());
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
        .thenApply(reply -> () -> reply.toByteString());
  }

  private CompletableFuture<Message> delete(long index, DeleteRequestProto request) {
    final String path = request.getPath().toStringUtf8();
    return files.delete(index, path).thenApply(resolved -> () ->
        DeleteReplyProto.newBuilder().setResolvedPath(
            FileStoreCommon.toByteString(resolved)).build().toByteString());
  }

  private void updateLatestTermIndex(long term, long index) {
    final TermIndex newTI = TermIndex.newTermIndex(term, index);
    final TermIndex oldTI = latestTermIndex.getAndSet(newTI);
    if (oldTI != null) {
      Preconditions.assertTrue(newTI.compareTo(oldTI) >= 0);
    }
  }
}
