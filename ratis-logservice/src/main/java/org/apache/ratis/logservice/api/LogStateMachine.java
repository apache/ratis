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
package org.apache.ratis.logservice.api;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.ratis.logservice.impl.LogStreamImpl;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.AppendLogEntryRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ArchiveLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ArchiveLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CloseLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CloseLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CreateLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.DeleteLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.DeleteLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogLengthRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetStateRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogServiceRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ReadLogRequestProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.server.impl.ServerState;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.server.storage.RaftLogIOException;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStateMachine extends BaseStateMachine {
  public static final Logger LOG = LoggerFactory.getLogger(LogStateMachine.class);
  /*
   *  State
   */
  private final Map<LogName, Long> state = new ConcurrentHashMap<>();

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private RaftLog log;

  private RaftGroupId groupId;

  private RaftServerProxy proxy ;

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  /**
   * Reset state machine
   */
  void reset() {
    state.clear();
    setLastAppliedTermIndex(null);
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    this.storage.init(raftStorage);
    this.proxy = (RaftServerProxy) server;
    this.groupId = groupId;
    loadSnapshot(storage.getLatestSnapshot());
  }

  private void checkInitialization() throws IOException {
    if (this.log == null) {
      ServerState state = proxy.getImpl(groupId).getState();
      this.log = state.getLog();
    }
  }

  @Override
  public void reinitialize() throws IOException {
    close();
    loadSnapshot(storage.getLatestSnapshot());
  }

  @Override
  public long takeSnapshot() {
    final Map<LogName, Long> copy;
    final TermIndex last;
    try(final AutoCloseableLock readLock = readLock()) {
      copy = new HashMap<>(state);
      last = getLastAppliedTermIndex();
    }

    final File snapshotFile =  storage.getSnapshotFile(last.getTerm(), last.getIndex());
    LOG.info("Taking a snapshot to file {}", snapshotFile);

    try(final ObjectOutputStream out = new ObjectOutputStream(
        new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
      out.writeObject(copy);
    } catch(IOException ioe) {
      LOG.warn("Failed to write snapshot file \"" + snapshotFile
          + "\", last applied index=" + last);
    }

    return last.getIndex();
  }

  private long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    return load(snapshot, false);
  }

  @SuppressWarnings("unchecked")
  private long load(SingleFileSnapshotInfo snapshot, boolean reload) throws IOException {
    if (snapshot == null) {
      LOG.warn("The snapshot info is null.");
      return RaftServerConstants.INVALID_LOG_INDEX;
    }
    final File snapshotFile = snapshot.getFile().getPath().toFile();
    if (!snapshotFile.exists()) {
      LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try(final AutoCloseableLock writeLock = writeLock();
        final ObjectInputStream in = new ObjectInputStream(
            new BufferedInputStream(new FileInputStream(snapshotFile)))) {
      if (reload) {
        reset();
      }
      setLastAppliedTermIndex(last);
      state.putAll((Map<LogName, Long>) in.readObject());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
    return last.getIndex();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public CompletableFuture<Message> query(Message request) {

    try {

      checkInitialization();
      LogServiceRequestProto logServiceRequestProto =
          LogServiceRequestProto.parseFrom(request.getContent());

      switch (logServiceRequestProto.getRequestCase()) {

        case READNEXTQUERY:
          return processReadRequest(logServiceRequestProto);
        case LENGTHQUERY:
          return processGetLengthRequest(logServiceRequestProto);
        case STARTINDEXQUERY:
          return processGetStartIndexRequest(logServiceRequestProto);
        case LISTLOGS:
          return processListLogsRequest();
        case GETLOG:
          return processGetLogRequest(logServiceRequestProto);
        case GETSTATE:
          return processGetStateRequest(logServiceRequestProto);
        default:
          // TODO
          throw new RuntimeException(
            "Wrong message type for query: " + logServiceRequestProto.getRequestCase());
      }

    } catch (IOException e) {
      // TODO exception handling
      throw new RuntimeException(e);
    }

  }

  /**
   * Process get start index request
   * @param msg message
   * @return reply message
   */
  private CompletableFuture<Message>
      processGetStartIndexRequest(LogServiceRequestProto proto)
  {
    long startIndex =log.getStartIndex();
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogStartIndexReplyProto(startIndex, null).toByteString()));
  }

  /**
   * Process get length request
   * @param msg message
   * @return reply message
   */
  private CompletableFuture<Message> processGetLengthRequest(LogServiceRequestProto proto) {
    Long len = null;
    GetLogLengthRequestProto msgProto = proto.getLengthQuery();
    LogName logName = LogName.of(msgProto.getLogName().getName());
    try(final AutoCloseableLock readLock = readLock()) {
      len = state.get(logName);
      if (len == null) {
        len = new Long(-1);
      }
    }
    LOG.debug("QUERY: {}, RESULT: {}", msgProto, len);
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogLengthReplyProto(len, null).toByteString()));
  }

  /**
   * Process read log entries request
   * @param msg message
   * @return reply message
   */
  private CompletableFuture<Message> processReadRequest(LogServiceRequestProto proto) {

    ReadLogRequestProto msgProto = proto.getReadNextQuery();
    long startRecordId = msgProto.getStartRecordId();
    int num = msgProto.getNumRecords();
    Throwable t = null;
    List<byte[]> list = new ArrayList<byte[]>();
    for (long index = startRecordId; index < startRecordId + num; index++) {
      try {
        list.add(log.getEntryWithData(index).getEntry().getStateMachineLogEntry().getLogData().toByteArray());
      } catch(RaftLogIOException e) {
        t = e;
        list = null;
        break;
      }
    }
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toReadLogReplyProto(list, t).toByteString()));
  }

  /**
   * Process sync request
   * @param trx transaction
   * @param logMessage message
   * @return reply message
   */
  private CompletableFuture<Message> processSyncRequest(TransactionContext trx,
      LogServiceRequestProto logMessage) {

    // TODO
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toSyncLogReplyProto(null).toByteString()));

  }

  private CompletableFuture<Message> processAppendRequest(TransactionContext trx,
      LogServiceRequestProto logProto) {

    final LogEntryProto entry = trx.getLogEntry();
    AppendLogEntryRequestProto proto = logProto.getAppendRequest();
    final long index = entry.getIndex();
    Long val = null;
    LogName name = null;
    long total = 0;
    try (final AutoCloseableLock writeLock = writeLock()) {
      name = LogServiceProtoUtil.toLogName(proto.getLogName());
      List<byte[]> entries = LogServiceProtoUtil.toListByteArray(proto.getDataList());
      for (byte[] bb : entries) {
        total += bb.length;
      }
      val = state.get(name);
      if (val == null) {
        val = new Long(0);
      }
      state.put(name, val + total);
      // TODO do we need this for other write request (close, sync)
      updateLastAppliedTermIndex(entry.getTerm(), index);
    }
    final CompletableFuture<Message> f =
        // TODO record ids?
        CompletableFuture.completedFuture(Message
          .valueOf(LogServiceProtoUtil.toAppendLogReplyProto(null, null).toByteString()));
    final RaftProtos.RaftPeerRole role = trx.getServerRole();
    LOG.debug("{}:{}-{}: {} new length {}", role, getId(), index, proto, val);
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}-{}: variables={}", getId(), index, state);
    }
    return f;
  }

  @Override
  public void close() {
    reset();
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      checkInitialization();
      final LogEntryProto entry = trx.getLogEntry();
      LogServiceRequestProto logServiceRequestProto =
          LogServiceRequestProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
      switch (logServiceRequestProto.getRequestCase()) {

        case CREATELOG:
          return processCreateLogRequest(logServiceRequestProto);
        case ARCHIVELOG:
          return processArchiveLog(logServiceRequestProto);
        case CLOSELOG:
          return processCloseLog(logServiceRequestProto);
        case DELETELOG:
          return processDeleteLog(logServiceRequestProto);
        case APPENDREQUEST:
          return processAppendRequest(trx, logServiceRequestProto);
        case SYNCREQUEST:
          return processSyncRequest(trx, logServiceRequestProto);
        default:
          //TODO
          return null;
      }
    } catch (IOException e) {
      // TODO exception handling
      throw new RuntimeException(e);
    }
  }

  private CompletableFuture<Message>
      processDeleteLog(LogServiceRequestProto logServiceRequestProto) {
    DeleteLogRequestProto deleteLog = logServiceRequestProto.getDeleteLog();
    LogName logName = LogServiceProtoUtil.toLogName(deleteLog.getLogName());
    try (final AutoCloseableLock writeLock = writeLock()) {
      state.remove(logName);
    }
    // TODO need to handle exceptions while operating with files.
    return CompletableFuture.completedFuture(Message
      .valueOf(DeleteLogReplyProto.newBuilder().build().toByteString()));
  }

  private CompletableFuture<Message> processCloseLog(LogServiceRequestProto logServiceRequestProto) {
    CloseLogRequestProto closeLog = logServiceRequestProto.getCloseLog();
    LogName logName = LogServiceProtoUtil.toLogName(closeLog.getLogName());
    // Need to check whether the file is opened if opened close it.
    // TODO need to handle exceptions while operating with files.
    return CompletableFuture.completedFuture(Message
      .valueOf(CloseLogReplyProto.newBuilder().build().toByteString()));
  }

  private CompletableFuture<Message>
      processArchiveLog(LogServiceRequestProto logServiceRequestProto) {
    ArchiveLogRequestProto archiveLog = logServiceRequestProto.getArchiveLog();
    LogName logName = LogServiceProtoUtil.toLogName(archiveLog.getLogName());
    // Handle log archiving.
    return CompletableFuture.completedFuture(Message
      .valueOf(ArchiveLogReplyProto.newBuilder().build().toByteString()));
  }

  private CompletableFuture<Message> processGetStateRequest(
      LogServiceRequestProto logServiceRequestProto) {
    GetStateRequestProto getState = logServiceRequestProto.getGetState();
    LogName logName = LogServiceProtoUtil.toLogName(getState.getLogName());
    return CompletableFuture.completedFuture(Message.valueOf(LogServiceProtoUtil
        .toGetStateReplyProto(state.containsKey(logName)).toByteString()));
  }

  private CompletableFuture<Message> processCreateLogRequest(
      LogServiceRequestProto logServiceRequestProto) {
    Long val;
    LogName name;
    try (final AutoCloseableLock writeLock = writeLock()) {
      CreateLogRequestProto createLog = logServiceRequestProto.getCreateLog();
      name = LogServiceProtoUtil.toLogName(createLog.getLogName());
      val = state.get(name);
      if (val == null) {
        val = new Long(0);
      }
      state.put(name, val);
    }
    //TODO This can't be part of a state machine (REMOVE)
    return CompletableFuture.completedFuture(Message.valueOf(LogServiceProtoUtil
        .toCreateLogReplyProto(
          new LogStreamImpl(name, null, new LogServiceConfiguration())).toByteString()));
  }

  //TODO REMOVE this code
  private CompletableFuture<Message> processListLogsRequest() {
    List<LogStream> logStreams = new ArrayList<LogStream>(state.size());
    for (Entry<LogName, Long> e : state.entrySet()) {
      logStreams.add(new LogStreamImpl(e.getKey(), null, new LogServiceConfiguration()));
    }
    return CompletableFuture.completedFuture(Message.valueOf(LogServiceProtoUtil
        .toListLogLogsReplyProto(logStreams).toByteString()));
  }

  //TODO REMOVE this code

  private CompletableFuture<Message> processGetLogRequest(
      LogServiceRequestProto logServiceRequestProto) {
    GetLogRequestProto getLog = logServiceRequestProto.getGetLog();
    LogName logName = LogServiceProtoUtil.toLogName(getLog.getLogName());
    if (state.containsKey(logName)) {
      return CompletableFuture.completedFuture(Message.valueOf(LogServiceProtoUtil
          .toGetLogReplyProto(new LogStreamImpl(logName, null, new LogServiceConfiguration()))
          .toByteString()));
    } else {
      return CompletableFuture.completedFuture(Message.valueOf(GetLogReplyProto.newBuilder()
          .build().toByteString()));
    }
  }
}
