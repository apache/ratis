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
package org.apache.ratis.logservice.server;

import static org.apache.ratis.logservice.api.LogStream.State;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.codahale.metrics.Timer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.api.ArchiveLogWriter;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.common.Constants;
import org.apache.ratis.logservice.impl.ArchiveHdfsLogReader;
import org.apache.ratis.logservice.impl.ArchiveHdfsLogWriter;
import org.apache.ratis.logservice.metrics.LogServiceMetrics;
import org.apache.ratis.logservice.proto.LogServiceProtos;
import org.apache.ratis.logservice.proto.LogServiceProtos.AppendLogEntryRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogLengthRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogSizeRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogServiceRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.ReadLogRequestProto;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.logservice.server.ArchivalInfo.ArchivalStatus;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.TextFormat;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStateMachine extends BaseStateMachine {
  public static final Logger LOG = LoggerFactory.getLogger(LogStateMachine.class);
  public static final long DEFAULT_ARCHIVE_THRESHOLD_PER_FILE = 1000000;
  private final RaftProperties properties;
  private LogServiceMetrics logServiceMetrics;
  private Timer sizeRequestTimer;
  private Timer readNextQueryTimer;
  private Timer getStateTimer;
  private Timer lastIndexQueryTimer;
  private Timer lengthQueryTimer;
  private Timer startIndexTimer;
  private Timer appendRequestTimer;
  private Timer syncRequesTimer;
  private Timer archiveLogRequestTimer;
  private Timer getCloseLogTimer;
  private RaftClient client;
  //Archival information
  /*
   *  State is a log's length, size, and state (closed/open);
   */
  private long length;

  /**
   * The size (number of bytes) of the log records. Does not include Ratis storage overhead
   */
  private long dataRecordsSize;

  private State state = State.OPEN;

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private RaftLog log;


  private RaftServer proxy;
  private ExecutorService executorService;
  private boolean isArchivalRequest;
  private ArchivalInfo archivalInfo;
  private Map<String,ArchivalInfo> exportMap = new HashMap<String,ArchivalInfo>();
  private Map<String, Future<Boolean>> archiveExportFutures = new HashMap<>();
  private Timer archiveLogTimer;

  public LogStateMachine(RaftProperties properties) {
    this.properties = properties;
  }

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
    this.length = 0;
    this.dataRecordsSize = 0;
    setLastAppliedTermIndex(TermIndex.valueOf(0, -1));
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    this.storage.init(raftStorage);
    this.proxy = server;
    //TODO: using groupId for metric now but better to tag it with LogName
    this.logServiceMetrics = new LogServiceMetrics(groupId.toString(),
        server.getId().toString());
    this.readNextQueryTimer = logServiceMetrics.getTimer("readNextQueryTime");
    this.startIndexTimer= logServiceMetrics.getTimer("startIndexTime");
    this.sizeRequestTimer = logServiceMetrics.getTimer("sizeRequestTime");
    this.getStateTimer = logServiceMetrics.getTimer("getStateTime");
    this.lastIndexQueryTimer = logServiceMetrics.getTimer("lastIndexQueryTime");
    this.lengthQueryTimer = logServiceMetrics.getTimer("lengthQueryTime");
    this.syncRequesTimer = logServiceMetrics.getTimer("syncRequesTime");
    this.appendRequestTimer = logServiceMetrics.getTimer("appendRequestTime");
    this.getCloseLogTimer = logServiceMetrics.getTimer("getCloseLogTime");
    //archiving request time not the actual archiving time
    this.archiveLogRequestTimer = logServiceMetrics.getTimer("archiveLogRequestTime");
    this.archiveLogTimer = logServiceMetrics.getTimer("archiveLogTime");
    loadSnapshot(storage.getLatestSnapshot());
    executorService = Executors.newSingleThreadExecutor();
    this.archivalInfo =
        new ArchivalInfo(properties.get(Constants.LOG_SERVICE_ARCHIVAL_LOCATION_KEY));


  }

  private void checkInitialization() throws IOException {
    if (this.log == null) {
      this.log = proxy.getDivision(getGroupId()).getRaftLog();
    }
  }

  @Override
  public void reinitialize() throws IOException {
    close();
    loadSnapshot(storage.getLatestSnapshot());
  }

  @Override
  public long takeSnapshot() {
    final TermIndex last;
    try(AutoCloseableLock readLock = readLock()) {
      last = getLastAppliedTermIndex();
    }

    final File snapshotFile =  storage.getSnapshotFile(last.getTerm(), last.getIndex());
    LOG.info("Taking a snapshot to file {}", snapshotFile);

    try(AutoCloseableLock readLock = readLock();
        ObjectOutputStream out = new ObjectOutputStream(
        new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
      out.writeLong(length);
      out.writeLong(dataRecordsSize);
      out.writeObject(state);
    } catch(IOException ioe) {
      LOG.warn("Failed to write snapshot file \"" + snapshotFile
          + "\", last applied index=" + last);
    }

    return last.getIndex();
  }

  private long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    return load(snapshot, false);
  }

  private long load(SingleFileSnapshotInfo snapshot, boolean reload) throws IOException {
    if (snapshot == null) {
      LOG.warn("The snapshot info is null.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    final File snapshotFile = snapshot.getFile().getPath().toFile();
    if (!snapshotFile.exists()) {
      LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
      return RaftLog.INVALID_LOG_INDEX;
    }

    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try(AutoCloseableLock writeLock = writeLock();
        ObjectInputStream in = new ObjectInputStream(
            new BufferedInputStream(new FileInputStream(snapshotFile)))) {
      if (reload) {
        reset();
      }
      setLastAppliedTermIndex(last);
      this.length = in.readLong();
      this.dataRecordsSize = in.readLong();
      this.state = (State) in.readObject();
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
      if (LOG.isTraceEnabled()) {
        LOG.trace("Processing LogService query: {}", TextFormat.shortDebugString(logServiceRequestProto));
      }

      switch (logServiceRequestProto.getRequestCase()) {

        case READNEXTQUERY:
          return recordTime(readNextQueryTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processReadRequest(logServiceRequestProto);
            }
          });
        case SIZEREQUEST:
          return recordTime(sizeRequestTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetSizeRequest(logServiceRequestProto);
            }
          });
        case STARTINDEXQUERY:
          return recordTime(startIndexTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetStartIndexRequest(logServiceRequestProto);
            }
          });
        case GETSTATE:
          return recordTime(getStateTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetStateRequest();
            }
          });
        case LASTINDEXQUERY:
          return recordTime(lastIndexQueryTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetLastCommittedIndexRequest(logServiceRequestProto);
            }
          });
        case LENGTHQUERY:
          return recordTime(lengthQueryTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetLengthRequest(logServiceRequestProto);
            }
          });
      case ARCHIVELOG:
        return recordTime(archiveLogRequestTimer, new Task(){
          @Override public CompletableFuture<Message> run() {
            return processArchiveLog(logServiceRequestProto);
          }});
      case EXPORTINFO:
        return processExportInfo();
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

  private CompletableFuture<Message> processExportInfo() {
    LogServiceProtos.GetExportInfoReplyProto.Builder exportBuilder =
        LogServiceProtos.GetExportInfoReplyProto.newBuilder();
    exportMap.values().forEach(
        archInfo -> exportBuilder.addInfo(LogServiceProtoUtil.toExportInfoProto(archInfo)));

    return CompletableFuture.completedFuture(Message.valueOf(exportBuilder.build().toByteString()));
  }

  /**
   * Process get start index request
   * @param proto message
   * @return reply message
   */
  private CompletableFuture<Message>
      processGetStartIndexRequest(LogServiceRequestProto proto) {

    Throwable t = verifyState(State.OPEN);
    long startIndex = log.getStartIndex();
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogStartIndexReplyProto(startIndex, t).toByteString()));
  }

  /**
   * Process get last committed record index
   * @param proto message
   * @return reply message
   */
  private CompletableFuture<Message>
      processGetLastCommittedIndexRequest(LogServiceRequestProto proto) {
    Throwable t = verifyState(State.OPEN);
    long lastIndex = log.getLastCommittedIndex();
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogLastIndexReplyProto(lastIndex, t).toByteString()));
  }

  /**
   * Process get length request
   * @param proto message
   * @return reply message
   */
  private CompletableFuture<Message> processGetSizeRequest(LogServiceRequestProto proto) {
    GetLogSizeRequestProto msgProto = proto.getSizeRequest();
    Throwable t = verifyState(State.OPEN);
    LOG.trace("Size query: {}, Result: {}", msgProto, this.dataRecordsSize);
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogSizeReplyProto(this.dataRecordsSize, t).toByteString()));
  }

  private CompletableFuture<Message> processGetLengthRequest(LogServiceRequestProto proto) {
    GetLogLengthRequestProto msgProto = proto.getLengthQuery();
    Throwable t = verifyState(State.OPEN);
    LOG.trace("Length query: {}, Result: {}", msgProto, this.length);
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogLengthReplyProto(this.length, t).toByteString()));
  }
  /**
   * Process read log entries request
   * @param proto message
   * @return reply message
   */
  private CompletableFuture<Message> processReadRequest(LogServiceRequestProto proto) {
    ReadLogRequestProto msgProto = proto.getReadNextQuery();
    // Get the recordId the user wants to start reading at
    long startRecordId = msgProto.getStartRecordId();
    // And the number of records they want to read
    int numRecordsToRead = msgProto.getNumRecords();
    //Log must have been closed while Archiving , so we can let user only to
    // read when the log is either OPEN or ARCHIVED
    Throwable t = verifyState(State.OPEN, State.ARCHIVING, State.CLOSED, State.ARCHIVED);
    List<byte[]> list = null;

    if (t == null) {
      RaftLogReader reader = null;
      try {
        if (this.state == State.OPEN || this.state == State.CLOSED
            || this.state == State.ARCHIVING) {
          reader = new LogServiceRaftLogReader(log);
        } else if (this.state == State.ARCHIVED) {
          reader = new ArchiveHdfsLogReader(LogServiceUtils
              .getArchiveLocationForLog(archivalInfo.getArchiveLocation(),
                  archivalInfo.getArchiveLogName()));
        } else {
          //could be a race condition
          t = verifyState(State.OPEN, State.ARCHIVED);
        }
        if (t == null && reader != null) {
          list = new ArrayList<byte[]>();
          reader.seek(startRecordId);
          for (int i = 0; i < numRecordsToRead; i++) {
            if (!reader.hasNext()) {
              break;
            }
            list.add(reader.next());
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to execute ReadNextQuery", e);
        t = e;
        list = null;
      }
    }
    return CompletableFuture.completedFuture(
      Message.valueOf(LogServiceProtoUtil.toReadLogReplyProto(list, t).toByteString()));
  }

  /**
   * Process sync request
   * @param trx transaction
   * @param logMessage message
   * @return reply message
   */
  private CompletableFuture<Message> processSyncRequest(TransactionContext trx,
      LogServiceRequestProto logMessage) {
     long index = trx.getLogEntry().getIndex();
    // TODO: Do we really need this call?
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toSyncLogReplyProto(index, null).toByteString()));

  }

  private CompletableFuture<Message> processAppendRequest(TransactionContext trx,
      LogServiceRequestProto logProto) {

    final LogEntryProto entry = trx.getLogEntry();
    AppendLogEntryRequestProto proto = logProto.getAppendRequest();
    final long index = entry.getIndex();
    long newSize = 0;
    Throwable t = verifyState(State.OPEN);
    final List<Long> ids = new ArrayList<Long>();
    if (t == null) {
      try (AutoCloseableLock writeLock = writeLock()) {
          List<byte[]> entries = LogServiceProtoUtil.toListByteArray(proto.getDataList());
          for (byte[] bb : entries) {
            ids.add(this.length);
            newSize += bb.length;
            this.length++;
          }
          this.dataRecordsSize += newSize;
          // TODO do we need this for other write request (close, sync)
          updateLastAppliedTermIndex(entry.getTerm(), index);
      }
    }
    final CompletableFuture<Message> f =
        CompletableFuture.completedFuture(
          Message.valueOf(LogServiceProtoUtil.toAppendLogReplyProto(ids, t).toByteString()));
    final RaftProtos.RaftPeerRole role = trx.getServerRole();
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}:{}-{}: {} new length {}", role, getId(), index,
          TextFormat.shortDebugString(proto), dataRecordsSize);
    }
    return f;
  }

  @Override
  public void close() {
    reset();
    logServiceMetrics.unregister();
    if (client != null) {
      try {
        client.close();
      } catch (Exception ignored) {
        LOG.warn("{} is ignored", JavaUtils.getClassSimpleName(ignored.getClass()), ignored);
      }
    }
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      checkInitialization();
      final LogEntryProto entry = trx.getLogEntry();
      LogServiceRequestProto logServiceRequestProto =
          LogServiceRequestProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
      switch (logServiceRequestProto.getRequestCase()) {
      case CHANGESTATE:
          return recordTime(getCloseLogTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processChangeState(logServiceRequestProto);
            }});
        case APPENDREQUEST:
          return recordTime(appendRequestTimer, new Task(){
              @Override public CompletableFuture<Message> run() {
                return processAppendRequest(trx, logServiceRequestProto);
              }});
        case SYNCREQUEST:
          return recordTime(syncRequesTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processSyncRequest(trx, logServiceRequestProto);
            }});
        case ARCHIVELOG:
          return updateArchiveLogInfo(logServiceRequestProto);
        default:
          //TODO
          return null;
      }
    } catch (IOException e) {
      // TODO exception handling
      throw new RuntimeException(e);
    }
  }



  private CompletableFuture<Message> processChangeState(LogServiceRequestProto logServiceRequestProto) {
    LogServiceProtos.ChangeStateLogRequestProto changeState = logServiceRequestProto.getChangeState();
    // Need to check whether the file is opened if opened close it.
    // TODO need to handle exceptions while operating with files.

    State targetState = State.valueOf(changeState.getState().name());
    Throwable t = null;
    //if forced skip checking states
    if(!changeState.getForce()) {
      switch (targetState) {
      case OPEN:
        if (state != null) {
          t = verifyState(State.OPEN, State.CLOSED);
        }
        break;
      case CLOSED:
        t =  verifyState(State.OPEN);
        break;
      case ARCHIVED:
        t = verifyState(State.ARCHIVING);
        break;
      case ARCHIVING:
        t = verifyState(State.CLOSED);
        break;
      case DELETED:
        t = verifyState(State.CLOSED);
        break;
      default:
      }
    }
    if(t != null) {
      return CompletableFuture.completedFuture(Message
              .valueOf(LogServiceProtos.ChangeStateReplyProto.newBuilder().
                      setException(LogServiceProtoUtil.toLogException(t)).build().toByteString()));
    }
    this.state = targetState;
    return CompletableFuture.completedFuture(Message
        .valueOf(LogServiceProtos.ChangeStateReplyProto.newBuilder().build().toByteString()));
  }

  private CompletableFuture<Message> processGetStateRequest() {
    return CompletableFuture.completedFuture(Message
        .valueOf(LogServiceProtoUtil.toGetStateReplyProto(state).toByteString()));
  }

  private Throwable verifyState(State... states) {
    for (State st : states) {
      if (this.state == st) {
        return null;
      }
    }
    return new IOException("Wrong state: " + this.state);
  }

  private CompletableFuture<Message> updateArchiveLogInfo(
      LogServiceRequestProto logServiceRequestProto) {
    LogServiceProtos.ArchiveLogRequestProto archiveLog = logServiceRequestProto.getArchiveLog();
    this.isArchivalRequest = !archiveLog.getIsExport();
    Throwable t = null;
    if(isArchivalRequest) {
      archivalInfo.updateArchivalInfo(archiveLog);
      t = verifyState(State.ARCHIVING);
    }else{
      t = verifyState(State.OPEN, State.CLOSED);
      ArchivalInfo info = exportMap.get(archiveLog.getLocation());
      if(info==null) {
        info = new ArchivalInfo(archiveLog.getLocation());
        exportMap.put(archiveLog.getLocation(),info);
      }
      info.updateArchivalInfo(archiveLog);
    }
    return CompletableFuture.completedFuture(
        Message.valueOf(LogServiceProtoUtil.toArchiveLogReplyProto(t).toByteString()));
  }

  private CompletableFuture<Message> processArchiveLog(
      LogServiceRequestProto logServiceRequestProto) {
    LogServiceProtos.ArchiveLogRequestProto archiveLog = logServiceRequestProto.getArchiveLog();
    LogName logName = LogServiceProtoUtil.toLogName(archiveLog.getLogName());
    Throwable t = null;
    try {
      String loc = null;
      this.isArchivalRequest = !archiveLog.getIsExport();
      if (isArchivalRequest) {
        loc = archivalInfo.getArchiveLocation();
        archivalInfo.updateArchivalInfo(archiveLog);
      } else {
        loc = archiveLog.getLocation();
        ArchivalInfo exportInfo =
            exportMap.putIfAbsent(loc, new ArchivalInfo(loc));
        if (exportInfo != null) {
          if (exportInfo.getLastArchivedIndex() == archiveLog
              .getLastArchivedRaftIndex()) {
            throw new IllegalStateException("Export of " + logName + "for the given location " + loc
                + "is already present and in " + exportInfo.getStatus());
          } else {
            exportInfo.updateArchivalInfo(archiveLog);
          }
        }
      }
      if (loc == null) {
        throw new IllegalArgumentException(isArchivalRequest ?
            "Location for archive is not configured" :
            "Location for export provided is null");
      }
      final String location = loc;
      long recordId = archiveLog.getLastArchivedRaftIndex();
      if (isArchivalRequest) {
        t = verifyState(State.CLOSED);
      } else {
        t = verifyState(State.OPEN, State.CLOSED);
      }
      if (t == null) {
        Callable<Boolean> callable = () -> {
          final Timer.Context timerContext = archiveLogTimer.time();
          try {
            startArchival(recordId, logName, location);
            //Init ArchiveLogWriter for writing in export/archival location
            ArchiveLogWriter writer = new ArchiveHdfsLogWriter();
            writer.init(location, logName);

            LogServiceRaftLogReader reader = new LogServiceRaftLogReader(log);
            reader.seek(recordId);
            long records = 0;
            boolean isInterrupted = false;
            while (reader.hasNext()) {
              writer.write(ByteBuffer.wrap(reader.next()));
              isInterrupted = Thread.currentThread().isInterrupted();
              if (records >= DEFAULT_ARCHIVE_THRESHOLD_PER_FILE || isInterrupted) {
                //roll writer when interuppted or no. of records threshold per file is met
                commit(writer, logName, location);
                if (isInterrupted) {
                  break;
                }
                records = 0;
              }
              records++;
            }
            writer.close();
            if (!isInterrupted) {
              //It means archival is successfully completed on this leader
              completeArchival(writer.getLastWrittenRecordId(), logName, location);
            } else {
              //Thread is interuppted either leader is going down or it become follower
              try {
                //Sleeping here before sending archival request to new leader to
                // avoid causing problem during leader election storm
                Thread.sleep(10000);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              sendArchiveLogrequestToNewLeader(writer.getLastWrittenRecordId(), logName, location);
            }
            return true;
          } catch (Exception e) {
            LOG.error("Archival failed for the log:" + logName, e);
            failArchival(recordId, logName, location);
          } finally {
            timerContext.stop();
          }
          return false;
        };

        archiveExportFutures.put(location, executorService.submit(callable));
      }
    }catch (Exception e){
      LOG.warn("Exception while processing archival request for " + logName, e);
      t = e;
    }
    return CompletableFuture.completedFuture(
        Message.valueOf(LogServiceProtoUtil.toArchiveLogReplyProto(t).toByteString()));
  }

  private void failArchival(long recordId, LogName logName, String location) throws IOException {
    updateArchivingInfo(recordId, logName, location, isArchivalRequest,
        ArchivalStatus.FAILED);
    if(isArchivalRequest) {
      sendChangeStateRequest(State.CLOSED, true);
    }
  }

  private void startArchival(long recordId, LogName logName, String location) throws IOException {
    if (isArchivalRequest) {
      sendChangeStateRequest(State.ARCHIVING, false);
    }
    updateArchivingInfo(recordId, logName, location, isArchivalRequest, ArchivalStatus.STARTED);
  }

  private void sendArchiveLogrequestToNewLeader(long recordId, LogName logName, String location)
      throws IOException {
    getClient().io().sendReadOnly(() -> LogServiceProtoUtil
        .toArchiveLogRequestProto(logName, location, recordId, isArchivalRequest,
            ArchivalStatus.INTERRUPTED).toByteString());
  }

  public void completeArchival(long recordId, LogName logName, String location) throws IOException {
    if (isArchivalRequest) {
      sendChangeStateRequest(State.ARCHIVED, false);
    }
    updateArchivingInfo(recordId, logName, location, isArchivalRequest, ArchivalStatus.COMPLETED);
  }

  private void commit(ArchiveLogWriter writer, LogName logName, String location)
      throws IOException {
    writer.rollWriter();
    updateArchivingInfo(writer.getLastWrittenRecordId(), logName, location, isArchivalRequest,
        ArchivalStatus.RUNNING);
  }

  private void updateArchivingInfo(long recordId, LogName logName, String location,
      boolean isArchival, ArchivalStatus status)
      throws IOException {
    RaftClientReply archiveLogReply = getClient().io().send(() -> LogServiceProtoUtil
        .toArchiveLogRequestProto(logName, location, recordId, isArchival, status).toByteString());
    LogServiceProtos.ArchiveLogReplyProto message=LogServiceProtos.ArchiveLogReplyProto
        .parseFrom(archiveLogReply.getMessage().getContent());
    if (message.hasException()) {
      throw new IOException(message.getException().getErrorMsg());
    }
  }

  private void sendChangeStateRequest(State st, boolean force) throws IOException {
      getClient().io().send(
          () -> LogServiceProtoUtil.toChangeStateRequestProto(LogName.of("Dummy"), st, force)
              .toByteString());
  }

  private RaftClient getClient() throws IOException {
    if (client == null) {
      try {
        RaftServer raftServer = getServer().get();
        client = RaftClient.newBuilder().setRaftGroup(getGroupFromGroupId(raftServer, getGroupId()))
            .setClientId(ClientId.randomId())
            .setProperties(raftServer.getProperties()).build();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return client;
  }

  private RaftGroup getGroupFromGroupId(RaftServer raftServer, RaftGroupId raftGroupId)
      throws IOException {
    List<RaftGroup> x = StreamSupport.stream(raftServer.getGroups().spliterator(), false)
        .filter(group -> group.getGroupId().equals(raftGroupId)).collect(Collectors.toList());
    if (x.size() == 1) {
      return x.get(0);
    } else {
      throw new IOException(x.size() + " are group found for group id:" + raftGroupId);
    }
  }

  @Override public void notifyNotLeader(Collection<TransactionContext> pendingEntries)
      throws IOException {
    for(Future<Boolean> archiveFuture:archiveExportFutures.values()) {
      if (archiveFuture != null && !archiveFuture.isCancelled()) {
        archiveFuture.cancel(true);
      }
    }
  }

}
