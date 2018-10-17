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
package org.apache.ratis.logservice.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogService;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.logservice.impl.LogStreamImpl;
import org.apache.ratis.proto.logservice.LogServiceProtos.AppendLogEntryReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.AppendLogEntryRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ArchiveLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ArchiveLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CloseLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CloseLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CreateLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CreateLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.DeleteLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.DeleteLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogLengthReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogLengthRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogStartIndexReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogStartIndexRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetStateReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetStateRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ListLogsReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ListLogsReplyProto.Builder;
import org.apache.ratis.proto.logservice.LogServiceProtos.ListLogsRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogNameProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogServiceException;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogServiceRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogStreamProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogStreamState;
import org.apache.ratis.proto.logservice.LogServiceProtos.ReadLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ReadLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.SyncLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.SyncLogRequestProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

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
public class LogServiceProtoUtil {
  public static LogServiceRequestProto toCreateLogRequestProto(LogName logName) {
    LogNameProto logNameProto = LogNameProto.newBuilder().setName(logName.getName()).build();
    CreateLogRequestProto createLog =
        CreateLogRequestProto.newBuilder().setLogName(logNameProto).build();
    return LogServiceRequestProto.newBuilder().setCreateLog(createLog).build();
  }

  public static LogServiceRequestProto toListLogRequestProto() {
    ListLogsRequestProto listLogs = ListLogsRequestProto.newBuilder().build();
    return LogServiceRequestProto.newBuilder().setListLogs(listLogs).build();
  }

  public static LogServiceRequestProto toGetLogRequestProto(LogName name) {
    GetLogRequestProto getLog =
        GetLogRequestProto.newBuilder().setLogName(toLogNameProto(name)).build();
    return LogServiceRequestProto.newBuilder().setGetLog(getLog).build();
  }

  public static LogServiceRequestProto toCloseLogRequestProto(LogName logName) {
    LogNameProto logNameProto = LogNameProto.newBuilder().setName(logName.getName()).build();
    CloseLogRequestProto closeLog =
        CloseLogRequestProto.newBuilder().setLogName(logNameProto).build();
    return LogServiceRequestProto.newBuilder().setCloseLog(closeLog).build();
  }

  public static CloseLogReplyProto toCloseLogReplyProto() {
    CloseLogReplyProto.Builder builder = CloseLogReplyProto.newBuilder();
    return builder.build();
  }

  public static LogServiceRequestProto toGetStateRequestProto(LogName logName) {
    LogNameProto logNameProto = LogNameProto.newBuilder().setName(logName.getName()).build();
    GetStateRequestProto getState =
        GetStateRequestProto.newBuilder().setLogName(logNameProto).build();
    return LogServiceRequestProto.newBuilder().setGetState(getState).build();
  }

  public static LogServiceRequestProto toArchiveLogRequestProto(LogName logName) {
    LogNameProto logNameProto = LogNameProto.newBuilder().setName(logName.getName()).build();
    ArchiveLogRequestProto archiveLog =
        ArchiveLogRequestProto.newBuilder().setLogName(logNameProto).build();
    return LogServiceRequestProto.newBuilder().setArchiveLog(archiveLog).build();
  }

  public static LogServiceRequestProto toDeleteLogRequestProto(LogName logName) {
    LogNameProto logNameProto = LogNameProto.newBuilder().setName(logName.getName()).build();
    DeleteLogRequestProto deleteLog =
        DeleteLogRequestProto.newBuilder().setLogName(logNameProto).build();
    return LogServiceRequestProto.newBuilder().setDeleteLog(deleteLog).build();
  }

  public static DeleteLogReplyProto toDeleteLogReplyProto() {
    DeleteLogReplyProto.Builder builder = DeleteLogReplyProto.newBuilder();
    return builder.build();
  }

  public static LogNameProto toLogNameProto(LogName logName) {
    return LogNameProto.newBuilder().setName(logName.getName()).build();
  }

  public static LogName toLogName(LogNameProto logNameProto) {
    return LogName.of(logNameProto.getName());
  }

  public static LogStreamProto toLogStreamProto(LogStream logStream) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(logStream.getName().getName()).build();
    LogStreamProto logStreamProto =
        LogStreamProto
            .newBuilder()
            .setLogName(logNameProto)
            .setSize(logStream.getSize())
            .setState(
              logStream.getState().equals(State.OPEN) ? LogStreamState.OPEN : LogStreamState.CLOSED)
            .build();
    return logStreamProto;
  }

  public static LogStream toLogStream(LogStreamProto logStream, LogService parent) {
    return new LogStreamImpl(logStream, parent);
  }

  public static CreateLogReplyProto toCreateLogReplyProto(LogStream logStream) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(logStream.getName().getName()).build();
    LogStreamProto logStreamProto =
        LogStreamProto
            .newBuilder()
            .setLogName(logNameProto)
            .setSize(logStream.getSize())
            .setState(
              logStream.getState().equals(State.OPEN) ? LogStreamState.OPEN : LogStreamState.CLOSED)
            .build();
    return CreateLogReplyProto.newBuilder().setLogStream(logStreamProto).build();
  }

  public static ListLogsReplyProto toListLogLogsReplyProto(List<LogStream> logStreams) {
    Builder newBuilder = ListLogsReplyProto.newBuilder();
    for (LogStream stream : logStreams) {
      newBuilder.addLogStrems(toLogStreamProto(stream));
    }
    return newBuilder.build();
  }

  public static ArchiveLogReplyProto toArchiveLogReplyProto() {
    ArchiveLogReplyProto.Builder builder = ArchiveLogReplyProto.newBuilder();
    return builder.build();
  }

  public static LogServiceRequestProto toGetLengthRequestProto(LogName name) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    GetLogLengthRequestProto.Builder builder = GetLogLengthRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    return LogServiceRequestProto.newBuilder().setLengthQuery(builder.build()).build();
  }

  public static LogServiceRequestProto toGetStartIndexProto(LogName name) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    GetLogStartIndexRequestProto.Builder builder = GetLogStartIndexRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    return LogServiceRequestProto.newBuilder().setStartIndexQuery(builder.build()).build();
  }

  public static LogServiceRequestProto toReadLogRequestProto(LogName name, long start, int total) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    ReadLogRequestProto.Builder builder = ReadLogRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    builder.setStartRecordId(start);
    builder.setNumRecords(total);
    return LogServiceRequestProto.newBuilder().setReadNextQuery(builder.build()).build();
  }

  public static LogServiceRequestProto toSyncLogRequestProto(LogName name) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    SyncLogRequestProto.Builder builder = SyncLogRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    return LogServiceRequestProto.newBuilder().setSyncRequest(builder.build()).build();
  }

  public static LogServiceRequestProto toAppendEntryLogRequestProto(LogName name,
      List<byte[]> entries) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    AppendLogEntryRequestProto.Builder builder = AppendLogEntryRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    for (int i=0; i < entries.size(); i++) {
      builder.addData(ByteString.copyFrom(entries.get(i)));
    }
    return LogServiceRequestProto.newBuilder().setAppendRequest(builder.build()).build();
  }

  public static LogServiceRequestProto toAppendBBEntryLogRequestProto(LogName name,
      List<ByteBuffer> entries) {
    LogNameProto logNameProto =
        LogNameProto.newBuilder().setName(name.getName()).build();
    AppendLogEntryRequestProto.Builder builder = AppendLogEntryRequestProto.newBuilder();
    builder.setLogName(logNameProto);
    for (int i=0; i < entries.size(); i++) {
      builder.addData(ByteString.copyFrom(entries.get(i)));
    }
    return LogServiceRequestProto.newBuilder().setAppendRequest(builder.build()).build();
  }

  public static List<LogStream> toListLogStreams(List<LogStreamProto> logStreamProtos,
      LogService parent) {
    List<LogStream> logStreams = new ArrayList<>(logStreamProtos.size());
    for (LogStreamProto proto : logStreamProtos) {
      logStreams.add(toLogStream(proto, parent));
    }
    return logStreams;
  }

  public static List<byte[]> toListByteArray(List<ByteString> list) {
    List<byte[]> retVal = new ArrayList<byte[]>(list.size());
    for(int i=0; i < list.size(); i++) {
      retVal.add(list.get(i).toByteArray());
    }
    return retVal;
  }

  public static GetLogReplyProto toGetLogReplyProto(LogStream logStream) {
    return GetLogReplyProto.newBuilder().setLogStream(toLogStreamProto(logStream)).build();
  }

  public static GetStateReplyProto toGetStateReplyProto(boolean exists) {
    return GetStateReplyProto.newBuilder()
        .setState(exists ? LogStreamState.OPEN : LogStreamState.CLOSED).build();
  }


  public static GetLogLengthReplyProto toGetLogLengthReplyProto(long length, Throwable t) {
    GetLogLengthReplyProto.Builder builder = GetLogLengthReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else {
      builder.setLength(length);
    }
    return builder.build();
  }

  public static GetLogStartIndexReplyProto toGetLogStartIndexReplyProto(long length, Throwable t) {
    GetLogStartIndexReplyProto.Builder builder = GetLogStartIndexReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else {
      builder.setStartIndex(length);
    }
    return builder.build();
  }

  public static ReadLogReplyProto toReadLogReplyProto(List<byte[]> entries, Throwable t) {
    ReadLogReplyProto.Builder builder = ReadLogReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    } else {
      for(byte[] record: entries) {
        builder.addLogRecord( ByteString.copyFrom(record));
      }
    }
    return builder.build();
  }

  public static AppendLogEntryReplyProto toAppendLogReplyProto(List<Long> ids, Throwable t) {
    AppendLogEntryReplyProto.Builder builder = AppendLogEntryReplyProto.newBuilder();
    if (t!= null) {
      builder.setException(toLogException(t));
    } else if (ids != null){
      int index = 0;
      for(long id: ids) {
        builder.setRecordId(index++, id);
      }
    }
    return builder.build();
  }

  public static SyncLogReplyProto toSyncLogReplyProto(Throwable t) {
    SyncLogReplyProto.Builder builder = SyncLogReplyProto.newBuilder();
    if (t != null) {
      builder.setException(toLogException(t));
    }
    return builder.build();
  }

  public GetLogLengthReplyProto toGetLogLengthReplyProto(long length) {
    GetLogLengthReplyProto.Builder builder = GetLogLengthReplyProto.newBuilder();
    builder.setLength(length);
    return builder.build();
  }

  public static LogServiceException toLogException(Throwable t) {
    LogServiceException.Builder builder = LogServiceException.newBuilder();
    builder.setExceptionClassName(t.getClass().getName());
    builder.setErrorMsg(t.getMessage());
    StackTraceElement[] trace = t.getStackTrace();
    StringBuffer buf = new StringBuffer();
    for (StackTraceElement el: trace) {
      buf.append(el.toString()).append("\n");
    }
    String strace = buf.toString();
    builder.setStacktrace(ByteString.copyFrom(strace, Charset.defaultCharset()));
    return builder.build();
  }

}
