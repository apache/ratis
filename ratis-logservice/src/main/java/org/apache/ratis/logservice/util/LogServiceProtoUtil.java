/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.ratis.logservice.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.logservice.api.LogMessage;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.logservice.impl.BaseLogStream;
import org.apache.ratis.proto.logservice.LogServiceProtos.ArchiveLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CloseLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CreateLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CreateLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.DeleteLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetStateReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetStateRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ListLogsReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ListLogsReplyProto.Builder;
import org.apache.ratis.proto.logservice.LogServiceProtos.ListLogsRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogNameProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogServiceRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogStreamProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogStreamState;

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

  public static LogMessage toLogMessage(
      org.apache.ratis.proto.logservice.LogServiceProtos.LogMessage message) {
    if (!message.getData().isEmpty()) {
      return new LogMessage(LogName.of(message.getLogName()), message.getData().toByteArray());
    } else if (message.getLength() != 0) {
      return new LogMessage(LogName.of(message.getLogName()), message.getLength());
    } else {
      return new LogMessage(LogName.of(message.getLogName()));
    }
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

  public static LogStream toLogStream(LogStreamProto logStream) {
    return new BaseLogStream(toLogName(logStream.getLogName()),
        (logStream.getState() == LogStreamState.OPEN ? State.OPEN : State.CLOSED),
        logStream.getSize());
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

  public static List<LogStream> toListLogStreams(List<LogStreamProto> logStreamProtos) {
    List<LogStream> logStreams = new ArrayList<>(logStreamProtos.size());
    for (LogStreamProto proto : logStreamProtos) {
      logStreams.add(toLogStream(proto));
    }
    return logStreams;
  }

  public static GetLogReplyProto toGetLogReplyProto(LogStream logStream) {
    return GetLogReplyProto.newBuilder().setLogStream(toLogStreamProto(logStream)).build();
  }

  public static GetStateReplyProto toGetStateReplyProto(boolean exists) {
    return GetStateReplyProto.newBuilder()
        .setState(exists ? LogStreamState.OPEN : LogStreamState.CLOSED).build();
  }
}
