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
package org.apache.ratis.logservice.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogService;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.logservice.api.RecordListener;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.proto.logservice.LogServiceProtos.ArchiveLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CloseLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CreateLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.DeleteLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetStateReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ListLogsReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogStreamProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogStreamState;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

public class LogServiceImpl implements LogService {

  final private RaftClient raftClient;
  final private LogServiceConfiguration config;

  public LogServiceImpl(RaftClient raftClient, LogServiceConfiguration config) {
    this.raftClient = raftClient;
    this.config = config;
  }

  @Override
  public LogStream createLog(LogName name) throws IOException {
    RaftClientReply reply =
        raftClient.send(Message.valueOf(LogServiceProtoUtil.toCreateLogRequestProto(name)
            .toByteString()));
    CreateLogReplyProto parseFrom = CreateLogReplyProto.parseFrom(reply.getMessage().getContent());
    return LogServiceProtoUtil.toLogStream(parseFrom.getLogStream(), this);
  }



  @Override
  public LogStream getLog(LogName name) throws IOException {
    RaftClientReply reply =
        raftClient.sendReadOnly(Message.valueOf(LogServiceProtoUtil.toGetLogRequestProto(name)
            .toByteString()));
    GetLogReplyProto parseFrom = GetLogReplyProto.parseFrom(reply.getMessage().getContent());
    return LogServiceProtoUtil.toLogStream(parseFrom.getLogStream(), this);
  }

  @Override
  public Iterator<LogStream> listLogs() throws IOException {
    RaftClientReply reply =
        raftClient
            .sendReadOnly(Message.valueOf(LogServiceProtoUtil.toListLogRequestProto().toByteString()));
    ListLogsReplyProto parseFrom = ListLogsReplyProto.parseFrom(reply.getMessage().getContent());
    List<LogStreamProto> logStremsList = parseFrom.getLogStremsList();
    return LogServiceProtoUtil.toListLogStreams(logStremsList, this).iterator();
  }

  @Override
  public void closeLog(LogName name) throws IOException {
    RaftClientReply reply =
        raftClient.send(Message.valueOf(LogServiceProtoUtil.toCloseLogRequestProto(name)
            .toByteString()));
    CloseLogReplyProto parseFrom = CloseLogReplyProto.parseFrom(reply.getMessage().getContent());
  }

  @Override
  public State getState(LogName name) throws IOException {
    RaftClientReply reply =
        raftClient.sendReadOnly(Message.valueOf(LogServiceProtoUtil.toGetStateRequestProto(name)
            .toByteString()));
    GetStateReplyProto parseFrom = GetStateReplyProto.parseFrom(reply.getMessage().getContent());
    return parseFrom.getState() == LogStreamState.OPEN ? State.OPEN : State.CLOSED;
  }

  @Override
  public void archiveLog(LogName name) throws IOException {
    RaftClientReply reply =
        raftClient.send(Message.valueOf(LogServiceProtoUtil.toArchiveLogRequestProto(name)
            .toByteString()));
    ArchiveLogReplyProto parseFrom =
        ArchiveLogReplyProto.parseFrom(reply.getMessage().getContent());
  }

  @Override
  public void deleteLog(LogName name) throws IOException {
    RaftClientReply reply =
        raftClient.send(Message.valueOf(LogServiceProtoUtil.toDeleteLogRequestProto(name)
            .toByteString()));
    DeleteLogReplyProto parseFrom = DeleteLogReplyProto.parseFrom(reply.getMessage().getContent());
  }


  @Override
  public void addRecordListener(LogName name, RecordListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean removeRecordListener(LogName name, RecordListener listener) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public LogStream createLog(LogName name, LogServiceConfiguration config) throws IOException {
    //TODO configuration
    RaftClientReply reply =
        raftClient.send(Message.valueOf(LogServiceProtoUtil.toCreateLogRequestProto(name)
            .toByteString()));
    CreateLogReplyProto parseFrom = CreateLogReplyProto.parseFrom(reply.getMessage().getContent());
    return LogServiceProtoUtil.toLogStream(parseFrom.getLogStream(), this);
  }

  @Override
  public void updateConfiguration(LogName name, LogServiceConfiguration config) {
    // TODO Auto-generated method stub

  }


  @Override
  public RaftClient getRaftClient() {
    return raftClient;
  }

  @Override
  public LogServiceConfiguration getConfiguration() {
    return config;
  }

}
