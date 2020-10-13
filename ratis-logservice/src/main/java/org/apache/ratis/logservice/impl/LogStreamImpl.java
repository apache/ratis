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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.api.RecordListener;
import org.apache.ratis.logservice.proto.LogServiceProtos;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogLastCommittedIndexReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogLengthReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogSizeReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogStartIndexReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogServiceException;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStreamImpl implements LogStream {
  public static final Logger LOG = LoggerFactory.getLogger(LogStreamImpl.class);

  /*
   * Log stream listeners
   */
  private List<RecordListener> listeners;
  /*
   * Log stream name
   */
  private LogName name;
  /*
   * Parent log service instance
   */
  private RaftClient raftClient;
  /*
   * Log stream configuration
   */
  private LogServiceConfiguration config;
  /*
   * State
   */
  private LogStream.State state;

  /*
   * Length
   */
  private long length;


  public LogStreamImpl(LogName name, RaftClient raftClient) {
    this.raftClient = raftClient;
    this.name = name;
    this.config = LogServiceConfiguration.create();
    init();
  }

  public LogStreamImpl(LogName name, RaftClient raftClient, LogServiceConfiguration config) {
    this.raftClient = raftClient;
    this.name = name;
    this.config = config;
    init();
  }

  private void init() {
    // TODO create new state machine. etc
    this.state = State.OPEN;
    this.listeners = Collections.synchronizedList(new ArrayList<RecordListener>());
  }

  @Override
  public LogName getName() {
    return name;
  }

  @Override public State getState() throws IOException {
    RaftClientReply reply = raftClient.io().sendReadOnly(
        Message.valueOf(LogServiceProtoUtil.toGetStateRequestProto(name).toByteString()));
    LogServiceProtos.GetStateReplyProto proto =
        LogServiceProtos.GetStateReplyProto.parseFrom(reply.getMessage().getContent());
    return State.valueOf(proto.getState().name());
  }

  @Override
  public long getSize() throws IOException{
    RaftClientReply reply = raftClient
        .io().sendReadOnly(Message.valueOf(LogServiceProtoUtil
            .toGetSizeRequestProto(name).toByteString()));
    if (reply.getException() != null) {
      throw new IOException(reply.getException());
    }

    GetLogSizeReplyProto proto =
        GetLogSizeReplyProto.parseFrom(reply.getMessage().getContent());
    if (proto.hasException()) {
      LogServiceException e = proto.getException();
      throw new IOException(e.getErrorMsg());
    }
    return proto.getSize();
  }

  @Override
  public long getLength() throws IOException {
    RaftClientReply reply = raftClient
        .io().sendReadOnly(Message.valueOf(LogServiceProtoUtil
            .toGetLengthRequestProto(name).toByteString()));
    if (reply.getException() != null) {
      throw new IOException(reply.getException());
    }

    GetLogLengthReplyProto proto =
        GetLogLengthReplyProto.parseFrom(reply.getMessage().getContent());
    if (proto.hasException()) {
      LogServiceException e = proto.getException();
      throw new IOException(e.getErrorMsg());
    }
    return proto.getLength();
  }

  @Override
  public LogReader createReader() throws IOException{
    return new LogReaderImpl(this);
  }

  @Override
  public LogWriter createWriter() {
    return new LogWriterImpl(this);
  }

  @Override
  public long getLastRecordId() throws IOException {
    try {
      RaftClientReply reply = raftClient
          .io().sendReadOnly(Message.valueOf(LogServiceProtoUtil
              .toGetLastCommittedIndexRequestProto(name).toByteString()));
      if (reply.getException() != null) {
        throw new IOException(reply.getException());
      }

      GetLogLastCommittedIndexReplyProto proto =
          GetLogLastCommittedIndexReplyProto.parseFrom(reply.getMessage().getContent());
      if (proto.hasException()) {
        LogServiceException e = proto.getException();
        throw new IOException(e.getErrorMsg());
      }
      return proto.getLastCommittedIndex();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getStartRecordId() throws IOException {
    try {
      RaftClientReply reply = raftClient
          .io().sendReadOnly(Message.valueOf(LogServiceProtoUtil
              .toGetStartIndexProto(name).toByteString()));
      if (reply.getException() != null) {
        throw new IOException(reply.getException());
      }

      GetLogStartIndexReplyProto proto =
          GetLogStartIndexReplyProto.parseFrom(reply.getMessage().getContent());
      if (proto.hasException()) {
        LogServiceException e = proto.getException();
        throw new IOException(e.getErrorMsg());
      }
      return proto.getStartIndex();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Collection<RecordListener> getRecordListeners() {
    return listeners;
  }

  @Override
  public LogServiceConfiguration getConfiguration() {
    return config;
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    raftClient.close();
    state = State.CLOSED;
  }

  @Override
  public void addRecordListener(RecordListener listener) {
    synchronized (listeners) {
      if (!listeners.contains(listener)) {
        listeners.add(listener);
      }
    }
  }

  @Override
  public boolean removeRecordListener(RecordListener listener) {
    return listeners.remove(listener);
  }

  @Override
  public RaftClient getRaftClient() {
    return raftClient;
  }

}
