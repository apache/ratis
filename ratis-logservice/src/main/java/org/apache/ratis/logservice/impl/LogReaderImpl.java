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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.proto.LogServiceProtos.*;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogReaderImpl implements LogReader {
  public static final Logger LOG = LoggerFactory.getLogger(LogReaderImpl.class);

  /*
   * Parent log stream
   */
  private LogStream parent;
  /*
   * Raft client
   */
  private RaftClient   raftClient;
  /*
   * Log service configuration object
   */
  private LogServiceConfiguration config;

  /*
   * offset
   */
  long currentRecordId;

  public LogReaderImpl(LogStream logStream) {
    this.parent = logStream;
    this.raftClient = logStream.getLogService().getRaftClient();
    this.config = logStream.getConfiguration();
  }

  @Override
  public void seek(long recordId) throws IOException {
    this.currentRecordId = recordId;
  }

  @Override
  public ByteBuffer readNext() throws IOException {
    int num = 1;
    RaftClientReply reply =
        raftClient.sendReadOnly(Message.valueOf(LogServiceProtoUtil
            .toReadLogRequestProto(parent.getName(), currentRecordId, num)
            .toByteString()));
    ReadLogReplyProto proto = ReadLogReplyProto.parseFrom(reply.getMessage().getContent());
    if (proto.hasException()) {
      LogServiceException e = proto.getException();
      throw new IOException(e.getErrorMsg());
    }
    proto.getLogRecord(0);
    currentRecordId++;
    return ByteBuffer.wrap(proto.getLogRecord(0).toByteArray());
  }

  @Override
  public void readNext(ByteBuffer buffer) throws IOException {
    int num = 1;
    RaftClientReply reply =
        raftClient.sendReadOnly(Message.valueOf(LogServiceProtoUtil
            .toReadLogRequestProto(parent.getName(), currentRecordId, num)
            .toByteString()));
    ReadLogReplyProto proto = ReadLogReplyProto.parseFrom(reply.getMessage().getContent());
    if (proto.hasException()) {
      LogServiceException e = proto.getException();
      throw new IOException(e.getErrorMsg());
    }
    currentRecordId++;
    //TODO limits
    buffer.put(proto.getLogRecord(0).toByteArray());
  }

  @Override
  public List<ByteBuffer> readBulk(int numRecords) throws IOException {
    RaftClientReply reply =
        raftClient.sendReadOnly(Message.valueOf(LogServiceProtoUtil
            .toReadLogRequestProto(parent.getName(), currentRecordId, numRecords)
            .toByteString()));
    ReadLogReplyProto proto = ReadLogReplyProto.parseFrom(reply.getMessage().getContent());
    if (proto.hasException()) {
      LogServiceException e = proto.getException();
      throw new IOException(e.getErrorMsg());
    }
    //TODO correct current record
    currentRecordId+= numRecords;
    List<ByteBuffer> ret = new ArrayList<ByteBuffer>();
    int n = proto.getLogRecordCount();
    for(int i=0; i < n; i++) {
      ret.add(ByteBuffer.wrap(proto.getLogRecord(i).toByteArray()));
    }
    return ret;
  }

  @Override
  public int readBulk(List<ByteBuffer> buffers) throws IOException {
    RaftClientReply reply =
        raftClient.sendReadOnly(Message.valueOf(LogServiceProtoUtil
            .toReadLogRequestProto(parent.getName(), currentRecordId, buffers.size())
            .toByteString()));
    ReadLogReplyProto proto = ReadLogReplyProto.parseFrom(reply.getMessage().getContent());
    if (proto.hasException()) {
      LogServiceException e = proto.getException();
      throw new IOException(e.getErrorMsg());
    }
    //TODO correct current record
    int n = proto.getLogRecordCount();
    currentRecordId += n;
    for(int i=0; i< n; i++) {
      buffers.get(i).put(proto.getLogRecord(i).toByteArray());
    }
    return n;
  }

  @Override
  public long getPosition() {
    return currentRecordId;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

}
