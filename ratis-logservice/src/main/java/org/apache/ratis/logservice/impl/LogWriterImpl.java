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
import java.util.concurrent.ExecutionException;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.logservice.proto.LogServiceProtos.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogWriterImpl implements LogWriter {
  public static final Logger LOG = LoggerFactory.getLogger(LogWriterImpl.class);

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
   * TODO: usage of custom configuration
   */
  private LogServiceConfiguration config;

  public LogWriterImpl(LogStream logStream) {
    this.parent = logStream;
    this.raftClient = logStream.getRaftClient();
    this.config = logStream.getConfiguration();
  }

  @Override
  public long write(ByteBuffer data) throws IOException {
    List<ByteBuffer> list = new ArrayList<ByteBuffer>();
    list.add(data);
     return write(list);
   }

   @Override
   public long write(List<ByteBuffer> list) throws IOException {

     try {
       RaftClientReply reply = raftClient.send(Message.valueOf(
         LogServiceProtoUtil.toAppendBBEntryLogRequestProto(parent.getName(), list).toByteString()));
       AppendLogEntryReplyProto proto =
           AppendLogEntryReplyProto.parseFrom(reply.getMessage().getContent());
       if (proto.hasException()) {
         LogServiceException e = proto.getException();
         throw new IOException(e.getErrorMsg());
       }
       List<Long> ids = proto.getRecordIdList();
       // The above call Always returns one id (regardless of a batch size)
       return ids.get(0);
     } catch (Exception e) {
       throw new IOException(e);
   }
 }

 @Override
 public long sync() throws IOException {
     try {
       RaftClientReply reply = raftClient.send(Message
           .valueOf(LogServiceProtoUtil.toSyncLogRequestProto(parent.getName()).toByteString()));

       SyncLogReplyProto proto = SyncLogReplyProto.parseFrom(reply.getMessage().getContent());
       if (proto.hasException()) {
         LogServiceException e = proto.getException();
         throw new IOException(e.getErrorMsg());
       }
       return proto.getLastRecordId();
     } catch (Exception e) {
       throw new IOException(e);
   }
  }
  @Override
  public void close() throws IOException {
    //TODO
  }

}
