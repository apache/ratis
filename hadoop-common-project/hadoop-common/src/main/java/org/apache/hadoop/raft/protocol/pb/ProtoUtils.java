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
package org.apache.hadoop.raft.protocol.pb;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.raft.proto.RaftProtos.*;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.*;
import org.apache.hadoop.raft.protocol.RaftRpcMessage;
import org.apache.hadoop.raft.server.protocol.*;
import org.apache.hadoop.raft.server.storage.RaftLog;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ProtoUtils {
  static RaftRpcMessageProto.Builder toRaftRpcMessageProtoBuilder(
      RaftRpcMessage m) {
    return RaftRpcMessageProto.newBuilder()
        .setRequestorId(m.getRequestorId())
        .setReplyId(m.getReplierId());
  }

  public static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      RaftRpcMessage.Request request) {
    return RaftRpcRequestProto.newBuilder().setRpcMessage(
        toRaftRpcMessageProtoBuilder(request));
  }

  public static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      RaftRpcRequestProto request, RaftRpcMessage.Reply reply) {
    return RaftRpcReplyProto.newBuilder()
        .setRpcMessage(request.getRpcMessage())
        .setSuccess(reply.isSuccess());
  }

  public static IOException toIOException(ServiceException se) {
    final Throwable t = se.getCause();
    if (t == null) {
      return new IOException(se);
    }
    return t instanceof IOException? (IOException)t : new IOException(se);
  }
}
