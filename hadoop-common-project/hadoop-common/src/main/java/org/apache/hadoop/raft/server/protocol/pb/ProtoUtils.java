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
package org.apache.hadoop.raft.server.protocol.pb;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.raft.proto.RaftProtos.*;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.AppendEntriesReplyProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.AppendEntriesRequestProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.RequestVoteReplyProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.RequestVoteRequestProto;
import org.apache.hadoop.raft.protocol.RaftRpcMessage;
import org.apache.hadoop.raft.server.protocol.*;
import org.apache.hadoop.raft.server.storage.RaftLog;

import java.io.IOException;
import java.util.Arrays;

public class ProtoUtils {
  static TermIndex toTermIndex(TermIndexProto p) {
    return new TermIndex(p.getTerm(), p.getIndex());
  }

  static TermIndexProto toTermIndexProto(TermIndex ti) {
    return TermIndexProto.newBuilder()
        .setTerm(ti.getTerm())
        .setIndex(ti.getIndex())
        .build();
  }

  static RaftRpcMessageProto.Builder toRaftRpcMessageProtoBuilder(RaftRpcMessage m) {
    return RaftRpcMessageProto.newBuilder()
        .setRequestorId(m.getRequestorId())
        .setReplyId(m.getReplierId());
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      RaftRpcMessage.Request request) {
    return RaftRpcRequestProto.newBuilder().setRpcMessage(
        toRaftRpcMessageProtoBuilder(request));
  }

  static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      RaftRpcRequestProto request, RaftRpcMessage.Reply reply) {
    return RaftRpcReplyProto.newBuilder()
        .setRpcMessage(request.getRpcMessage())
        .setSuccess(reply.isSuccess());
  }

  static RaftServerRequestProto.Builder toRaftServerRequestProtoBuilder(
      RaftServerRequest request) {
    return RaftServerRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
  }

  static RaftServerReplyProto.Builder toRaftServerReplyProtoBuilder(
      RaftServerRequestProto request, RaftServerReply reply) {
    return RaftServerReplyProto.newBuilder()
        .setRpcReply(toRaftRpcReplyProtoBuilder(request.getRpcRequest(), reply))
        .setTerm(reply.getTerm());
  }

  static RequestVoteRequest toRequestVoteRequest(RequestVoteRequestProto p) {
    final RaftRpcMessageProto m = p.getServerRequest().getRpcRequest().getRpcMessage();
    return new RequestVoteRequest(m.getRequestorId(), m.getReplyId(),
        p.getCandidateTerm(), toTermIndex(p.getCandidateLastEntry()));
  }

  static RequestVoteRequestProto toRequestVoteRequestProto(
      RequestVoteRequest request) {
    return RequestVoteRequestProto.newBuilder()
        .setServerRequest(toRaftServerRequestProtoBuilder(request))
        .setCandidateTerm(request.getCandidateTerm())
        .setCandidateLastEntry(toTermIndexProto(request.getCandidateLastEntry()))
        .build();
  }

  static RequestVoteReply toRequestVoteReply(RequestVoteReplyProto p) {
    final RaftServerReplyProto serverReply = p.getServerReply();
    final RaftRpcReplyProto rpcReply = serverReply.getRpcReply();
    final RaftRpcMessageProto m = rpcReply.getRpcMessage();
    return new RequestVoteReply(m.getRequestorId(), m.getReplyId(),
        serverReply.getTerm(), rpcReply.getSuccess(), p.getShouldShutdown());
  }

  static RequestVoteReplyProto toRequestVoteReplyProto(
      RequestVoteRequestProto request, RequestVoteReply reply) {
    final RequestVoteReplyProto.Builder b = RequestVoteReplyProto.newBuilder();
    if (reply != null) {
      final RaftServerReplyProto.Builder serverReplyBuilder
          = toRaftServerReplyProtoBuilder(request.getServerRequest(), reply);
      b.setServerReply(serverReplyBuilder)
       .setShouldShutdown(reply.shouldShutdown());
    }
    return b.build();
  }

  static AppendEntriesRequest toAppendEntriesRequest(AppendEntriesRequestProto p) {
    final RaftRpcMessageProto m = p.getServerRequest().getRpcRequest().getRpcMessage();
    return new AppendEntriesRequest(m.getRequestorId(), m.getReplyId(),
        p.getLeaderTerm(), toTermIndex(p.getPreviousLog()),
        p.getEntriesList().toArray(RaftLog.EMPTY_LOGENTRY_ARRAY),
        p.getLeaderCommit(), p.getInitializing());
  }

  static AppendEntriesRequestProto toAppendEntriesRequestProto(
      AppendEntriesRequest request) {
    return AppendEntriesRequestProto.newBuilder()
        .setServerRequest(toRaftServerRequestProtoBuilder(request))
        .setLeaderTerm(request.getLeaderTerm())
        .setPreviousLog(toTermIndexProto(request.getPreviousLog()))
        .addAllEntries(Arrays.asList(request.getEntries()))
        .setLeaderCommit(request.getLeaderCommit())
        .setInitializing(request.isInitializing())
        .build();
  }

  static AppendEntriesReply toAppendEntriesReply(AppendEntriesReplyProto p) {
    final RaftServerReplyProto serverReply = p.getServerReply();
    final RaftRpcMessageProto m = serverReply.getRpcReply().getRpcMessage();
    return new AppendEntriesReply(m.getRequestorId(), m.getReplyId(),
        serverReply.getTerm(), p.getNextIndex(), toAppendResult(p.getResult()));
  }

  static AppendEntriesReplyProto toAppendEntriesReplyProto(
      AppendEntriesRequestProto request, AppendEntriesReply reply) {
    final AppendEntriesReplyProto.Builder b = AppendEntriesReplyProto.newBuilder();
    if (reply != null) {
      final RaftServerReplyProto.Builder serverReplyBuilder
          = toRaftServerReplyProtoBuilder(request.getServerRequest(), reply);
      b.setServerReply(serverReplyBuilder)
          .setNextIndex(reply.getNextIndex())
          .setResult(toAppendResult(reply.getResult()));
    }
    return b.build();
  }

  static AppendEntriesReplyProto.AppendResult toAppendResult(
      AppendEntriesReply.AppendResult result) {
    switch (result) {
      case SUCCESS:
        return AppendEntriesReplyProto.AppendResult.SUCCESS;
      case NOT_LEADER:
        return AppendEntriesReplyProto.AppendResult.NOT_LEADER;
      case INCONSISTENCY:
        return AppendEntriesReplyProto.AppendResult.INCONSISTENCY;
      default:
        throw new IllegalStateException("Unexpected value " + result);
    }
  }

  static AppendEntriesReply.AppendResult toAppendResult(
      AppendEntriesReplyProto.AppendResult result) {
    switch (result) {
      case SUCCESS:
        return AppendEntriesReply.AppendResult.SUCCESS;
      case NOT_LEADER:
        return AppendEntriesReply.AppendResult.NOT_LEADER;
      case INCONSISTENCY:
        return AppendEntriesReply.AppendResult.INCONSISTENCY;
      default:
        throw new IllegalStateException("Unexpected value " + result);
    }
  }

  static IOException toIOException(ServiceException se) {
    final Throwable t = se.getCause();
    if (t == null) {
      return new IOException(se);
    }
    return t instanceof IOException? (IOException)t : new IOException(se);
  }
}
