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
package org.apache.raft.server.protocol;

import org.apache.hadoop.io.MD5Hash;
import org.apache.raft.proto.RaftProtos;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.proto.RaftProtos.RaftRpcMessageProto;
import org.apache.raft.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.raft.proto.RaftProtos.TermIndexProto;
import org.apache.raft.proto.RaftServerProtocolProtos.*;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.util.ProtoUtils;
import org.apache.raft.server.storage.RaftLog;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ServerProtoUtils {
  public static TermIndex toTermIndex(TermIndexProto p) {
    return p == null? null: new TermIndex(p.getTerm(), p.getIndex());
  }

  public static TermIndexProto toTermIndexProto(TermIndex ti) {
    return ti == null? null: TermIndexProto.newBuilder()
        .setTerm(ti.getTerm())
        .setIndex(ti.getIndex())
        .build();
  }

  public static TermIndex toTermIndex(LogEntryProto entry) {
    return entry == null ? null :
        new TermIndex(entry.getTerm(), entry.getIndex());
  }

  public static String toString(LogEntryProto... entries) {
    return entries == null? "null"
        : entries.length == 0 ? "[]"
        : entries.length == 1? "" + toTermIndex(entries[0])
        : "" + Arrays.stream(entries).map(ServerProtoUtils::toTermIndex)
            .collect(Collectors.toList());
  }

  public static RaftServerRequestProto.Builder toRaftServerRequestProtoBuilder(
      RaftServerRequest request) {
    return RaftServerRequestProto.newBuilder()
        .setRpcRequest(ProtoUtils.toRaftRpcRequestProtoBuilder(request));
  }

  public static RaftServerReplyProto.Builder toRaftServerReplyProtoBuilder(
      RaftServerRequestProto request, RaftServerReply reply) {
    return RaftServerReplyProto.newBuilder()
        .setRpcReply(ProtoUtils.toRaftRpcReplyProtoBuilder(
            request.getRpcRequest(), reply))
        .setTerm(reply.getTerm());
  }

  public static RequestVoteRequest toRequestVoteRequest(RequestVoteRequestProto p) {
    final RaftRpcMessageProto m = p.getServerRequest().getRpcRequest().getRpcMessage();
    return new RequestVoteRequest(m.getRequestorId(), m.getReplyId(),
        p.getCandidateTerm(), toTermIndex(p.getCandidateLastEntry()));
  }

  public static RequestVoteRequestProto toRequestVoteRequestProto(
      RequestVoteRequest request) {
    final RequestVoteRequestProto.Builder b = RequestVoteRequestProto.newBuilder()
        .setServerRequest(toRaftServerRequestProtoBuilder(request))
        .setCandidateTerm(request.getCandidateTerm());
    final TermIndex candidateLastEntry = request.getCandidateLastEntry();
    if (candidateLastEntry != null) {
      b.setCandidateLastEntry(toTermIndexProto(candidateLastEntry));
    }
    return b.build();
  }

  public static RequestVoteReply toRequestVoteReply(RequestVoteReplyProto p) {
    final RaftServerReplyProto serverReply = p.getServerReply();
    final RaftRpcReplyProto rpcReply = serverReply.getRpcReply();
    final RaftRpcMessageProto m = rpcReply.getRpcMessage();
    return new RequestVoteReply(m.getRequestorId(), m.getReplyId(),
        serverReply.getTerm(), rpcReply.getSuccess(), p.getShouldShutdown());
  }

  public static RequestVoteReplyProto toRequestVoteReplyProto(
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

  public static AppendEntriesRequest toAppendEntriesRequest(AppendEntriesRequestProto p) {
    final RaftRpcMessageProto m = p.getServerRequest().getRpcRequest().getRpcMessage();
    final TermIndex previousLog = !p.hasPreviousLog()? null
        : toTermIndex(p.getPreviousLog());
    return new AppendEntriesRequest(m.getRequestorId(), m.getReplyId(),
        p.getLeaderTerm(), previousLog,
        p.getEntriesList().toArray(RaftLog.EMPTY_LOGENTRY_ARRAY),
        p.getLeaderCommit(), p.getInitializing());
  }

  public static AppendEntriesRequestProto toAppendEntriesRequestProto(
      AppendEntriesRequest request) {
    final AppendEntriesRequestProto.Builder b = AppendEntriesRequestProto.newBuilder()
        .setServerRequest(toRaftServerRequestProtoBuilder(request))
        .setLeaderTerm(request.getLeaderTerm())
        .addAllEntries(Arrays.asList(request.getEntries()))
        .setLeaderCommit(request.getLeaderCommit())
        .setInitializing(request.isInitializing());

    final TermIndex previousLog = request.getPreviousLog();
    if (previousLog != null) {
      b.setPreviousLog(toTermIndexProto(previousLog));
    }
    return b.build();
  }

  public static AppendEntriesReply toAppendEntriesReply(AppendEntriesReplyProto p) {
    final RaftServerReplyProto serverReply = p.getServerReply();
    final RaftRpcMessageProto m = serverReply.getRpcReply().getRpcMessage();
    return new AppendEntriesReply(m.getRequestorId(), m.getReplyId(),
        serverReply.getTerm(), p.getNextIndex(), toAppendResult(p.getResult()));
  }

  public static AppendEntriesReplyProto toAppendEntriesReplyProto(
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

  public static AppendEntriesReplyProto.AppendResult toAppendResult(
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

  public static AppendEntriesReply.AppendResult toAppendResult(
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

  public static InstallSnapshotRequestProto toInstallSnapshotRequestProto(
      InstallSnapshotRequest request) {
    InstallSnapshotRequestProto.Builder builder = InstallSnapshotRequestProto
        .newBuilder()
        .setServerRequest(toRaftServerRequestProtoBuilder(request))
        .setLeaderTerm(request.getLeaderTerm())
        .setLastIncludedIndex(request.getLastIncludedIndex())
        .setLastIncludedTerm(request.getLastIncludedTerm())
        .setChunk(request.getChunk())
        .setTotalSize(request.getTotalSize());
    return builder.build();
  }

  public static InstallSnapshotRequest toInstallSnapshotRequest(
      InstallSnapshotRequestProto requestProto) {
    RaftRpcMessageProto rpcMessage = requestProto.getServerRequest()
        .getRpcRequest().getRpcMessage();
    MD5Hash digest = new MD5Hash(requestProto.getFileDigest().toByteArray());
    return new InstallSnapshotRequest(rpcMessage.getRequestorId(),
        rpcMessage.getReplyId(), requestProto.getLeaderTerm(),
        requestProto.getLastIncludedIndex(), requestProto.getLastIncludedTerm(),
        requestProto.getChunk(), requestProto.getTotalSize(), digest);
  }

  public static InstallSnapshotReply toInstallSnapshotReply(
      InstallSnapshotReplyProto replyProto) {
    final RaftServerReplyProto serverReply = replyProto.getServerReply();
    final RaftRpcReplyProto rpcReply = serverReply.getRpcReply();
    final RaftRpcMessageProto m = rpcReply.getRpcMessage();
    return new InstallSnapshotReply(m.getRequestorId(), m.getReplyId(),
        serverReply.getTerm(), replyProto.getResult());
  }

  public static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      InstallSnapshotRequestProto request, InstallSnapshotReply reply) {
    final RaftServerReplyProto.Builder serverReplyBuilder
        = toRaftServerReplyProtoBuilder(request.getServerRequest(), reply);
    final InstallSnapshotReplyProto.Builder builder = InstallSnapshotReplyProto
        .newBuilder().setServerReply(serverReplyBuilder);
    return builder.build();
  }

  public static RaftProtos.RaftConfigurationProto toRaftConfigurationProto(
      RaftConfiguration conf) {
    return RaftProtos.RaftConfigurationProto.newBuilder()
        .addAllPeers(ProtoUtils.toRaftPeerProtos(conf.getPeersInConf()))
        .addAllOldPeers(ProtoUtils.toRaftPeerProtos(conf.getPeersInOldConf()))
        .build();
  }

  public static RaftConfiguration toRaftConfiguration(
      long index, RaftProtos.RaftConfigurationProto proto) {
    final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(proto.getPeersList());
    if (proto.getOldPeersCount() > 0) {
      final RaftPeer[] oldPeers = ProtoUtils.toRaftPeerArray(proto.getPeersList());
      return RaftConfiguration.composeOldNewConf(peers, oldPeers, index);
    } else {
      return RaftConfiguration.composeConf(peers, index);
    }
  }

  public static LogEntryProto toLogEntryProto(
      RaftConfiguration conf, long term, long index) {
    return LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setType(LogEntryProto.Type.CONFIGURATION)
        .setConfigurationEntry(toRaftConfigurationProto(conf))
        .build();
  }
}
