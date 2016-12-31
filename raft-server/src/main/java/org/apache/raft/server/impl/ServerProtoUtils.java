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
package org.apache.raft.server.impl;

import org.apache.raft.client.impl.ClientProtoUtils;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.shaded.proto.RaftProtos.*;
import org.apache.raft.util.ProtoUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.raft.server.RaftServerConstants.DEFAULT_SEQNUM;
import static org.apache.raft.shaded.proto.RaftProtos.AppendEntriesReplyProto.AppendResult.SUCCESS;

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

  public static String toString(AppendEntriesReplyProto reply) {
    return toString(reply.getServerReply()) + "," + reply.getResult()
        + ",nextIndex:" + reply.getNextIndex() + ",term:" + reply.getTerm();
  }

  private static String toString(RaftRpcReplyProto reply) {
    return reply.getRequestorId() + "->" + reply.getReplyId() + ","
        + reply.getSuccess();
  }

  public static RaftConfigurationProto toRaftConfigurationProto(
      RaftConfiguration conf) {
    return RaftConfigurationProto.newBuilder()
        .addAllPeers(ProtoUtils.toRaftPeerProtos(conf.getPeersInConf()))
        .addAllOldPeers(ProtoUtils.toRaftPeerProtos(conf.getPeersInOldConf()))
        .build();
  }

  public static RaftConfiguration toRaftConfiguration(
      long index, RaftConfigurationProto proto) {
    final RaftConfiguration.Builder b = RaftConfiguration.newBuilder()
        .setConf(ProtoUtils.toRaftPeerArray(proto.getPeersList()))
        .setLogEntryIndex(index);
    if (proto.getOldPeersCount() > 0) {
      b.setOldConf(ProtoUtils.toRaftPeerArray(proto.getOldPeersList()));
    }
    return b.build();
  }

  public static LogEntryProto toLogEntryProto(
      RaftConfiguration conf, long term, long index) {
    return LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setConfigurationEntry(toRaftConfigurationProto(conf))
        .build();
  }

  public static RequestVoteReplyProto toRequestVoteReplyProto(
      String requestorId, String replyId, boolean success, long term,
      boolean shouldShutdown) {
    final RequestVoteReplyProto.Builder b = RequestVoteReplyProto.newBuilder();
    b.setServerReply(ClientProtoUtils.toRaftRpcReplyProtoBuilder(requestorId, replyId,
        DEFAULT_SEQNUM, success))
        .setTerm(term)
        .setShouldShutdown(shouldShutdown);
    return b.build();
  }

  public static RequestVoteRequestProto toRequestVoteRequestProto(
      String requestorId, String replyId, long term, TermIndex lastEntry) {
    final RequestVoteRequestProto.Builder b = RequestVoteRequestProto.newBuilder()
        .setServerRequest(
            ClientProtoUtils.toRaftRpcRequestProtoBuilder(requestorId, replyId, DEFAULT_SEQNUM))
        .setCandidateTerm(term);
    if (lastEntry != null) {
      b.setCandidateLastEntry(toTermIndexProto(lastEntry));
    }
    return b.build();
  }

  public static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      String requestorId, String replyId, long term, int requestIndex,
      InstallSnapshotResult result) {
    final RaftRpcReplyProto.Builder rb = ClientProtoUtils.toRaftRpcReplyProtoBuilder(requestorId,
        replyId, DEFAULT_SEQNUM, result == InstallSnapshotResult.SUCCESS);
    final InstallSnapshotReplyProto.Builder builder = InstallSnapshotReplyProto
        .newBuilder().setServerReply(rb).setTerm(term).setResult(result)
        .setRequestIndex(requestIndex);
    return builder.build();
  }

  public static InstallSnapshotRequestProto toInstallSnapshotRequestProto(
      String requestorId, String replyId, String requestId, int requestIndex,
      long term, TermIndex lastTermIndex, List<FileChunkProto> chunks,
      long totalSize, boolean done) {
    return InstallSnapshotRequestProto.newBuilder()
        .setServerRequest(
            ClientProtoUtils.toRaftRpcRequestProtoBuilder(requestorId, replyId, DEFAULT_SEQNUM))
        .setRequestId(requestId)
        .setRequestIndex(requestIndex)
        // .setRaftConfiguration()  TODO: save and pass RaftConfiguration
        .setLeaderTerm(term)
        .setTermIndex(toTermIndexProto(lastTermIndex))
        .addAllFileChunks(chunks)
        .setTotalSize(totalSize)
        .setDone(done).build();
  }

  public static AppendEntriesReplyProto toAppendEntriesReplyProto(
      String requestorId, String replyId, long term,
      long nextIndex, AppendEntriesReplyProto.AppendResult appendResult) {
    RaftRpcReplyProto.Builder rb = ClientProtoUtils.toRaftRpcReplyProtoBuilder(requestorId,
        replyId, DEFAULT_SEQNUM, appendResult == SUCCESS);
    final AppendEntriesReplyProto.Builder b = AppendEntriesReplyProto.newBuilder();
    b.setServerReply(rb).setTerm(term).setNextIndex(nextIndex)
        .setResult(appendResult);
    return b.build();
  }

  public static AppendEntriesRequestProto toAppendEntriesRequestProto(
      String requestorId, String replyId, long leaderTerm,
      List<LogEntryProto> entries, long leaderCommit, boolean initializing,
      TermIndex previous) {
    final AppendEntriesRequestProto.Builder b = AppendEntriesRequestProto
        .newBuilder()
        .setServerRequest(
            ClientProtoUtils.toRaftRpcRequestProtoBuilder(requestorId, replyId, DEFAULT_SEQNUM))
        .setLeaderTerm(leaderTerm)
        .setLeaderCommit(leaderCommit)
        .setInitializing(initializing);
    if (entries != null && !entries.isEmpty()) {
      b.addAllEntries(entries);
    }

    if (previous != null) {
      b.setPreviousLog(toTermIndexProto(previous));
    }
    return b.build();
  }

}
