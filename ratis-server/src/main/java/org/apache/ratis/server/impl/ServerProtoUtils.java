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
package org.apache.ratis.server.impl;

import static org.apache.ratis.server.impl.RaftServerConstants.DEFAULT_CALLID;
import static org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto.AppendResult.SUCCESS;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.shaded.proto.RaftProtos;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.shaded.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.TermIndexProto;
import org.apache.ratis.util.ProtoUtils;


/** Server proto utilities for internal use. */
public class ServerProtoUtils {
  public static TermIndex toTermIndex(TermIndexProto p) {
    return p == null? null: TermIndex.newTermIndex(p.getTerm(), p.getIndex());
  }

  public static TermIndexProto toTermIndexProto(TermIndex ti) {
    return ti == null? null: TermIndexProto.newBuilder()
        .setTerm(ti.getTerm())
        .setIndex(ti.getIndex())
        .build();
  }

  public static TermIndex toTermIndex(LogEntryProto entry) {
    return entry == null ? null :
        TermIndex.newTermIndex(entry.getTerm(), entry.getIndex());
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
    return reply.getRequestorId().toStringUtf8() + "->"
        + reply.getReplyId().toStringUtf8() + "," + reply.getSuccess();
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
      RaftPeerId requestorId, RaftPeerId replyId, boolean success, long term,
      boolean shouldShutdown) {
    final RequestVoteReplyProto.Builder b = RequestVoteReplyProto.newBuilder();
    b.setServerReply(ClientProtoUtils.toRaftRpcReplyProtoBuilder(
        requestorId.toBytes(), replyId.toBytes(), DEFAULT_CALLID, success))
        .setTerm(term)
        .setShouldShutdown(shouldShutdown);
    return b.build();
  }

  public static RequestVoteRequestProto toRequestVoteRequestProto(
      RaftPeerId requestorId, RaftPeerId replyId, long term, TermIndex lastEntry) {
    RaftProtos.RaftRpcRequestProto.Builder rpb = ClientProtoUtils
        .toRaftRpcRequestProtoBuilder(requestorId.toBytes(), replyId.toBytes(), DEFAULT_CALLID);
    final RequestVoteRequestProto.Builder b = RequestVoteRequestProto.newBuilder()
        .setServerRequest(rpb)
        .setCandidateTerm(term);
    if (lastEntry != null) {
      b.setCandidateLastEntry(toTermIndexProto(lastEntry));
    }
    return b.build();
  }

  public static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      RaftPeerId requestorId, RaftPeerId replyId, long term, int requestIndex,
      InstallSnapshotResult result) {
    final RaftRpcReplyProto.Builder rb = ClientProtoUtils.toRaftRpcReplyProtoBuilder(requestorId.toBytes(),
        replyId.toBytes(), DEFAULT_CALLID, result == InstallSnapshotResult.SUCCESS);
    final InstallSnapshotReplyProto.Builder builder = InstallSnapshotReplyProto
        .newBuilder().setServerReply(rb).setTerm(term).setResult(result)
        .setRequestIndex(requestIndex);
    return builder.build();
  }

  public static InstallSnapshotRequestProto toInstallSnapshotRequestProto(
      RaftPeerId requestorId, RaftPeerId replyId, String requestId, int requestIndex,
      long term, TermIndex lastTermIndex, List<FileChunkProto> chunks,
      long totalSize, boolean done) {
    return InstallSnapshotRequestProto.newBuilder()
        .setServerRequest(
            ClientProtoUtils.toRaftRpcRequestProtoBuilder(requestorId.toBytes(),
                replyId.toBytes(), DEFAULT_CALLID))
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
      RaftPeerId requestorId, RaftPeerId replyId, long term,
      long nextIndex, AppendEntriesReplyProto.AppendResult appendResult) {
    RaftRpcReplyProto.Builder rb = ClientProtoUtils.toRaftRpcReplyProtoBuilder(requestorId.toBytes(),
        replyId.toBytes(), DEFAULT_CALLID, appendResult == SUCCESS);
    final AppendEntriesReplyProto.Builder b = AppendEntriesReplyProto.newBuilder();
    b.setServerReply(rb).setTerm(term).setNextIndex(nextIndex)
        .setResult(appendResult);
    return b.build();
  }

  public static AppendEntriesRequestProto toAppendEntriesRequestProto(
      RaftPeerId requestorId, RaftPeerId replyId, long leaderTerm,
      List<LogEntryProto> entries, long leaderCommit, boolean initializing,
      TermIndex previous) {
    final AppendEntriesRequestProto.Builder b = AppendEntriesRequestProto
        .newBuilder()
        .setServerRequest(
            ClientProtoUtils.toRaftRpcRequestProtoBuilder(requestorId.toBytes(),
                replyId.toBytes(), DEFAULT_CALLID))
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
