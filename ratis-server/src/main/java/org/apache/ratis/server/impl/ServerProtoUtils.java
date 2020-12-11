/*
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

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.util.ProtoUtils;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** Server proto utilities for internal use. */
public interface ServerProtoUtils {
  static TermIndex toTermIndex(TermIndexProto p) {
    return p == null? null: TermIndex.newTermIndex(p.getTerm(), p.getIndex());
  }

  static TermIndex toTermIndex(LogEntryProto entry) {
    return entry == null ? null :
        TermIndex.newTermIndex(entry.getTerm(), entry.getIndex());
  }

  static String toTermIndexString(TermIndexProto proto) {
    return TermIndex.toString(proto.getTerm(), proto.getIndex());
  }

  static String toShortString(List<LogEntryProto> entries) {
    return entries.size() == 0? "<empty>"
        : "size=" + entries.size() + ", first=" + LogProtoUtils.toLogEntryString(entries.get(0));
  }
  static String toString(AppendEntriesRequestProto proto) {
    if (proto == null) {
      return null;
    }
    return ProtoUtils.toString(proto.getServerRequest()) + "-t" + proto.getLeaderTerm()
        + ", previous=" + toTermIndexString(proto.getPreviousLog())
        + ", leaderCommit=" + proto.getLeaderCommit()
        + ", initializing? " + proto.getInitializing()
        + ", entries: " + toShortString(proto.getEntriesList());
  }
  static String toString(AppendEntriesReplyProto reply) {
    if (reply == null) {
      return null;
    }
    return ProtoUtils.toString(reply.getServerReply()) + "," + reply.getResult()
        + ",nextIndex:" + reply.getNextIndex() + ",term:" + reply.getTerm()
        + ",followerCommit:" + reply.getFollowerCommit();
  }

  static String toString(RequestVoteReplyProto proto) {
    if (proto == null) {
      return null;
    }
    return ProtoUtils.toString(proto.getServerReply()) + "-t" + proto.getTerm();
  }

  static String toString(InstallSnapshotRequestProto proto) {
    if (proto == null) {
      return null;
    }
    final String s;
    switch (proto.getInstallSnapshotRequestBodyCase()) {
      case SNAPSHOTCHUNK:
        final InstallSnapshotRequestProto.SnapshotChunkProto chunk = proto.getSnapshotChunk();
        s = "chunk:" + chunk.getRequestId() + "," + chunk.getRequestIndex();
        break;
      case NOTIFICATION:
        final InstallSnapshotRequestProto.NotificationProto notification = proto.getNotification();
        s = "notify:" + toTermIndexString(notification.getFirstAvailableTermIndex());
        break;
      default:
        throw new IllegalStateException("Unexpected body case in " + proto);
    }
    return ProtoUtils.toString(proto.getServerRequest()) + "-t" + proto.getLeaderTerm() + "," + s;
  }

  static String toString(InstallSnapshotReplyProto proto) {
    if (proto == null) {
      return null;
    }
    final String s;
    switch (proto.getInstallSnapshotReplyBodyCase()) {
      case REQUESTINDEX:
        s = ",requestIndex=" + proto.getRequestIndex();
        break;
      case SNAPSHOTINDEX:
        s = ",snapshotIndex=" + proto.getSnapshotIndex();
        break;
      default:
        s = ""; // result is not SUCCESS
    }
    return ProtoUtils.toString(proto.getServerReply()) + "-t" + proto.getTerm() + "," + proto.getResult() + s;
  }

  static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      RaftPeerId requestorId, RaftGroupMemberId replyId, boolean success) {
    return ClientProtoUtils.toRaftRpcReplyProtoBuilder(
        requestorId.toByteString(), replyId.getPeerId().toByteString(), replyId.getGroupId(), null, success);
  }

  static RequestVoteReplyProto toRequestVoteReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId, boolean success, long term, boolean shouldShutdown) {
    return RequestVoteReplyProto.newBuilder()
        .setServerReply(toRaftRpcReplyProtoBuilder(requestorId, replyId, success))
        .setTerm(term)
        .setShouldShutdown(shouldShutdown)
        .build();
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      RaftGroupMemberId requestorId, RaftPeerId replyId) {
    return ClientProtoUtils.toRaftRpcRequestProtoBuilder(
        requestorId.getPeerId().toByteString(), replyId.toByteString(), requestorId.getGroupId(), null, null);
  }

  static RequestVoteRequestProto toRequestVoteRequestProto(
      RaftGroupMemberId requestorId, RaftPeerId replyId, long term, TermIndex lastEntry) {
    final RequestVoteRequestProto.Builder b = RequestVoteRequestProto.newBuilder()
        .setServerRequest(toRaftRpcRequestProtoBuilder(requestorId, replyId))
        .setCandidateTerm(term);
    Optional.ofNullable(lastEntry).map(TermIndex::toProto).ifPresent(b::setCandidateLastEntry);
    return b.build();
  }

  static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId,
      long currentTerm, int requestIndex, InstallSnapshotResult result) {
    final RaftRpcReplyProto.Builder rb = toRaftRpcReplyProtoBuilder(requestorId,
        replyId, result == InstallSnapshotResult.SUCCESS);
    final InstallSnapshotReplyProto.Builder builder = InstallSnapshotReplyProto
        .newBuilder().setServerReply(rb).setTerm(currentTerm).setResult(result)
        .setRequestIndex(requestIndex);
    return builder.build();
  }

  static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId,
      long currentTerm, InstallSnapshotResult result, long installedSnapshotIndex) {
    final RaftRpcReplyProto.Builder rb = toRaftRpcReplyProtoBuilder(requestorId,
        replyId, result == InstallSnapshotResult.SUCCESS);
    final InstallSnapshotReplyProto.Builder builder = InstallSnapshotReplyProto
        .newBuilder().setServerReply(rb).setTerm(currentTerm).setResult(result);
    if (installedSnapshotIndex > 0) {
      builder.setSnapshotIndex(installedSnapshotIndex);
    }
    return builder.build();
  }

  static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId,
      InstallSnapshotResult result) {
    final RaftRpcReplyProto.Builder rb = toRaftRpcReplyProtoBuilder(requestorId,
        replyId, result == InstallSnapshotResult.SUCCESS);
    final InstallSnapshotReplyProto.Builder builder = InstallSnapshotReplyProto
        .newBuilder().setServerReply(rb).setResult(result);
    return builder.build();
  }

  @SuppressWarnings("parameternumber")
  static AppendEntriesReplyProto toAppendEntriesReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId, long term,
      long followerCommit, long nextIndex, AppendResult result, long callId,
      long matchIndex, boolean isHeartbeat) {
    RaftRpcReplyProto.Builder rpcReply = toRaftRpcReplyProtoBuilder(
        requestorId, replyId, result == AppendResult.SUCCESS)
        .setCallId(callId);
    return AppendEntriesReplyProto.newBuilder()
        .setServerReply(rpcReply)
        .setTerm(term)
        .setNextIndex(nextIndex)
        .setMatchIndex(matchIndex)
        .setFollowerCommit(followerCommit)
        .setResult(result)
        .setIsHearbeat(isHeartbeat)
        .build();
  }

  @SuppressWarnings("checkstyle:parameternumber")
  static AppendEntriesRequestProto toAppendEntriesRequestProto(
      RaftGroupMemberId requestorId, RaftPeerId replyId, long leaderTerm,
      List<LogEntryProto> entries, long leaderCommit, boolean initializing,
      TermIndex previous, Collection<CommitInfoProto> commitInfos, long callId) {
    RaftRpcRequestProto.Builder rpcRequest = toRaftRpcRequestProtoBuilder(requestorId, replyId)
        .setCallId(callId);
    final AppendEntriesRequestProto.Builder b = AppendEntriesRequestProto
        .newBuilder()
        .setServerRequest(rpcRequest)
        .setLeaderTerm(leaderTerm)
        .setLeaderCommit(leaderCommit)
        .setInitializing(initializing);
    if (entries != null && !entries.isEmpty()) {
      b.addAllEntries(entries);
    }

    Optional.ofNullable(previous).map(TermIndex::toProto).ifPresent(b::setPreviousLog);
    ProtoUtils.addCommitInfos(commitInfos, b::addCommitInfos);
    return b.build();
  }

  static ServerRpcProto toServerRpcProto(RaftPeer peer, long delay) {
    if (peer == null) {
      // if no peer information return empty
      return ServerRpcProto.getDefaultInstance();
    }
    return ServerRpcProto.newBuilder()
        .setId(peer.getRaftPeerProto())
        .setLastRpcElapsedTimeMs(delay)
        .build();
  }
}
