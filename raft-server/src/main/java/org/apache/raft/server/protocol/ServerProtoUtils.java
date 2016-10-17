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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.raft.proto.RaftProtos;
import org.apache.raft.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.raft.proto.RaftProtos.ClientMessageEntryProto;
import org.apache.raft.proto.RaftProtos.FileChunkProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotResult;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.proto.RaftProtos.RaftClientReplyProto;
import org.apache.raft.proto.RaftProtos.RaftClientRequestProto;
import org.apache.raft.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.raft.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.raft.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.raft.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.raft.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.raft.proto.RaftProtos.TermIndexProto;
import org.apache.raft.protocol.Message;
import org.apache.raft.protocol.NotLeaderException;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.util.ProtoUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.raft.util.ProtoUtils.toByteString;

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

  public static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      String requestorId, String replyId, boolean success) {
    return RaftRpcReplyProto.newBuilder()
        .setRequestorId(requestorId).setReplyId(replyId)
        .setSuccess(success);
  }

  public static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      String requesterId, String replyId) {
    return RaftRpcRequestProto.newBuilder()
        .setRequestorId(requesterId).setReplyId(replyId);
  }

  public static RequestVoteReplyProto toRequestVoteReplyProto(
      String requestorId, String replyId, boolean success, long term,
      boolean shouldShutdown) {
    final RequestVoteReplyProto.Builder b = RequestVoteReplyProto.newBuilder();
    b.setServerReply(toRaftRpcReplyProtoBuilder(requestorId, replyId, success))
        .setTerm(term)
        .setShouldShutdown(shouldShutdown);
    return b.build();
  }

  public static RequestVoteRequestProto toRequestVoteRequestProto(
      String requestorId, String replyId, long term, TermIndex lastEntry) {
    final RequestVoteRequestProto.Builder b = RequestVoteRequestProto.newBuilder()
        .setServerRequest(toRaftRpcRequestProtoBuilder(requestorId, replyId))
        .setCandidateTerm(term);
    if (lastEntry != null) {
      b.setCandidateLastEntry(toTermIndexProto(lastEntry));
    }
    return b.build();
  }

  public static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      String requestorId, String replyId, long term,
      InstallSnapshotResult result) {
    final RaftRpcReplyProto.Builder rb = toRaftRpcReplyProtoBuilder(requestorId,
        replyId, result == InstallSnapshotResult.SUCCESS);
    final InstallSnapshotReplyProto.Builder builder = InstallSnapshotReplyProto
        .newBuilder().setServerReply(rb).setTerm(term).setResult(result);
    return builder.build();
  }

  public static InstallSnapshotRequestProto toInstallSnapshotRequestProto(
      String requestorId, String replyId, String requestId, int requestIndex,
      long term, TermIndex lastTermIndex, List<FileChunkProto> chunks,
      long totalSize, boolean done) {
    return InstallSnapshotRequestProto.newBuilder()
        .setServerRequest(toRaftRpcRequestProtoBuilder(requestorId, replyId))
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
    RaftRpcReplyProto.Builder rb = toRaftRpcReplyProtoBuilder(requestorId,
        replyId, appendResult == AppendEntriesReplyProto.AppendResult.SUCCESS);
    final AppendEntriesReplyProto.Builder b = AppendEntriesReplyProto.newBuilder();
    b.setServerReply(rb).setTerm(term).setNextIndex(nextIndex)
        .setResult(appendResult);
    return b.build();
  }

  public static AppendEntriesRequestProto toAppendEntriesRequestProto(
      String requestorId, String replyId, long leaderTerm,
      LogEntryProto[] entries, long leaderCommit, boolean initializing,
      TermIndex previous) {
    final AppendEntriesRequestProto.Builder b = AppendEntriesRequestProto.newBuilder()
        .setServerRequest(toRaftRpcRequestProtoBuilder(requestorId, replyId))
        .setLeaderTerm(leaderTerm)
        .addAllEntries(Arrays.asList(entries))
        .setLeaderCommit(leaderCommit)
        .setInitializing(initializing);

    if (previous != null) {
      b.setPreviousLog(toTermIndexProto(previous));
    }
    return b.build();
  }

  public static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    return new RaftClientRequest(p.getRpcRequest().getRequestorId(),
        p.getRpcRequest().getReplyId(), toMessage(p.getMessage()), p.getReadOnly());
  }

  public static RaftClientRequestProto toRaftClientRequestProto(
      RaftClientRequest request) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request.getRequestorId(),
            request.getReplierId()))
        .setMessage(toClientMessageEntryProto(request.getMessage()))
        .setReadOnly(request.isReadOnly())
        .build();
  }

  public static RaftClientReplyProto toRaftClientReplyProto(
      RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getRequestorId(),
          reply.getReplierId(), reply.isSuccess()));
      if (reply.getMessage() != null) {
        b.setMessage(toClientMessageEntryProto(reply.getMessage()));
      }
      if (reply.isNotLeader()) {
        b.setIsNotLeader(true);
        final RaftPeer suggestedLeader = reply.getNotLeaderException()
            .getSuggestedLeader();
        if (suggestedLeader != null) {
          b.setSuggestedLeader(ProtoUtils.toRaftPeerProto(suggestedLeader));
        }
        b.addAllPeersInConf(ProtoUtils.toRaftPeerProtos(
            Arrays.asList(reply.getNotLeaderException().getPeers())));
      }
    }
    return b.build();
  }

  public static RaftClientReply toRaftClientReply(
      RaftClientReplyProto replyProto) {
    final RaftRpcReplyProto rp = replyProto.getRpcReply();
    NotLeaderException e = null;
    if (replyProto.getIsNotLeader()) {
      final RaftPeer suggestedLeader = replyProto.hasSuggestedLeader() ?
          ProtoUtils.toRaftPeer(replyProto.getSuggestedLeader()) : null;
      final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(
          replyProto.getPeersInConfList());
      e = new NotLeaderException(rp.getReplyId(), suggestedLeader, peers);
    }
    return new RaftClientReply(rp.getRequestorId(), rp.getReplyId(),
        rp.getSuccess(), toMessage(replyProto.getMessage()), e);
  }

  public static Message toMessage(final ClientMessageEntryProto p) {
    return () -> p.getContent().toByteArray();
  }

  public static ClientMessageEntryProto toClientMessageEntryProto(Message message) {
    return ClientMessageEntryProto.newBuilder()
        .setContent(toByteString(message.getContent())).build();
  }

  public static SetConfigurationRequest toSetConfigurationRequest(
      SetConfigurationRequestProto p) throws InvalidProtocolBufferException {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(p.getPeersList());
    return new SetConfigurationRequest(m.getRequestorId(), m.getReplyId(), peers);
  }

  public static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request.getRequestorId(),
            request.getReplierId()))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(
            Arrays.asList(request.getPeersInNewConf())))
        .build();
  }
}
