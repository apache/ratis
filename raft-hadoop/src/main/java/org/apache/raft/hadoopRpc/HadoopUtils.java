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
package org.apache.raft.hadoopRpc;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.raft.proto.RaftClientProtocolProtos.RaftClientReplyProto;
import org.apache.raft.proto.RaftClientProtocolProtos.RaftClientRequestProto;
import org.apache.raft.proto.RaftClientProtocolProtos.SetConfigurationRequestProto;
import org.apache.raft.proto.RaftProtos;
import org.apache.raft.proto.RaftProtos.RaftRpcMessageProto;
import org.apache.raft.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.raft.proto.RaftServerProtocolProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftServerProtocolProtos.AppendEntriesRequestProto;
import org.apache.raft.proto.RaftServerProtocolProtos.InstallSnapshotReplyProto;
import org.apache.raft.proto.RaftServerProtocolProtos.InstallSnapshotRequestProto;
import org.apache.raft.proto.RaftServerProtocolProtos.RaftServerReplyProto;
import org.apache.raft.proto.RaftServerProtocolProtos.RaftServerRequestProto;
import org.apache.raft.proto.RaftServerProtocolProtos.RequestVoteReplyProto;
import org.apache.raft.proto.RaftServerProtocolProtos.RequestVoteRequestProto;
import org.apache.raft.protocol.NotLeaderException;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.protocol.RaftRpcMessage;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.protocol.AppendEntriesReply;
import org.apache.raft.server.protocol.AppendEntriesRequest;
import org.apache.raft.server.protocol.InstallSnapshotReply;
import org.apache.raft.server.protocol.InstallSnapshotRequest;
import org.apache.raft.server.protocol.RaftServerReply;
import org.apache.raft.server.protocol.RaftServerRequest;
import org.apache.raft.server.protocol.RequestVoteReply;
import org.apache.raft.server.protocol.RequestVoteRequest;
import org.apache.raft.server.protocol.ServerProtoUtils;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.RaftLog;
import org.apache.raft.util.ProtoUtils;
import org.apache.raft.util.RaftUtils;

import java.io.IOException;
import java.util.Arrays;

public class HadoopUtils {
  public static void setProtobufRpcEngine(
      Class<?> protocol, Configuration conf) {
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngine.class);
  }

  public static <PROTOCOL> PROTOCOL getProxy(
      Class<PROTOCOL> clazz, String addressStr, Configuration conf)
      throws IOException {
    setProtobufRpcEngine(clazz, conf);
    return RPC.getProxy(clazz, RPC.getProtocolVersion(clazz),
        RaftUtils.newInetSocketAddress(addressStr),
        UserGroupInformation.getCurrentUser(),
        conf, NetUtils.getSocketFactory(conf, clazz));
  }

  public static RaftServerRequestProto.Builder toRaftServerRequestProtoBuilder(
      RaftServerRequest request) {
    return RaftServerRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
  }

  public static RaftServerReplyProto.Builder toRaftServerReplyProtoBuilder(
      RaftServerRequestProto request, RaftServerReply reply) {
    return RaftServerReplyProto.newBuilder()
        .setRpcReply(toRaftRpcReplyProtoBuilder(
            request.getRpcRequest(), reply))
        .setTerm(reply.getTerm());
  }

  public static RequestVoteRequest toRequestVoteRequest(RequestVoteRequestProto p) {
    final RaftRpcMessageProto m = p.getServerRequest()
        .getRpcRequest().getRpcMessage();
    return new RequestVoteRequest(m.getRequestorId(), m.getReplyId(),
        p.getCandidateTerm(),
        ServerProtoUtils.toTermIndex(p.getCandidateLastEntry()));
  }

  public static RequestVoteRequestProto toRequestVoteRequestProto(
      RequestVoteRequest request) {
    final RequestVoteRequestProto.Builder b = RequestVoteRequestProto.newBuilder()
        .setServerRequest(toRaftServerRequestProtoBuilder(request))
        .setCandidateTerm(request.getCandidateTerm());
    final TermIndex candidateLastEntry = request.getCandidateLastEntry();
    if (candidateLastEntry != null) {
      b.setCandidateLastEntry(ServerProtoUtils.toTermIndexProto(candidateLastEntry));
    }
    return b.build();
  }

  public static RequestVoteReply toRequestVoteReply(RequestVoteReplyProto p) {
    final RaftServerReplyProto serverReply = p.getServerReply();
    final RaftProtos.RaftRpcReplyProto rpcReply = serverReply.getRpcReply();
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

  public static AppendEntriesRequest toAppendEntriesRequest(
      AppendEntriesRequestProto p) {
    final RaftRpcMessageProto m = p.getServerRequest()
        .getRpcRequest().getRpcMessage();
    final TermIndex previousLog = !p.hasPreviousLog()? null
        : ServerProtoUtils.toTermIndex(p.getPreviousLog());
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
      b.setPreviousLog(ServerProtoUtils.toTermIndexProto(previousLog));
    }
    return b.build();
  }

  public static AppendEntriesReply toAppendEntriesReply(AppendEntriesReplyProto p) {
    final RaftServerReplyProto serverReply = p.getServerReply();
    final RaftRpcMessageProto m = serverReply.getRpcReply()
        .getRpcMessage();
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
        .setFileDigest(ProtoUtils.toByteString(request.getFileDigest().getDigest()))
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
    final RaftProtos.RaftRpcReplyProto rpcReply = serverReply.getRpcReply();
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

  public static RaftRpcMessageProto.Builder toRaftRpcMessageProtoBuilder(
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

  public static RaftProtos.RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      RaftRpcRequestProto request, RaftRpcMessage.Reply reply) {
    return RaftProtos.RaftRpcReplyProto.newBuilder()
        .setRpcMessage(request.getRpcMessage())
        .setSuccess(reply.isSuccess());
  }

  public static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    final RaftRpcMessageProto m = p.getRpcRequest().getRpcMessage();
    return new RaftClientRequest(m.getRequestorId(), m.getReplyId(),
        ProtoUtils.toMessage(p.getMessage()));
  }

  public static RaftClientRequestProto toRaftClientRequestProto(
      RaftClientRequest request) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .setMessage(ProtoUtils.toClientMessageEntryProto(request.getMessage()))
        .build();
  }

  public static RaftClientReplyProto toRaftClientReplyProto(
      RaftRpcRequestProto request, RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(request, reply));
      b.setMessage(ProtoUtils.toClientMessageEntryProto(reply.getMessage()));
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
    final RaftProtos.RaftRpcReplyProto rp = replyProto.getRpcReply();
    final RaftRpcMessageProto rm = rp.getRpcMessage();
    NotLeaderException e = null;
    if (replyProto.getIsNotLeader()) {
      final RaftPeer suggestedLeader = replyProto.hasSuggestedLeader() ?
          ProtoUtils.toRaftPeer(replyProto.getSuggestedLeader()) : null;
      final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(
          replyProto.getPeersInConfList());
      e = new NotLeaderException(rm.getReplyId(), suggestedLeader, peers);
    }
    return new RaftClientReply(rm.getRequestorId(), rm.getReplyId(),
        rp.getSuccess(), ProtoUtils.toMessage(replyProto.getMessage()), e);
  }

  public static SetConfigurationRequest toSetConfigurationRequest(
      SetConfigurationRequestProto p) throws InvalidProtocolBufferException {
    final RaftRpcMessageProto m = p.getRpcRequest().getRpcMessage();
    final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(p.getPeersList());
    return new SetConfigurationRequest(m.getRequestorId(), m.getReplyId(), peers);
  }

  public static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(
            Arrays.asList(request.getPeersInNewConf())))
        .build();
  }
}
