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
package org.apache.ratis.client.impl;

import org.apache.ratis.protocol.*;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.ReflectionUtils;

import java.util.Arrays;

import static org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.NOTLEADEREXCEPTION;
import static org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.STATEMACHINEEXCEPTION;

public interface ClientProtoUtils {

  static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      ByteString requestorId, ByteString replyId, RaftGroupId groupId,
      long callId, boolean success) {
    return RaftRpcReplyProto.newBuilder()
        .setRequestorId(requestorId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(callId)
        .setSuccess(success);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ByteString requesterId, ByteString replyId, RaftGroupId groupId, long callId, long seqNum) {
    return RaftRpcRequestProto.newBuilder()
        .setRequestorId(requesterId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(callId)
        .setSeqNum(seqNum);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ClientId requesterId, RaftPeerId replyId, RaftGroupId groupId, long callId, long seqNum) {
    return toRaftRpcRequestProtoBuilder(
        requesterId.toByteString(), replyId.toByteString(), groupId, callId, seqNum);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      RaftClientRequest request) {
    return toRaftRpcRequestProtoBuilder(
        request.getClientId(),
        request.getServerId(),
        request.getRaftGroupId(),
        request.getCallId(),
        request.getSeqNum());
  }

  static RaftClientRequest.Type toRaftClientRequestType(RaftClientRequestProto p) {
    switch (p.getTypeCase()) {
      case WRITE:
        return RaftClientRequest.Type.valueOf(p.getWrite());
      case READ:
        return RaftClientRequest.Type.valueOf(p.getRead());
      case STALEREAD:
        return RaftClientRequest.Type.valueOf(p.getStaleRead());
      default:
        throw new IllegalArgumentException("Unexpected request type: " + p.getTypeCase()
            + " in request proto " + p);
    }
  }

  static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    final RaftClientRequest.Type type = toRaftClientRequestType(p);
    final RaftRpcRequestProto request = p.getRpcRequest();
    return new RaftClientRequest(
        ClientId.valueOf(request.getRequestorId()),
        RaftPeerId.valueOf(request.getReplyId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        request.getCallId(),
        request.getSeqNum(),
        toMessage(p.getMessage()),
        type);
  }

  static RaftClientRequestProto toRaftClientRequestProto(
      RaftClientRequest request) {
    final RaftClientRequestProto.Builder b = RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .setMessage(toClientMessageEntryProtoBuilder(request.getMessage()));

    final RaftClientRequest.Type type = request.getType();
    switch (type.getTypeCase()) {
      case WRITE:
        b.setWrite(type.getWrite());
        break;
      case READ:
        b.setRead(type.getRead());
        break;
      case STALEREAD:
        b.setStaleRead(type.getStaleRead());
        break;
      default:
        throw new IllegalArgumentException("Unexpected request type: " + request.getType()
            + " in request " + request);
    }

    return b.build();
  }

  static RaftClientRequestProto toRaftClientRequestProto(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId,
      long seqNum, ByteString content) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(
            clientId, serverId, groupId, callId, seqNum))
        .setWrite(WriteRequestTypeProto.getDefaultInstance())
        .setMessage(toClientMessageEntryProtoBuilder(content))
        .build();
  }

  static RaftClientReplyProto toRaftClientReplyProto(
      RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      if (reply.getMessage() != null) {
        b.setMessage(toClientMessageEntryProtoBuilder(reply.getMessage()));
      }
      ProtoUtils.addCommitInfos(reply.getCommitInfos(), i -> b.addCommitInfos(i));

      final NotLeaderException nle = reply.getNotLeaderException();
      final StateMachineException sme;
      if (nle != null) {
        NotLeaderExceptionProto.Builder nleBuilder =
            NotLeaderExceptionProto.newBuilder();
        final RaftPeer suggestedLeader = nle.getSuggestedLeader();
        if (suggestedLeader != null) {
          nleBuilder.setSuggestedLeader(ProtoUtils.toRaftPeerProto(suggestedLeader));
        }
        nleBuilder.addAllPeersInConf(
            ProtoUtils.toRaftPeerProtos(Arrays.asList(nle.getPeers())));
        b.setNotLeaderException(nleBuilder.build());
      } else if ((sme = reply.getStateMachineException()) != null) {
        StateMachineExceptionProto.Builder smeBuilder =
            StateMachineExceptionProto.newBuilder();
        final Throwable t = sme.getCause() != null ? sme.getCause() : sme;
        smeBuilder.setExceptionClassName(t.getClass().getName())
            .setErrorMsg(t.getMessage())
            .setStacktrace(ProtoUtils.writeObject2ByteString(t.getStackTrace()));
        b.setStateMachineException(smeBuilder.build());
      }
    }
    return b.build();
  }

  static ServerInformationReplyProto toServerInformationReplyProto(
      ServerInformationReply reply) {
    final ServerInformationReplyProto.Builder b =
        ServerInformationReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      if (reply.getRaftGroupId() != null) {
        b.setGroup(ProtoUtils.toRaftGroupProtoBuilder(reply.getGroup()));
      }
      ProtoUtils.addCommitInfos(reply.getCommitInfos(), i -> b.addCommitInfos(i));
    }
    return b.build();
  }

  static RaftClientReply toRaftClientReply(
      RaftClientReplyProto replyProto) {
    final RaftRpcReplyProto rp = replyProto.getRpcReply();
    RaftException e = null;
    if (replyProto.getExceptionDetailsCase().equals(NOTLEADEREXCEPTION)) {
      NotLeaderExceptionProto nleProto = replyProto.getNotLeaderException();
      final RaftPeer suggestedLeader = nleProto.hasSuggestedLeader() ?
          ProtoUtils.toRaftPeer(nleProto.getSuggestedLeader()) : null;
      final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(
          nleProto.getPeersInConfList());
      e = new NotLeaderException(RaftPeerId.valueOf(rp.getReplyId()),
          suggestedLeader, peers);
    } else if (replyProto.getExceptionDetailsCase().equals(STATEMACHINEEXCEPTION)) {
      StateMachineExceptionProto smeProto = replyProto.getStateMachineException();
      e = wrapStateMachineException(RaftPeerId.valueOf(rp.getReplyId()),
          smeProto.getExceptionClassName(), smeProto.getErrorMsg(),
          smeProto.getStacktrace());
    }
    ClientId clientId = ClientId.valueOf(rp.getRequestorId());
    final RaftGroupId groupId = ProtoUtils.toRaftGroupId(rp.getRaftGroupId());
    return new RaftClientReply(clientId, RaftPeerId.valueOf(rp.getReplyId()),
        groupId, rp.getCallId(), rp.getSuccess(),
        toMessage(replyProto.getMessage()), e,
        replyProto.getCommitInfosList());
  }

  static ServerInformationReply toServerInformationReply(
      ServerInformationReplyProto replyProto) {
    final RaftRpcReplyProto rp = replyProto.getRpcReply();
    ClientId clientId = ClientId.valueOf(rp.getRequestorId());
    final RaftGroupId groupId = ProtoUtils.toRaftGroupId(rp.getRaftGroupId());
    final RaftGroup raftGroup = ProtoUtils.toRaftGroup(replyProto.getGroup());
    return new ServerInformationReply(clientId, RaftPeerId.valueOf(rp.getReplyId()),
        groupId, rp.getCallId(), rp.getSuccess(),
        replyProto.getCommitInfosList(), raftGroup);
  }

  static StateMachineException wrapStateMachineException(
      RaftPeerId serverId, String className, String errorMsg,
      ByteString stackTraceBytes) {
    StateMachineException sme;
    if (className == null) {
      sme = new StateMachineException(errorMsg);
    } else {
      try {
        Class<?> clazz = Class.forName(className);
        final Exception e = ReflectionUtils.instantiateException(
            clazz.asSubclass(Exception.class), errorMsg, null);
        sme = new StateMachineException(serverId, e);
      } catch (Exception e) {
        sme = new StateMachineException(className + ": " + errorMsg);
      }
    }
    StackTraceElement[] stacktrace =
        (StackTraceElement[]) ProtoUtils.toObject(stackTraceBytes);
    sme.setStackTrace(stacktrace);
    return sme;
  }

  static Message toMessage(final ClientMessageEntryProto p) {
    return Message.valueOf(p.getContent());
  }

  static ClientMessageEntryProto.Builder toClientMessageEntryProtoBuilder(ByteString message) {
    return ClientMessageEntryProto.newBuilder().setContent(message);
  }

  static ClientMessageEntryProto.Builder toClientMessageEntryProtoBuilder(Message message) {
    return toClientMessageEntryProtoBuilder(message.getContent());
  }

  static SetConfigurationRequest toSetConfigurationRequest(
      SetConfigurationRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(p.getPeersList());
    return new SetConfigurationRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        p.getRpcRequest().getCallId(), peers);
  }

  static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(
            Arrays.asList(request.getPeersInNewConf())))
        .build();
  }

  static ReinitializeRequest toReinitializeRequest(
      ReinitializeRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new ReinitializeRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId(),
        ProtoUtils.toRaftGroup(p.getGroup()));
  }

  static ServerInformatonRequest toServerInformationRequest(
      ServerInformationRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new ServerInformatonRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId());
  }

  static ReinitializeRequestProto toReinitializeRequestProto(
      ReinitializeRequest request) {
    return ReinitializeRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .setGroup(ProtoUtils.toRaftGroupProtoBuilder(request.getGroup()))
        .build();
  }

  static ServerInformationRequestProto toServerInformationRequestProto(
      ServerInformatonRequest request) {
    return ServerInformationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .build();
  }

  static String toString(RaftClientRequestProto proto) {
    final RaftRpcRequestProto rpc = proto.getRpcRequest();
    return ClientId.valueOf(rpc.getRequestorId()) + "->" + rpc.getReplyId().toStringUtf8()
        + "#" + rpc.getCallId() + "-" + rpc.getSeqNum();
  }

  static String toString(RaftClientReplyProto proto) {
    final RaftRpcReplyProto rpc = proto.getRpcReply();
    return ClientId.valueOf(rpc.getRequestorId()) + "<-" + rpc.getReplyId().toStringUtf8()
        + "#" + rpc.getCallId();
  }
}
