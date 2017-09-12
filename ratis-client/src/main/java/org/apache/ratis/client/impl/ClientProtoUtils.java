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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.NOTLEADEREXCEPTION;
import static org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.STATEMACHINEEXCEPTION;

public class ClientProtoUtils {

  public static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      ByteString requestorId, ByteString replyId, RaftGroupId groupId,
      long callId, boolean success) {
    return RaftRpcReplyProto.newBuilder()
        .setRequestorId(requestorId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(callId)
        .setSuccess(success);
  }

  public static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ByteString requesterId, ByteString replyId, RaftGroupId groupId, long callId) {
    return RaftRpcRequestProto.newBuilder()
        .setRequestorId(requesterId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(callId);
  }

  public static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ClientId requesterId, RaftPeerId replyId, RaftGroupId groupId, long callId) {
    return toRaftRpcRequestProtoBuilder(
        requesterId.toByteString(), replyId.toByteString(), groupId, callId);
  }

  private static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      RaftClientRequest request) {
    return toRaftRpcRequestProtoBuilder(
        request.getClientId(),
        request.getServerId(),
        request.getRaftGroupId(),
        request.getCallId());
  }

  public static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    final RaftRpcRequestProto request = p.getRpcRequest();
    return new RaftClientRequest(
        ClientId.valueOf(request.getRequestorId()),
        RaftPeerId.valueOf(request.getReplyId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        request.getCallId(),
        toMessage(p.getMessage()), p.getReadOnly());
  }

  public static RaftClientRequestProto toRaftClientRequestProto(
      RaftClientRequest request) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .setMessage(toClientMessageEntryProtoBuilder(request.getMessage()))
        .setReadOnly(request.isReadOnly())
        .build();
  }

  public static RaftClientRequestProto toRaftClientRequestProto(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId,
      ByteString content, boolean readOnly) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(
            clientId, serverId, groupId, callId))
        .setMessage(toClientMessageEntryProtoBuilder(content))
        .setReadOnly(readOnly)
        .build();
  }

  public static RaftClientReplyProto toRaftClientReplyProto(
      RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      if (reply.getMessage() != null) {
        b.setMessage(toClientMessageEntryProtoBuilder(reply.getMessage()));
      }
      if (reply.isNotLeader()) {
        NotLeaderException nle = reply.getNotLeaderException();
        NotLeaderExceptionProto.Builder nleBuilder =
            NotLeaderExceptionProto.newBuilder();
        final RaftPeer suggestedLeader = nle.getSuggestedLeader();
        if (suggestedLeader != null) {
          nleBuilder.setSuggestedLeader(ProtoUtils.toRaftPeerProto(suggestedLeader));
        }
        nleBuilder.addAllPeersInConf(
            ProtoUtils.toRaftPeerProtos(Arrays.asList(nle.getPeers())));
        b.setNotLeaderException(nleBuilder.build());
      } else if (reply.hasStateMachineException()) {
        StateMachineException sme = reply.getStateMachineException();
        StateMachineExceptionProto.Builder smeBuilder =
            StateMachineExceptionProto.newBuilder();
        final Throwable t = sme.getCause() != null ? sme.getCause() : sme;
        smeBuilder.setExceptionClassName(t.getClass().getName())
            .setErrorMsg(t.getMessage())
            .setStacktrace(ProtoUtils.toByteString(t.getStackTrace()));
        b.setStateMachineException(smeBuilder.build());
      }
    }
    return b.build();
  }

  public static ServerInformationReplyProto toServerInformationReplyProto(
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
    }
    return b.build();
  }

  public static RaftClientReply toRaftClientReply(
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
        toMessage(replyProto.getMessage()), e);
  }

  public static ServerInformationReply toServerInformationReply(
      ServerInformationReplyProto replyProto) {
    final RaftRpcReplyProto rp = replyProto.getRpcReply();
    ClientId clientId = ClientId.valueOf(rp.getRequestorId());
    final RaftGroupId groupId = ProtoUtils.toRaftGroupId(rp.getRaftGroupId());
    final RaftGroup raftGroup = ProtoUtils.toRaftGroup(replyProto.getGroup());
    return new ServerInformationReply(clientId, RaftPeerId.valueOf(rp.getReplyId()),
        groupId, rp.getCallId(), rp.getSuccess(), null,
        null, raftGroup);
  }

  private static StateMachineException wrapStateMachineException(
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

  private static Message toMessage(final ClientMessageEntryProto p) {
    return p::getContent;
  }

  private static ClientMessageEntryProto.Builder toClientMessageEntryProtoBuilder(ByteString message) {
    return ClientMessageEntryProto.newBuilder().setContent(message);
  }

  private static ClientMessageEntryProto.Builder toClientMessageEntryProtoBuilder(Message message) {
    return toClientMessageEntryProtoBuilder(message.getContent());
  }

  public static SetConfigurationRequest toSetConfigurationRequest(
      SetConfigurationRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(p.getPeersList());
    return new SetConfigurationRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        p.getRpcRequest().getCallId(), peers);
  }

  public static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(
            Arrays.asList(request.getPeersInNewConf())))
        .build();
  }

  public static ReinitializeRequest toReinitializeRequest(
      ReinitializeRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new ReinitializeRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId(),
        ProtoUtils.toRaftGroup(p.getGroup()));
  }

  public static ServerInformatonRequest toServerInformationRequest(
      ServerInformationRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new ServerInformatonRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId());
  }

  public static ReinitializeRequestProto toReinitializeRequestProto(
      ReinitializeRequest request) {
    return ReinitializeRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .setGroup(ProtoUtils.toRaftGroupProtoBuilder(request.getGroup()))
        .build();
  }

  public static ServerInformationRequestProto toServerInformationRequestProto(
      ServerInformatonRequest request) {
    return ServerInformationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .build();
  }
}
