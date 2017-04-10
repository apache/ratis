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

import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.ReflectionUtils;

import java.util.Arrays;

import static org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.NOTLEADEREXCEPTION;
import static org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.STATEMACHINEEXCEPTION;

public class ClientProtoUtils {
  public static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      byte[] requestorId, byte[] replyId, long callId, boolean success) {
    return RaftRpcReplyProto.newBuilder()
        .setRequestorId(ProtoUtils.toByteString(requestorId))
        .setReplyId(ProtoUtils.toByteString(replyId))
        .setCallId(callId)
        .setSuccess(success);
  }

  public static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      byte[] requesterId, byte[] replyId, long callId) {
    return RaftRpcRequestProto.newBuilder()
        .setRequestorId(ProtoUtils.toByteString(requesterId))
        .setReplyId(ProtoUtils.toByteString(replyId))
        .setCallId(callId);
  }

  public static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    ClientId clientId = new ClientId(
        p.getRpcRequest().getRequestorId().toByteArray());
    RaftPeerId serverId = new RaftPeerId(
        p.getRpcRequest().getReplyId());
    return new RaftClientRequest(clientId, serverId,
        p.getRpcRequest().getCallId(),
        toMessage(p.getMessage()), p.getReadOnly());
  }

  public static RaftClientRequestProto toRaftClientRequestProto(
      RaftClientRequest request) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request.getClientId().toBytes(),
            request.getServerId().toBytes(), request.getCallId()))
        .setMessage(toClientMessageEntryProto(request.getMessage()))
        .setReadOnly(request.isReadOnly())
        .build();
  }

  public static RaftClientRequestProto genRaftClientRequestProto(
      ClientId clientId, RaftPeerId serverId, long callId, ByteString content,
      boolean readOnly) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(clientId.toBytes(),
            serverId.toBytes(), callId))
        .setMessage(ClientMessageEntryProto.newBuilder().setContent(content))
        .setReadOnly(readOnly)
        .build();
  }

  public static RaftClientReplyProto toRaftClientReplyProto(
      RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toBytes(),
          reply.getServerId().toBytes(), reply.getCallId(), reply.isSuccess()));
      if (reply.getMessage() != null) {
        b.setMessage(toClientMessageEntryProto(reply.getMessage()));
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
      e = new NotLeaderException(new RaftPeerId(rp.getReplyId()),
          suggestedLeader, peers);
    } else if (replyProto.getExceptionDetailsCase().equals(STATEMACHINEEXCEPTION)) {
      StateMachineExceptionProto smeProto = replyProto.getStateMachineException();
      e = wrapStateMachineException(rp.getReplyId().toStringUtf8(),
          smeProto.getExceptionClassName(), smeProto.getErrorMsg(),
          smeProto.getStacktrace());
    }
    return new RaftClientReply(new ClientId(rp.getRequestorId().toByteArray()),
        new RaftPeerId(rp.getReplyId()),
        rp.getCallId(), rp.getSuccess(), toMessage(replyProto.getMessage()), e);
  }

  private static StateMachineException wrapStateMachineException(
      String serverId, String className, String errorMsg,
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

  private static ClientMessageEntryProto toClientMessageEntryProto(Message message) {
    return ClientMessageEntryProto.newBuilder()
        .setContent(message.getContent()).build();
  }

  public static SetConfigurationRequest toSetConfigurationRequest(
      SetConfigurationRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(p.getPeersList());
    return new SetConfigurationRequest(
        new ClientId(m.getRequestorId().toByteArray()),
        new RaftPeerId(m.getReplyId()),
        p.getRpcRequest().getCallId(), peers);
  }

  public static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(
            request.getClientId().toBytes(),
            request.getServerId().toBytes(),
            request.getCallId()))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(
            Arrays.asList(request.getPeersInNewConf())))
        .build();
  }
}
