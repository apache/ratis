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
package org.apache.ratis.client.impl;

import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.proto.RaftProtos.AlreadyClosedExceptionProto;
import org.apache.ratis.proto.RaftProtos.ClientMessageEntryProto;
import org.apache.ratis.proto.RaftProtos.GroupAddRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupListReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupListRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupRemoveRequestProto;
import org.apache.ratis.proto.RaftProtos.LeaderElectionManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.LeaderElectionPauseRequestProto;
import org.apache.ratis.proto.RaftProtos.LeaderElectionResumeRequestProto;
import org.apache.ratis.proto.RaftProtos.LeaderNotReadyExceptionProto;
import org.apache.ratis.proto.RaftProtos.NotLeaderExceptionProto;
import org.apache.ratis.proto.RaftProtos.NotReplicatedExceptionProto;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RouteProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.proto.RaftProtos.SlidingWindowEntry;
import org.apache.ratis.proto.RaftProtos.SnapshotCreateRequestProto;
import org.apache.ratis.proto.RaftProtos.SnapshotManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.StateMachineExceptionProto;
import org.apache.ratis.proto.RaftProtos.TransferLeadershipRequestProto;
import org.apache.ratis.proto.RaftProtos.WriteRequestTypeProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.LeaderElectionManagementRequest;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.DataStreamException;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.ReflectionUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.ALREADYCLOSEDEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.DATASTREAMEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.LEADERNOTREADYEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.LEADERSTEPPINGDOWNEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.NOTLEADEREXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.NOTREPLICATEDEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.READEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.READINDEXEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.STATEMACHINEEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.TRANSFERLEADERSHIPEXCEPTION;

public interface ClientProtoUtils {

  static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      ByteString requestorId, ByteString replyId, RaftGroupId groupId, Long callId, boolean success) {
    return RaftRpcReplyProto.newBuilder()
        .setRequestorId(requestorId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(Optional.ofNullable(callId).orElseGet(CallId::getDefault))
        .setSuccess(success);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(RaftGroupMemberId requestorId, RaftPeerId replyId) {
    return toRaftRpcRequestProtoBuilder(requestorId.getPeerId().toByteString(),
        replyId.toByteString(), requestorId.getGroupId(), null, false, null, null, 0);
  }

  @SuppressWarnings("parameternumber")
  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ByteString requesterId, ByteString replyId, RaftGroupId groupId, Long callId, boolean toLeader,
      SlidingWindowEntry slidingWindowEntry, RoutingTable routingTable, long timeoutMs) {
    if (slidingWindowEntry == null) {
      slidingWindowEntry = SlidingWindowEntry.getDefaultInstance();
    }

    RaftRpcRequestProto.Builder b = RaftRpcRequestProto.newBuilder()
        .setRequestorId(requesterId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(Optional.ofNullable(callId).orElseGet(CallId::getDefault))
        .setToLeader(toLeader)
        .setSlidingWindowEntry(slidingWindowEntry)
        .setTimeoutMs(timeoutMs);

    if (routingTable != null) {
      b.setRoutingTable(routingTable.toProto());
    }

    return b;
  }

  @SuppressWarnings("parameternumber")
  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ClientId requesterId, RaftPeerId replyId, RaftGroupId groupId, long callId, boolean toLeader,
      SlidingWindowEntry slidingWindowEntry, RoutingTable routingTable, long timeoutMs) {
    return toRaftRpcRequestProtoBuilder(
        requesterId.toByteString(), replyId.toByteString(), groupId, callId, toLeader, slidingWindowEntry, routingTable,
        timeoutMs);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      RaftClientRequest request) {
    return toRaftRpcRequestProtoBuilder(
        request.getClientId(),
        request.getServerId(),
        request.getRaftGroupId(),
        request.getCallId(),
        request.isToLeader(),
        request.getSlidingWindowEntry(),
        request.getRoutingTable(),
        request.getTimeoutMs());
  }

  static RaftClientRequest.Type toRaftClientRequestType(RaftClientRequestProto p) {
    switch (p.getTypeCase()) {
      case WRITE:
        return RaftClientRequest.Type.valueOf(p.getWrite());
      case DATASTREAM:
        return RaftClientRequest.Type.valueOf(p.getDataStream());
      case FORWARD:
        return RaftClientRequest.Type.valueOf(p.getForward());
      case MESSAGESTREAM:
        return RaftClientRequest.Type.valueOf(p.getMessageStream());
      case READ:
        return RaftClientRequest.Type.valueOf(p.getRead());
      case STALEREAD:
        return RaftClientRequest.Type.valueOf(p.getStaleRead());
      case WATCH:
        return RaftClientRequest.Type.valueOf(p.getWatch());
      default:
        throw new IllegalArgumentException("Unexpected request type: " + p.getTypeCase()
            + " in request proto " + p);
    }
  }

  static RoutingTable getRoutingTable(RaftRpcRequestProto p) {
    if (!p.hasRoutingTable()) {
      return null;
    }

    RoutingTable.Builder builder = RoutingTable.newBuilder();
    for (RouteProto routeProto : p.getRoutingTable().getRoutesList()) {
      RaftPeerId from = RaftPeerId.valueOf(routeProto.getPeerId().getId());
      List<RaftPeerId> to = routeProto.getSuccessorsList().stream()
          .map(v -> RaftPeerId.valueOf(v.getId())).collect(Collectors.toList());
      builder.addSuccessors(from, to);
    }

    return builder.build();
  }

  static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    final RaftClientRequest.Type type = toRaftClientRequestType(p);
    final RaftRpcRequestProto request = p.getRpcRequest();

    final RaftClientRequest.Builder b = RaftClientRequest.newBuilder();

    final RaftPeerId perrId = RaftPeerId.valueOf(request.getReplyId());
    if (request.getToLeader()) {
      b.setLeaderId(perrId);
    } else {
      b.setServerId(perrId);
    }
    return b.setClientId(ClientId.valueOf(request.getRequestorId()))
        .setGroupId(ProtoUtils.toRaftGroupId(request.getRaftGroupId()))
        .setCallId(request.getCallId())
        .setMessage(toMessage(p.getMessage()))
        .setType(type)
        .setSlidingWindowEntry(request.getSlidingWindowEntry())
        .setRoutingTable(getRoutingTable(request))
        .setTimeoutMs(request.getTimeoutMs())
        .build();
  }

  static ByteBuffer toRaftClientRequestProtoByteBuffer(RaftClientRequest request) {
    return toRaftClientRequestProto(request).toByteString().asReadOnlyByteBuffer();
  }

  static RaftClientRequestProto toRaftClientRequestProto(RaftClientRequest request) {
    final RaftClientRequestProto.Builder b = RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
    if (request.getMessage() != null) {
      b.setMessage(toClientMessageEntryProtoBuilder(request.getMessage()));
    }

    final RaftClientRequest.Type type = request.getType();
    switch (type.getTypeCase()) {
      case WRITE:
        b.setWrite(type.getWrite());
        break;
      case DATASTREAM:
        b.setDataStream(type.getDataStream());
        break;
      case FORWARD:
        b.setForward(type.getForward());
        break;
      case MESSAGESTREAM:
        b.setMessageStream(type.getMessageStream());
        break;
      case READ:
        b.setRead(type.getRead());
        break;
      case STALEREAD:
        b.setStaleRead(type.getStaleRead());
        break;
      case WATCH:
        b.setWatch(type.getWatch());
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
            clientId, serverId, groupId, callId, false, ProtoUtils.toSlidingWindowEntry(seqNum, false), null, 0))
        .setWrite(WriteRequestTypeProto.getDefaultInstance())
        .setMessage(toClientMessageEntryProtoBuilder(content))
        .build();
  }

  static StateMachineExceptionProto.Builder toStateMachineExceptionProtoBuilder(StateMachineException e) {
    final Throwable t = e.getCause() != null? e.getCause(): e;
    return StateMachineExceptionProto.newBuilder()
        .setExceptionClassName(t.getClass().getName())
        .setErrorMsg(t.getMessage())
        .setStacktrace(ProtoUtils.writeObject2ByteString(t.getStackTrace()));
  }

  static AlreadyClosedExceptionProto.Builder toAlreadyClosedExceptionProtoBuilder(AlreadyClosedException ace) {
    final Throwable t = ace.getCause() != null ? ace.getCause() : ace;
    return AlreadyClosedExceptionProto.newBuilder()
        .setExceptionClassName(t.getClass().getName())
        .setErrorMsg(ace.getMessage())
        .setStacktrace(ProtoUtils.writeObject2ByteString(ace.getStackTrace()));
  }

  static RaftClientReplyProto toRaftClientReplyProto(RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      b.setLogIndex(reply.getLogIndex());
      if (reply.getMessage() != null) {
        b.setMessage(toClientMessageEntryProtoBuilder(reply.getMessage()));
      }
      b.addAllCommitInfos(reply.getCommitInfos());

      final NotLeaderException nle = reply.getNotLeaderException();
      if (nle != null) {
        NotLeaderExceptionProto.Builder nleBuilder =
            NotLeaderExceptionProto.newBuilder();
        final RaftPeer suggestedLeader = nle.getSuggestedLeader();
        if (suggestedLeader != null) {
          nleBuilder.setSuggestedLeader(suggestedLeader.getRaftPeerProto());
        }
        nleBuilder.addAllPeersInConf(ProtoUtils.toRaftPeerProtos(nle.getPeers()));
        b.setNotLeaderException(nleBuilder.build());
      }

      final NotReplicatedException nre = reply.getNotReplicatedException();
      if (nre != null) {
        final NotReplicatedExceptionProto.Builder nreBuilder = NotReplicatedExceptionProto.newBuilder()
            .setCallId(nre.getCallId())
            .setReplication(nre.getRequiredReplication())
            .setLogIndex(nre.getLogIndex());
        b.setNotReplicatedException(nreBuilder);
      }

      Optional.ofNullable(reply.getLeaderNotReadyException())
          .map(e -> LeaderNotReadyExceptionProto.newBuilder().setServerId(e.getRaftGroupMemberIdProto()))
          .ifPresent(b::setLeaderNotReadyException);

      Optional.ofNullable(reply.getStateMachineException())
          .map(ClientProtoUtils::toStateMachineExceptionProtoBuilder)
          .ifPresent(b::setStateMachineException);

      Optional.ofNullable(reply.getDataStreamException())
          .map(ProtoUtils::toThrowableProto)
          .ifPresent(b::setDataStreamException);

      Optional.ofNullable(reply.getAlreadyClosedException())
          .map(ClientProtoUtils::toAlreadyClosedExceptionProtoBuilder)
          .ifPresent(b::setAlreadyClosedException);

      Optional.ofNullable(reply.getLeaderSteppingDownException())
          .map(ProtoUtils::toThrowableProto)
          .ifPresent(b::setLeaderSteppingDownException);

      Optional.ofNullable(reply.getTransferLeadershipException())
          .map(ProtoUtils::toThrowableProto)
          .ifPresent(b::setTransferLeadershipException);

      Optional.ofNullable(reply.getReadException())
          .map(ProtoUtils::toThrowableProto)
          .ifPresent(b::setReadException);

      Optional.ofNullable(reply.getReadIndexException())
          .map(ProtoUtils::toThrowableProto)
          .ifPresent(b::setReadIndexException);

      final RaftClientReplyProto serialized = b.build();
      final RaftException e = reply.getException();
      if (e != null) {
        final RaftClientReply deserialized = toRaftClientReply(serialized);
        if (!Optional.ofNullable(deserialized.getException())
            .map(Object::getClass).filter(e.getClass()::equals).isPresent()) {
          throw new AssertionError("Corruption while serializing reply= " + reply
              + " but serialized=" + serialized + " and deserialized=" + deserialized, e);
        }
      }
      return serialized;
    }
    return b.build();
  }

  static GroupListReplyProto toGroupListReplyProto(
      GroupListReply reply) {
    final GroupListReplyProto.Builder b =
        GroupListReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      if (reply.getGroupIds() != null) {
        reply.getGroupIds().forEach(groupId -> b.addGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId)));
      }
    }
    return b.build();
  }

  static GroupInfoReplyProto toGroupInfoReplyProto(GroupInfoReply reply) {
    final GroupInfoReplyProto.Builder b =
        GroupInfoReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      if (reply.getRaftGroupId() != null) {
        b.setGroup(ProtoUtils.toRaftGroupProtoBuilder(reply.getGroup()));
        b.setIsRaftStorageHealthy(reply.isRaftStorageHealthy());
        b.setRole(reply.getRoleInfoProto());
        b.addAllCommitInfos(reply.getCommitInfos());
      }
    }
    return b.build();
  }

  static RaftClientReply getRaftClientReply(DataStreamReply reply) {
    if (!(reply instanceof DataStreamReplyByteBuffer)) {
      throw new IllegalStateException("Unexpected " + reply.getClass() + ": reply is " + reply);
    }
    try {
      return toRaftClientReply(((DataStreamReplyByteBuffer) reply).slice());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("Failed to getRaftClientReply from " + reply, e);
    }
  }

  static RaftClientReply toRaftClientReply(ByteBuffer buffer) throws InvalidProtocolBufferException {
    return toRaftClientReply(RaftClientReplyProto.parseFrom(buffer));
  }

  static RaftClientReply toRaftClientReply(RaftClientReplyProto replyProto) {
    final RaftRpcReplyProto rp = replyProto.getRpcReply();
    final RaftGroupMemberId serverMemberId = ProtoUtils.toRaftGroupMemberId(rp.getReplyId(), rp.getRaftGroupId());

    final RaftException e;
    if (replyProto.getExceptionDetailsCase().equals(NOTLEADEREXCEPTION)) {
      NotLeaderExceptionProto nleProto = replyProto.getNotLeaderException();
      final RaftPeer suggestedLeader = nleProto.hasSuggestedLeader() ?
          ProtoUtils.toRaftPeer(nleProto.getSuggestedLeader()) : null;
      final List<RaftPeer> peers = ProtoUtils.toRaftPeers(nleProto.getPeersInConfList());
      e = new NotLeaderException(serverMemberId, suggestedLeader, peers);
    } else if (replyProto.getExceptionDetailsCase() == NOTREPLICATEDEXCEPTION) {
      final NotReplicatedExceptionProto nre = replyProto.getNotReplicatedException();
      e = new NotReplicatedException(nre.getCallId(), nre.getReplication(), nre.getLogIndex());
    } else if (replyProto.getExceptionDetailsCase().equals(STATEMACHINEEXCEPTION)) {
      e = toStateMachineException(serverMemberId, replyProto.getStateMachineException());
    } else if (replyProto.getExceptionDetailsCase().equals(DATASTREAMEXCEPTION)) {
      e = ProtoUtils.toThrowable(replyProto.getDataStreamException(), DataStreamException.class);
    } else if (replyProto.getExceptionDetailsCase().equals(LEADERNOTREADYEXCEPTION)) {
      LeaderNotReadyExceptionProto lnreProto = replyProto.getLeaderNotReadyException();
      e = new LeaderNotReadyException(ProtoUtils.toRaftGroupMemberId(lnreProto.getServerId()));
    } else if (replyProto.getExceptionDetailsCase().equals(ALREADYCLOSEDEXCEPTION)) {
      e = toAlreadyClosedException(replyProto.getAlreadyClosedException());
    } else if (replyProto.getExceptionDetailsCase().equals(LEADERSTEPPINGDOWNEXCEPTION)) {
      e = ProtoUtils.toThrowable(replyProto.getLeaderSteppingDownException(), LeaderSteppingDownException.class);
    } else if (replyProto.getExceptionDetailsCase().equals(TRANSFERLEADERSHIPEXCEPTION)) {
      e = ProtoUtils.toThrowable(replyProto.getTransferLeadershipException(), TransferLeadershipException.class);
    } else if (replyProto.getExceptionDetailsCase().equals(READEXCEPTION)) {
      e = ProtoUtils.toThrowable(replyProto.getReadException(), ReadException.class);
    } else if (replyProto.getExceptionDetailsCase().equals(READINDEXEXCEPTION)) {
      e = ProtoUtils.toThrowable(replyProto.getReadIndexException(), ReadIndexException.class);
    } else {
      e = null;
    }

    return RaftClientReply.newBuilder()
        .setClientId(ClientId.valueOf(rp.getRequestorId()))
        .setServerId(serverMemberId)
        .setCallId(rp.getCallId())
        .setSuccess(rp.getSuccess())
        .setMessage(toMessage(replyProto.getMessage()))
        .setException(e)
        .setLogIndex(replyProto.getLogIndex())
        .setCommitInfos(replyProto.getCommitInfosList())
        .build();
  }

  static StateMachineException toStateMachineException(RaftGroupMemberId memberId, StateMachineExceptionProto proto) {
    return toStateMachineException(memberId,
        proto.getExceptionClassName(),
        proto.getErrorMsg(),
        proto.getStacktrace());
  }

  static StateMachineException toStateMachineException(RaftGroupMemberId memberId,
      String className, String errorMsg, ByteString stackTraceBytes) {
    StateMachineException sme;
    if (className == null) {
      sme = new StateMachineException(errorMsg);
    } else {
      try {
        final Class<?> clazz = Class.forName(className);
        final Exception e = ReflectionUtils.instantiateException(clazz.asSubclass(Exception.class), errorMsg);
        sme = new StateMachineException(memberId, e);
      } catch (Exception e) {
        sme = new StateMachineException(className + ": " + errorMsg);
      }
    }
    final StackTraceElement[] stacktrace = (StackTraceElement[]) ProtoUtils.toObject(stackTraceBytes);
    sme.setStackTrace(stacktrace);
    return sme;
  }

  static AlreadyClosedException toAlreadyClosedException(AlreadyClosedExceptionProto proto) {
    return toAlreadyClosedException(
        proto.getExceptionClassName(),
        proto.getErrorMsg(),
        proto.getStacktrace());
  }

  static AlreadyClosedException toAlreadyClosedException(
      String className, String errorMsg, ByteString stackTraceBytes) {
    AlreadyClosedException ace;
    if (className == null) {
      ace = new AlreadyClosedException(errorMsg);
    } else {
      try {
        Class<?> clazz = Class.forName(className);
        final Exception e = ReflectionUtils.instantiateException(clazz.asSubclass(Exception.class), errorMsg);
        ace = new AlreadyClosedException(errorMsg, e);
      } catch (Exception e) {
        ace = new AlreadyClosedException(className + ": " + errorMsg);
      }
    }
    StackTraceElement[] stacktrace =
        (StackTraceElement[]) ProtoUtils.toObject(stackTraceBytes);
    ace.setStackTrace(stacktrace);
    return ace;
  }

  static GroupListReply toGroupListReply(GroupListReplyProto replyProto) {
    final RaftRpcReplyProto rpc = replyProto.getRpcReply();
    final List<RaftGroupId> groupIds = replyProto.getGroupIdList().stream()
        .map(ProtoUtils::toRaftGroupId)
        .collect(Collectors.toList());
    return new GroupListReply(ClientId.valueOf(rpc.getRequestorId()),
        RaftPeerId.valueOf(rpc.getReplyId()),
        ProtoUtils.toRaftGroupId(rpc.getRaftGroupId()),
        rpc.getCallId(),
        groupIds);
  }

  static GroupInfoReply toGroupInfoReply(GroupInfoReplyProto replyProto) {
    final RaftRpcReplyProto rpc = replyProto.getRpcReply();
    return new GroupInfoReply(ClientId.valueOf(rpc.getRequestorId()),
        RaftPeerId.valueOf(rpc.getReplyId()),
        ProtoUtils.toRaftGroupId(rpc.getRaftGroupId()),
        rpc.getCallId(),
        replyProto.getCommitInfosList(),
        ProtoUtils.toRaftGroup(replyProto.getGroup()),
        replyProto.getRole(),
        replyProto.getIsRaftStorageHealthy(),
        replyProto.hasConf()? replyProto.getConf(): null);
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
    final SetConfigurationRequest.Arguments arguments = SetConfigurationRequest.Arguments.newBuilder()
        .setServersInNewConf(ProtoUtils.toRaftPeers(p.getPeersList()))
        .setListenersInNewConf(ProtoUtils.toRaftPeers(p.getListenersList()))
        .setServersInCurrentConf(ProtoUtils.toRaftPeers(p.getCurrentPeersList()))
        .setListenersInCurrentConf(ProtoUtils.toRaftPeers(p.getCurrentListenersList()))
        .setMode(toSetConfigurationMode(p.getMode()))
        .build();
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new SetConfigurationRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        p.getRpcRequest().getCallId(), arguments);
  }

  static SetConfigurationRequest.Mode toSetConfigurationMode(
      SetConfigurationRequestProto.Mode p) {
    switch (p) {
      case SET_UNCONDITIONALLY:
        return SetConfigurationRequest.Mode.SET_UNCONDITIONALLY;
      case ADD:
        return SetConfigurationRequest.Mode.ADD;
      case COMPARE_AND_SET:
        return SetConfigurationRequest.Mode.COMPARE_AND_SET;
      default:
        throw new IllegalArgumentException("Unexpected mode " + p);
    }
  }

  static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    final SetConfigurationRequest.Arguments arguments = request.getArguments();
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(arguments.getPeersInNewConf(RaftPeerRole.FOLLOWER)))
        .addAllListeners(ProtoUtils.toRaftPeerProtos(arguments.getPeersInNewConf(RaftPeerRole.LISTENER)))
        .addAllCurrentPeers(ProtoUtils.toRaftPeerProtos(arguments.getServersInCurrentConf()))
        .addAllCurrentListeners(ProtoUtils.toRaftPeerProtos(arguments.getListenersInCurrentConf()))
        .setMode(SetConfigurationRequestProto.Mode.valueOf(arguments.getMode().name()))
        .build();
  }

  static TransferLeadershipRequest toTransferLeadershipRequest(
      TransferLeadershipRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final RaftPeerId newLeader = p.hasNewLeader()? ProtoUtils.toRaftPeer(p.getNewLeader()).getId(): null;
    return new TransferLeadershipRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        p.getRpcRequest().getCallId(),
        newLeader,
        m.getTimeoutMs());
  }

  static TransferLeadershipRequestProto toTransferLeadershipRequestProto(
      TransferLeadershipRequest request) {
    final TransferLeadershipRequestProto.Builder b = TransferLeadershipRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
    Optional.ofNullable(request.getNewLeader())
        .map(l -> RaftPeer.newBuilder().setId(l).build())
        .map(RaftPeer::getRaftPeerProto)
        .ifPresent(b::setNewLeader);
    return b.build();
  }

  static GroupManagementRequest toGroupManagementRequest(GroupManagementRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final ClientId clientId = ClientId.valueOf(m.getRequestorId());
    final RaftPeerId serverId = RaftPeerId.valueOf(m.getReplyId());
    switch(p.getOpCase()) {
      case GROUPADD:
        return GroupManagementRequest.newAdd(clientId, serverId, m.getCallId(),
            ProtoUtils.toRaftGroup(p.getGroupAdd().getGroup()));
      case GROUPREMOVE:
        final GroupRemoveRequestProto remove = p.getGroupRemove();
        return GroupManagementRequest.newRemove(clientId, serverId, m.getCallId(),
            ProtoUtils.toRaftGroupId(remove.getGroupId()),
            remove.getDeleteDirectory(), remove.getRenameDirectory());
      default:
        throw new IllegalArgumentException("Unexpected op " + p.getOpCase() + " in " + p);
    }
  }

  static GroupInfoRequest toGroupInfoRequest(
      GroupInfoRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new GroupInfoRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId());
  }

  static GroupListRequest toGroupListRequest(
      GroupListRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new GroupListRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId());
  }


  static GroupManagementRequestProto toGroupManagementRequestProto(GroupManagementRequest request) {
    final GroupManagementRequestProto.Builder b = GroupManagementRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
    final GroupManagementRequest.Add add = request.getAdd();
    if (add != null) {
      b.setGroupAdd(GroupAddRequestProto.newBuilder().setGroup(
          ProtoUtils.toRaftGroupProtoBuilder(add.getGroup())).build());
    }
    final GroupManagementRequest.Remove remove = request.getRemove();
    if (remove != null) {
      b.setGroupRemove(GroupRemoveRequestProto.newBuilder()
          .setGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(remove.getGroupId()))
          .setDeleteDirectory(remove.isDeleteDirectory())
          .setRenameDirectory(remove.isRenameDirectory())
          .build());
    }
    return b.build();
  }

  static SnapshotManagementRequest toSnapshotManagementRequest(SnapshotManagementRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final ClientId clientId = ClientId.valueOf(m.getRequestorId());
    final RaftPeerId serverId = RaftPeerId.valueOf(m.getReplyId());
    switch(p.getOpCase()) {
      case CREATE:
        return SnapshotManagementRequest.newCreate(clientId, serverId,
            ProtoUtils.toRaftGroupId(m.getRaftGroupId()), m.getCallId(), m.getTimeoutMs());
      default:
        throw new IllegalArgumentException("Unexpected op " + p.getOpCase() + " in " + p);
    }
  }

  static SnapshotManagementRequestProto toSnapshotManagementRequestProto(
      SnapshotManagementRequest request) {
    final SnapshotManagementRequestProto.Builder b = SnapshotManagementRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
    final SnapshotManagementRequest.Create create = request.getCreate();
    if (create != null) {
      b.setCreate(SnapshotCreateRequestProto.newBuilder().build());
    }
    return b.build();
  }

  static LeaderElectionManagementRequest toLeaderElectionManagementRequest(LeaderElectionManagementRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final ClientId clientId = ClientId.valueOf(m.getRequestorId());
    final RaftPeerId serverId = RaftPeerId.valueOf(m.getReplyId());
    switch(p.getOpCase()) {
      case PAUSE:
        return LeaderElectionManagementRequest.newPause(clientId, serverId,
            ProtoUtils.toRaftGroupId(m.getRaftGroupId()), m.getCallId());
      case RESUME:
        return LeaderElectionManagementRequest.newResume(clientId, serverId,
            ProtoUtils.toRaftGroupId(m.getRaftGroupId()), m.getCallId());
      default:
        throw new IllegalArgumentException("Unexpected op " + p.getOpCase() + " in " + p);
    }
  }

  static LeaderElectionManagementRequestProto toLeaderElectionManagementRequestProto(
      LeaderElectionManagementRequest request) {
    final LeaderElectionManagementRequestProto.Builder b = LeaderElectionManagementRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
    final LeaderElectionManagementRequest.Pause pause = request.getPause();
    if (pause != null) {
      b.setPause(LeaderElectionPauseRequestProto.newBuilder().build());
    }
    final LeaderElectionManagementRequest.Resume resume = request.getResume();
    if (resume != null) {
      b.setResume(LeaderElectionResumeRequestProto.newBuilder().build());
    }
    return b.build();
  }

  static GroupInfoRequestProto toGroupInfoRequestProto(
      GroupInfoRequest request) {
    return GroupInfoRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .build();
  }

  static GroupListRequestProto toGroupListRequestProto(
      GroupListRequest request) {
    return GroupListRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .build();
  }

  static String toString(RaftClientRequestProto proto) {
    final RaftRpcRequestProto rpc = proto.getRpcRequest();
    return ClientId.valueOf(rpc.getRequestorId()) + "->" + rpc.getReplyId().toStringUtf8()
        + "#" + rpc.getCallId() + "-" + ProtoUtils.toString(rpc.getSlidingWindowEntry());
  }

  static String toString(RaftClientReplyProto proto) {
    final RaftRpcReplyProto rpc = proto.getRpcReply();
    return ClientId.valueOf(rpc.getRequestorId()) + "<-" + rpc.getReplyId().toStringUtf8()
        + "#" + rpc.getCallId();
  }
}
