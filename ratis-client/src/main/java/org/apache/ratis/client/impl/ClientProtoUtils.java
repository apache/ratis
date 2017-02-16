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

import java.util.Arrays;

public class ClientProtoUtils {
  public static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      byte[] requestorId, byte[] replyId, long seqNum, boolean success) {
    return RaftRpcReplyProto.newBuilder()
        .setRequestorId(ProtoUtils.toByteString(requestorId))
        .setReplyId(ProtoUtils.toByteString(replyId))
        .setSeqNum(seqNum)
        .setSuccess(success);
  }

  public static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      byte[] requesterId, byte[] replyId, long seqNum) {
    return RaftRpcRequestProto.newBuilder()
        .setRequestorId(ProtoUtils.toByteString(requesterId))
        .setReplyId(ProtoUtils.toByteString(replyId))
        .setSeqNum(seqNum);
  }

  public static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    ClientId clientId = new ClientId(
        p.getRpcRequest().getRequestorId().toByteArray());
    RaftPeerId serverId = new RaftPeerId(
        p.getRpcRequest().getReplyId());
    return new RaftClientRequest(clientId, serverId,
        p.getRpcRequest().getSeqNum(),
        toMessage(p.getMessage()), p.getReadOnly());
  }

  public static RaftClientRequestProto toRaftClientRequestProto(
      RaftClientRequest request) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request.getClientId().toBytes(),
            request.getServerId().toBytes(), request.getSeqNum()))
        .setMessage(toClientMessageEntryProto(request.getMessage()))
        .setReadOnly(request.isReadOnly())
        .build();
  }

  public static RaftClientRequestProto genRaftClientRequestProto(
      ClientId clientId, RaftPeerId serverId, long seqNum, ByteString content,
      boolean readOnly) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(clientId.toBytes(),
            serverId.toBytes(), seqNum))
        .setMessage(ClientMessageEntryProto.newBuilder().setContent(content))
        .setReadOnly(readOnly)
        .build();
  }

  public static RaftClientReplyProto toRaftClientReplyProto(
      RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toBytes(),
          reply.getServerId().toBytes(), reply.getSeqNum(), reply.isSuccess()));
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
      e = new NotLeaderException(new RaftPeerId(rp.getReplyId()),
          suggestedLeader, peers);
    }
    return new RaftClientReply(new ClientId(rp.getRequestorId().toByteArray()),
        new RaftPeerId(rp.getReplyId()),
        rp.getSeqNum(), rp.getSuccess(), toMessage(replyProto.getMessage()), e);
  }

  public static Message toMessage(final ClientMessageEntryProto p) {
    return p::getContent;
  }

  public static ClientMessageEntryProto toClientMessageEntryProto(Message message) {
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
        p.getRpcRequest().getSeqNum(), peers);
  }

  public static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(
            request.getClientId().toBytes(),
            request.getServerId().toBytes(),
            request.getSeqNum()))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(
            Arrays.asList(request.getPeersInNewConf())))
        .build();
  }
}
