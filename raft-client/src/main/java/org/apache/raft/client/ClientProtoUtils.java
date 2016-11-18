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
package org.apache.raft.client;

import com.google.protobuf.ByteString;
import org.apache.raft.protocol.*;
import org.apache.raft.shaded.proto.RaftProtos.*;
import org.apache.raft.util.ProtoUtils;

import java.util.Arrays;

public class ClientProtoUtils {
  public static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      String requestorId, String replyId, long seqNum, boolean success) {
    return RaftRpcReplyProto.newBuilder()
        .setRequestorId(requestorId).setReplyId(replyId).setSeqNum(seqNum)
        .setSuccess(success);
  }

  public static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      String requesterId, String replyId, long seqNum) {
    return RaftRpcRequestProto.newBuilder()
        .setRequestorId(requesterId).setReplyId(replyId).setSeqNum(seqNum);
  }

  public static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    return new RaftClientRequest(p.getRpcRequest().getRequestorId(),
        p.getRpcRequest().getReplyId(), p.getRpcRequest().getSeqNum(),
        toMessage(p.getMessage()), p.getReadOnly());
  }

  public static RaftClientRequestProto toRaftClientRequestProto(
      RaftClientRequest request) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request.getRequestorId(),
            request.getReplierId(), request.getSeqNum()))
        .setMessage(toClientMessageEntryProto(request.getMessage()))
        .setReadOnly(request.isReadOnly())
        .build();
  }

  public static RaftClientRequestProto genRaftClientRequestProto(
      String requestorId, String replierId, long seqNum, ByteString content,
      boolean readOnly) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(requestorId, replierId, seqNum))
        .setMessage(ClientMessageEntryProto.newBuilder().setContent(content))
        .setReadOnly(readOnly)
        .build();
  }

  public static RaftClientReplyProto toRaftClientReplyProto(
      RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getRequestorId(),
          reply.getReplierId(), reply.getSeqNum(), reply.isSuccess()));
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
    return new SetConfigurationRequest(m.getRequestorId(), m.getReplyId(),
        p.getRpcRequest().getSeqNum(), peers);
  }

  public static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request.getRequestorId(),
            request.getReplierId(), request.getSeqNum()))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(
            Arrays.asList(request.getPeersInNewConf())))
        .build();
  }

  public static String toString(RaftRpcRequestProto request) {
    return request.getRequestorId() + "->" + request.getReplyId() + ", seq# "
        + request.getSeqNum();
  }
}
