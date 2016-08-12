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
package org.apache.raft.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ServiceException;
import org.apache.raft.proto.RaftClientProtocolProtos.RaftClientReplyProto;
import org.apache.raft.proto.RaftClientProtocolProtos.RaftClientRequestProto;
import org.apache.raft.proto.RaftClientProtocolProtos.SetConfigurationReplyProto;
import org.apache.raft.proto.RaftClientProtocolProtos.SetConfigurationRequestProto;
import org.apache.raft.proto.RaftProtos.*;
import org.apache.raft.protocol.*;
import org.apache.raft.server.RaftConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ProtoUtils {
  public static ByteString toByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return bytes.length == 0? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  public static RaftPeerProto toRaftPeerProto(RaftPeer peer) {
    return RaftPeerProto.newBuilder()
        .setId(peer.getId())
        .setAddress(peer.getAddress())
        .build();
  }

  public static RaftPeer toRaftPeer(RaftPeerProto p) {
    return new RaftPeer(p.getId(), p.getAddress());
  }

  public static RaftPeer[] toRaftPeerArray(List<RaftPeerProto> protos) {
    final RaftPeer[] peers = new RaftPeer[protos.size()];
    for (int i = 0; i < peers.length; i++) {
      peers[i] = toRaftPeer(protos.get(i));
    }
    return peers;
  }

  public static Iterable<RaftPeerProto> toRaftPeerProtos(
      final Collection<RaftPeer> peers) {
    return () -> new Iterator<RaftPeerProto>() {
      final Iterator<RaftPeer> i = peers.iterator();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public RaftPeerProto next() {
        return toRaftPeerProto(i.next());
      }
    };
  }

  public static ConfigurationMessageProto toConfigurationMessageProto(
      ConfigurationMessage m) {
    return ConfigurationMessageProto.newBuilder()
        .addAllPeers(toRaftPeerProtos(Arrays.asList(m.getMembers())))
        .build();
  }

  public static ConfigurationMessage toConfigurationMessage(
      ConfigurationMessageProto p) {
    return new ConfigurationMessage(toRaftPeerArray(p.getPeersList()));
  }

  public static RaftConfigurationProto toRaftConfigurationProto(
      RaftConfiguration conf) {
    return RaftConfigurationProto.newBuilder()
        .addAllPeers(toRaftPeerProtos(conf.getPeersInConf()))
        .addAllOldPeers(toRaftPeerProtos(conf.getPeersInOldConf()))
        .build();
  }

  public static RaftConfiguration toRaftConfiguration(
      long index, RaftConfigurationProto proto) {
    final RaftPeer[] peers = toRaftPeerArray(proto.getPeersList());
    if (proto.getOldPeersCount() > 0) {
      final RaftPeer[] oldPeers = toRaftPeerArray(proto.getPeersList());
      return RaftConfiguration.composeOldNewConf(peers, oldPeers, index);
    } else {
      return RaftConfiguration.composeConf(peers, index);
    }
  }

  public static boolean isConfigurationLogEntry(LogEntryProto entry) {
    return entry.getType() == LogEntryProto.Type.CONFIGURATION;
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

  public static ClientMessageEntryProto toClientMessageEntryProto(Message message) {
    return ClientMessageEntryProto.newBuilder()
        .setContent(toByteString(message.getContent())).build();
  }

  public static LogEntryProto toLogEntryProto(
      Message message, long term, long index) {
    return LogEntryProto.newBuilder().setTerm(term).setIndex(index)
        .setType(LogEntryProto.Type.CLIENT_MESSAGE)
        .setClientMessageEntry(toClientMessageEntryProto(message))
        .build();
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

  public static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      RaftRpcRequestProto request, RaftRpcMessage.Reply reply) {
    return RaftRpcReplyProto.newBuilder()
        .setRpcMessage(request.getRpcMessage())
        .setSuccess(reply.isSuccess());
  }

  public static Message toMessage(final ClientMessageEntryProto p) {
    return () -> p.getContent().toByteArray();
  }

  public static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    final RaftRpcMessageProto m = p.getRpcRequest().getRpcMessage();
    return new RaftClientRequest(m.getRequestorId(), m.getReplyId(),
        toMessage(p.getMessage()));
  }

  public static RaftClientRequestProto toRaftClientRequestProto(
      RaftClientRequest request) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .setMessage(toClientMessageEntryProto(request.getMessage()))
        .build();
  }

  public static RaftClientReplyProto toRaftClientReplyProto(
      RaftClientRequestProto request, RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(request.getRpcRequest(), reply));
    }
    return b.build();
  }

  public static SetConfigurationRequest toSetConfigurationRequest(
      SetConfigurationRequestProto p) throws InvalidProtocolBufferException {
    final RaftRpcMessageProto m = p.getClientRequest().getRpcRequest().getRpcMessage();
    final ConfigurationMessageProto conf = ConfigurationMessageProto.parseFrom(
        p.getClientRequest().getMessage().getContent().toByteArray());
    return new SetConfigurationRequest(m.getRequestorId(), m.getReplyId(),
        toRaftPeerArray(conf.getPeersList()));
  }

  public static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setClientRequest(toRaftClientRequestProto(request))
        .build();
  }

  public static SetConfigurationReplyProto toSetConfigurationReplyProto(
      SetConfigurationRequestProto request, RaftClientReply reply) {
    return SetConfigurationReplyProto.newBuilder()
        .setClientReply(toRaftClientReplyProto(request.getClientRequest(), reply))
        .build();
  }

  public static IOException toIOException(ServiceException se) {
    final Throwable t = se.getCause();
    if (t == null) {
      return new IOException(se);
    }
    return t instanceof IOException? (IOException)t : new IOException(se);
  }
}
