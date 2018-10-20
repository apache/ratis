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
package org.apache.ratis.util;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftGroupProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.ServiceException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public interface ProtoUtils {
  static ByteString writeObject2ByteString(Object obj) {
    final ByteString.Output byteOut = ByteString.newOutput();
    try(final ObjectOutputStream objOut = new ObjectOutputStream(byteOut)) {
      objOut.writeObject(obj);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected IOException when writing an object to a ByteString.", e);
    }
    return byteOut.toByteString();
  }

  static Object toObject(ByteString bytes) {
    try(final ObjectInputStream in = new ObjectInputStream(bytes.newInput())) {
      return in.readObject();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected IOException when reading an object from a ByteString.", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  static ByteString toByteString(String string) {
    return ByteString.copyFromUtf8(string);
  }

  static ByteString toByteString(byte[] bytes) {
    return toByteString(bytes, 0, bytes.length);
  }

  static ByteString toByteString(byte[] bytes, int offset, int size) {
    // return singleton to reduce object allocation
    return bytes.length == 0 ?
        ByteString.EMPTY : ByteString.copyFrom(bytes, offset, size);
  }

  static RaftPeerProto toRaftPeerProto(RaftPeer peer) {
    RaftPeerProto.Builder builder = RaftPeerProto.newBuilder()
        .setId(peer.getId().toByteString());
    if (peer.getAddress() != null) {
      builder.setAddress(peer.getAddress());
    }
    return builder.build();
  }

  static RaftPeer toRaftPeer(RaftPeerProto p) {
    return new RaftPeer(RaftPeerId.valueOf(p.getId()), p.getAddress());
  }

  static RaftPeer[] toRaftPeerArray(List<RaftPeerProto> protos) {
    final RaftPeer[] peers = new RaftPeer[protos.size()];
    for (int i = 0; i < peers.length; i++) {
      peers[i] = toRaftPeer(protos.get(i));
    }
    return peers;
  }

  static Iterable<RaftPeerProto> toRaftPeerProtos(
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

  static RaftGroupId toRaftGroupId(RaftGroupIdProto proto) {
    return RaftGroupId.valueOf(proto.getId());
  }

  static RaftGroupIdProto.Builder toRaftGroupIdProtoBuilder(RaftGroupId id) {
    return RaftGroupIdProto.newBuilder().setId(id.toByteString());
  }

  static RaftGroup toRaftGroup(RaftGroupProto proto) {
    return RaftGroup.valueOf(toRaftGroupId(proto.getGroupId()), toRaftPeerArray(proto.getPeersList()));
  }

  static RaftGroupProto.Builder toRaftGroupProtoBuilder(RaftGroup group) {
    return RaftGroupProto.newBuilder()
        .setGroupId(toRaftGroupIdProtoBuilder(group.getGroupId()))
        .addAllPeers(toRaftPeerProtos(group.getPeers()));
  }

  static CommitInfoProto toCommitInfoProto(RaftPeer peer, long commitIndex) {
    return CommitInfoProto.newBuilder()
        .setServer(toRaftPeerProto(peer))
        .setCommitIndex(commitIndex)
        .build();
  }

  static void addCommitInfos(Collection<CommitInfoProto> commitInfos, Consumer<CommitInfoProto> adder) {
    if (commitInfos != null && !commitInfos.isEmpty()) {
      commitInfos.stream().forEach(i -> adder.accept(i));
    }
  }

  static String toString(CommitInfoProto proto) {
    return RaftPeerId.valueOf(proto.getServer().getId()) + ":c" + proto.getCommitIndex();
  }

  static String toString(Collection<CommitInfoProto> protos) {
    return protos.stream().map(ProtoUtils::toString).collect(Collectors.toList()).toString();
  }

  static IOException toIOException(ServiceException se) {
    final Throwable t = se.getCause();
    if (t == null) {
      return new IOException(se);
    }
    return t instanceof IOException? (IOException)t : new IOException(se);
  }

  static String toString(RaftRpcRequestProto proto) {
    return proto.getRequestorId().toStringUtf8() + "->" + proto.getReplyId().toStringUtf8()
        + "#" + proto.getCallId();
  }

  static String toString(RaftRpcReplyProto proto) {
    return proto.getRequestorId().toStringUtf8() + "<-" + proto.getReplyId().toStringUtf8()
        + "#" + proto.getCallId() + ":"
        + (proto.getSuccess()? "OK": "FAIL");
  }
  static String toString(RequestVoteReplyProto proto) {
    return toString(proto.getServerReply()) + "-t" + proto.getTerm();
  }
  static String toString(AppendEntriesReplyProto proto) {
    return toString(proto.getServerReply()) + "-t" + proto.getTerm()
        + ", nextIndex=" + proto.getNextIndex()
        + ", result: " + proto.getResult();
  }
}
