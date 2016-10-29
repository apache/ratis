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
import com.google.protobuf.ServiceException;
import org.apache.raft.proto.RaftProtos.*;
import org.apache.raft.protocol.RaftPeer;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ProtoUtils {
  public static ByteString toByteString(byte[] bytes) {
    return toByteString(bytes, 0, bytes.length);
  }

  public static ByteString toByteString(byte[] bytes, int offset, int size) {
    // return singleton to reduce object allocation
    return bytes.length == 0 ?
        ByteString.EMPTY : ByteString.copyFrom(bytes, offset, size);
  }

  public static RaftPeerProto toRaftPeerProto(RaftPeer peer) {
    RaftPeerProto.Builder builder = RaftPeerProto.newBuilder()
        .setId(peer.getId());
    if (peer.getAddress() != null) {
      builder.setAddress(peer.getAddress());
    }
    return builder.build();
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

  public static boolean isConfigurationLogEntry(LogEntryProto entry) {
    return entry.getType() == LogEntryProto.Type.CONFIGURATION;
  }

  public static LogEntryProto toLogEntryProto(
      SMLogEntryProto operation, long term, long index) {
    return LogEntryProto.newBuilder().setTerm(term).setIndex(index)
        .setType(LogEntryProto.Type.CLIENT_MESSAGE)
        .setSmLogEntry(operation)
        .build();
  }

  public static IOException toIOException(ServiceException se) {
    final Throwable t = se.getCause();
    if (t == null) {
      return new IOException(se);
    }
    return t instanceof IOException? (IOException)t : new IOException(se);
  }

  public static String toString(RaftRpcRequestProto proto) {
    return proto.getRequestorId() + "->" + proto.getReplyId()
        + "#" + proto.getSeqNum();
  }

  public static String toString(RaftRpcReplyProto proto) {
    return proto.getRequestorId() + "<-" + proto.getReplyId()
        + "#" + proto.getSeqNum() + ":"
        + (proto.getSuccess()? "OK": "FAIL");
  }
  public static String toString(RequestVoteReplyProto proto) {
    return toString(proto.getServerReply()) + "-t" + proto.getTerm();
  }
}
