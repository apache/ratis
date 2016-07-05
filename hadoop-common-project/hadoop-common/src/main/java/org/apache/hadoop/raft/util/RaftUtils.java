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
package org.apache.hadoop.raft.util;

import com.google.protobuf.ByteString;
import org.apache.hadoop.raft.proto.RaftProtos;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto.ConfigurationEntryProto;
import org.apache.hadoop.raft.proto.RaftProtos.RaftConfigurationProto;
import org.apache.hadoop.raft.proto.RaftProtos.RaftPeerProto;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.raft.server.protocol.ConfigurationEntry;
import org.apache.hadoop.raft.server.protocol.RaftLogEntry;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class RaftUtils {
  public static InterruptedIOException toInterruptedIOException(
      String message, InterruptedException e) {
    final InterruptedIOException iioe = new InterruptedIOException(message);
    iioe.initCause(e);
    return iioe;
  }

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  public static Iterable<RaftPeerProto> convertPeersToProtos(
      Collection<RaftPeer> peers) {
    List<RaftPeerProto> protos = new ArrayList<>(peers.size());
    for (RaftPeer p : peers) {
      protos.add(RaftPeerProto.newBuilder().setId(p.getId()).build());
    }
    return protos;
  }

  public static RaftConfigurationProto convertConfToProto(RaftConfiguration conf) {
    return RaftConfigurationProto.newBuilder()
        .addAllPeers(convertPeersToProtos(conf.getPeersInConf()))
        .addAllOldPeers(convertPeersToProtos(conf.getPeersInOldConf()))
        .build();
  }

  public static RaftPeer convertProtoToRaftPeer(RaftPeerProto proto) {
    return new RaftPeer(proto.getId());
  }

  public static RaftPeer[] convertProtoToRaftPeerArray(List<RaftPeerProto> protos) {
    RaftPeer[] peers = new RaftPeer[protos.size()];
    for (int i = 0; i < peers.length; i++) {
      peers[i] = convertProtoToRaftPeer(protos.get(i));
    }
    return peers;
  }

  public static RaftConfiguration convertProtoToConf(long index,
      RaftConfigurationProto proto) {
    RaftPeer[] peers = convertProtoToRaftPeerArray(proto.getPeersList());
    if (proto.getOldPeersCount() > 0) {
      RaftPeer[] oldPeers = convertProtoToRaftPeerArray(proto.getPeersList());
      return RaftConfiguration.composeOldNewConf(peers, oldPeers, index);
    } else {
      return RaftConfiguration.composeConf(peers, index);
    }
  }

  public static RaftLogEntry convertToRaftLogEntry(LogEntryProto proto) {
    if (proto.hasConfigurationEntry()) {
      RaftConfiguration conf = convertProtoToConf(proto.getIndex(),
          proto.getConfigurationEntry().getConf());
      return new ConfigurationEntry(proto.getTerm(), proto.getIndex(), conf);
    } else {
      return new RaftLogEntry(proto.getTerm(), proto.getIndex(),
          proto.getClientMessageEntry().getContent().toByteArray());
    }
  }
}
