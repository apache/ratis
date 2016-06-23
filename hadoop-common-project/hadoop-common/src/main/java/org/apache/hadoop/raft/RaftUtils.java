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
package org.apache.hadoop.raft;

import com.google.protobuf.ByteString;
import org.apache.hadoop.raft.proto.RaftProtos.RaftConfigurationProto;
import org.apache.hadoop.raft.proto.RaftProtos.RaftPeerProto;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.RaftConfiguration;

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
}
