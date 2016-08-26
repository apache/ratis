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
package org.apache.raft.server.protocol;

import org.apache.raft.proto.RaftProtos;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.proto.RaftProtos.TermIndexProto;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.util.ProtoUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ServerProtoUtils {
  public static TermIndex toTermIndex(TermIndexProto p) {
    return p == null? null: new TermIndex(p.getTerm(), p.getIndex());
  }

  public static TermIndexProto toTermIndexProto(TermIndex ti) {
    return ti == null? null: TermIndexProto.newBuilder()
        .setTerm(ti.getTerm())
        .setIndex(ti.getIndex())
        .build();
  }

  public static TermIndex toTermIndex(LogEntryProto entry) {
    return entry == null ? null :
        new TermIndex(entry.getTerm(), entry.getIndex());
  }

  public static String toString(LogEntryProto... entries) {
    return entries == null? "null"
        : entries.length == 0 ? "[]"
        : entries.length == 1? "" + toTermIndex(entries[0])
        : "" + Arrays.stream(entries).map(ServerProtoUtils::toTermIndex)
            .collect(Collectors.toList());
  }

  public static RaftProtos.RaftConfigurationProto toRaftConfigurationProto(
      RaftConfiguration conf) {
    return RaftProtos.RaftConfigurationProto.newBuilder()
        .addAllPeers(ProtoUtils.toRaftPeerProtos(conf.getPeersInConf()))
        .addAllOldPeers(ProtoUtils.toRaftPeerProtos(conf.getPeersInOldConf()))
        .build();
  }

  public static RaftConfiguration toRaftConfiguration(
      long index, RaftProtos.RaftConfigurationProto proto) {
    final RaftPeer[] peers = ProtoUtils.toRaftPeerArray(proto.getPeersList());
    if (proto.getOldPeersCount() > 0) {
      final RaftPeer[] oldPeers = ProtoUtils.toRaftPeerArray(proto.getPeersList());
      return RaftConfiguration.composeOldNewConf(peers, oldPeers, index);
    } else {
      return RaftConfiguration.composeConf(peers, index);
    }
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
}
