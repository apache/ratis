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
package org.apache.ratis.server.raftlog;

import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.ProtoUtils;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Log proto utilities. */
public final class LogProtoUtils {
  private LogProtoUtils() {}

  public static String toLogEntryString(LogEntryProto entry, Function<StateMachineLogEntryProto, String> function) {
    if (entry == null) {
      return null;
    }
    final String s;
    if (entry.hasStateMachineLogEntry()) {
      s = ", " + ServerProtoUtils.toStateMachineLogEntryString(entry.getStateMachineLogEntry(), function);
    } else if (entry.hasMetadataEntry()) {
      final MetadataProto metadata = entry.getMetadataEntry();
      s = "(c:" + metadata.getCommitIndex() + ")";
    } else {
      s = "";
    }
    return TermIndex.toString(entry.getTerm(), entry.getIndex()) + ", " + entry.getLogEntryBodyCase() + s;
  }

  public static String toLogEntryString(LogEntryProto entry) {
    return toLogEntryString(entry, null);
  }

  public static String toLogEntryStrings(LogEntryProto... entries) {
    return entries == null ? null
        : entries.length == 0 ? "[]"
        : entries.length == 1 ? toLogEntryString(entries[0])
        : "" + Arrays.stream(entries).map(LogProtoUtils::toLogEntryString).collect(Collectors.toList());
  }

  public static LogEntryProto toLogEntryProto(RaftConfiguration conf, Long term, long index) {
    final LogEntryProto.Builder b = LogEntryProto.newBuilder();
    Optional.ofNullable(term).ifPresent(b::setTerm);
    return b.setIndex(index)
        .setConfigurationEntry(toRaftConfigurationProtoBuilder(conf))
        .build();
  }

  private static RaftConfigurationProto.Builder toRaftConfigurationProtoBuilder(RaftConfiguration conf) {
    return RaftConfigurationProto.newBuilder()
        .addAllPeers(ProtoUtils.toRaftPeerProtos(conf.getCurrentPeers()))
        .addAllOldPeers(ProtoUtils.toRaftPeerProtos(conf.getPreviousPeers()));
  }

  public static LogEntryProto toLogEntryProto(StateMachineLogEntryProto proto, long term, long index) {
    return LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(proto)
        .build();
  }

  public static LogEntryProto toLogEntryProto(long commitIndex, long term, long index) {
    return LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(commitIndex))
        .build();
  }
}
