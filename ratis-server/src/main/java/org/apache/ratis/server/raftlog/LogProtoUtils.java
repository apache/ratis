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
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.impl.ServerImplUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;

import java.util.List;
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
      s = ", " + Optional.ofNullable(function)
          .orElseGet(() -> proto -> "" + ClientInvocationId.valueOf(proto))
          .apply(entry.getStateMachineLogEntry());
    } else if (entry.hasMetadataEntry()) {
      final MetadataProto metadata = entry.getMetadataEntry();
      s = "(c:" + metadata.getCommitIndex() + ")";
    } else if (entry.hasConfigurationEntry()) {
      final RaftConfigurationProto config = entry.getConfigurationEntry();
      s = "(current:" + config.getPeersList().stream().map(p -> p.toString()).collect(Collectors.joining(",")) +
          ", old:" + config.getOldPeersList().stream().map(p -> p.toString()).collect(Collectors.joining(",")) + ")";
    } else {
      s = "";
    }
    return TermIndex.valueOf(entry) + ", " + entry.getLogEntryBodyCase() + s;
  }

  public static String toLogEntryString(LogEntryProto entry) {
    return toLogEntryString(entry, null);
  }

  public static String toLogEntriesString(List<LogEntryProto> entries) {
    return entries == null ? null
        : entries.stream().map(LogProtoUtils::toLogEntryString).collect(Collectors.toList()).toString();
  }

  public static String toLogEntriesShortString(List<LogEntryProto> entries) {
    return entries == null ? null
        : entries.size() == 0 ? "<empty>"
        : "size=" + entries.size() + ", first=" + LogProtoUtils.toLogEntryString(entries.get(0));
  }

  public static LogEntryProto toLogEntryProto(RaftConfiguration conf, Long term, long index) {
    final LogEntryProto.Builder b = LogEntryProto.newBuilder();
    Optional.ofNullable(term).ifPresent(b::setTerm);
    return b.setIndex(index)
        .setConfigurationEntry(toRaftConfigurationProtoBuilder(conf))
        .build();
  }

  public static RaftConfigurationProto.Builder toRaftConfigurationProtoBuilder(RaftConfiguration conf) {
    return RaftConfigurationProto.newBuilder()
        .addAllPeers(ProtoUtils.toRaftPeerProtos(conf.getCurrentPeers()))
        .addAllListeners(ProtoUtils.toRaftPeerProtos(conf.getCurrentPeers(RaftPeerRole.LISTENER)))
        .addAllOldPeers(ProtoUtils.toRaftPeerProtos(conf.getPreviousPeers()))
        .addAllOldListeners(
            ProtoUtils.toRaftPeerProtos(conf.getPreviousPeers(RaftPeerRole.LISTENER)));
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

  /**
   * If the given entry has state machine log entry and it has state machine data,
   * build a new entry without the state machine data.
   *
   * @return a new entry without the state machine data if the given has state machine data;
   *         otherwise, return the given entry.
   */
  public static LogEntryProto removeStateMachineData(LogEntryProto entry) {
    return getStateMachineEntry(entry)
        .map(StateMachineEntryProto::getStateMachineData)
        .filter(stateMachineData -> !stateMachineData.isEmpty())
        .map(_dummy -> replaceStateMachineDataWithSerializedSize(entry))
        .orElse(entry);
  }

  private static LogEntryProto replaceStateMachineDataWithSerializedSize(LogEntryProto entry) {
    return replaceStateMachineEntry(entry,
        StateMachineEntryProto.newBuilder().setLogEntryProtoSerializedSize(entry.getSerializedSize()));
  }

  private static LogEntryProto replaceStateMachineEntry(LogEntryProto proto, StateMachineEntryProto.Builder newEntry) {
    Preconditions.assertTrue(proto.hasStateMachineLogEntry(), () -> "Unexpected proto " + proto);
    return LogEntryProto.newBuilder(proto).setStateMachineLogEntry(
        StateMachineLogEntryProto.newBuilder(proto.getStateMachineLogEntry()).setStateMachineEntry(newEntry)
    ).build();
  }

  /**
   * Return a new log entry based on the input log entry with stateMachineData added.
   * @param stateMachineData - state machine data to be added
   * @param entry - log entry to which stateMachineData needs to be added
   * @return LogEntryProto with stateMachineData added
   */
  static LogEntryProto addStateMachineData(ByteString stateMachineData, LogEntryProto entry) {
    Preconditions.assertTrue(isStateMachineDataEmpty(entry),
        () -> "Failed to addStateMachineData to " + entry + " since shouldReadStateMachineData is false.");
    return replaceStateMachineEntry(entry, StateMachineEntryProto.newBuilder().setStateMachineData(stateMachineData));
  }

  public static boolean isStateMachineDataEmpty(LogEntryProto entry) {
    return getStateMachineEntry(entry)
        .map(StateMachineEntryProto::getStateMachineData)
        .map(ByteString::isEmpty)
        .orElse(false);
  }

  private static Optional<StateMachineEntryProto> getStateMachineEntry(LogEntryProto entry) {
    return Optional.of(entry)
        .filter(LogEntryProto::hasStateMachineLogEntry)
        .map(LogEntryProto::getStateMachineLogEntry)
        .filter(StateMachineLogEntryProto::hasStateMachineEntry)
        .map(StateMachineLogEntryProto::getStateMachineEntry);
  }

  public static int getSerializedSize(LogEntryProto entry) {
    return getStateMachineEntry(entry)
        .filter(stateMachineEntry -> stateMachineEntry.getStateMachineData().isEmpty())
        .map(StateMachineEntryProto::getLogEntryProtoSerializedSize)
        .orElseGet(entry::getSerializedSize);
  }

  private static StateMachineLogEntryProto.Type toStateMachineLogEntryProtoType(RaftClientRequestProto.TypeCase type) {
    switch (type) {
      case WRITE: return StateMachineLogEntryProto.Type.WRITE;
      case DATASTREAM: return StateMachineLogEntryProto.Type.DATASTREAM;
      default:
        throw new IllegalStateException("Unexpected request type " + type);
    }
  }

  public static StateMachineLogEntryProto toStateMachineLogEntryProto(
      RaftClientRequest request, ByteString logData, ByteString stateMachineData) {
    if (logData == null) {
      logData = request.getMessage().getContent();
    }
    final StateMachineLogEntryProto.Type type = toStateMachineLogEntryProtoType(request.getType().getTypeCase());
    return toStateMachineLogEntryProto(request.getClientId(), request.getCallId(), type, logData, stateMachineData);
  }

  public static StateMachineLogEntryProto toStateMachineLogEntryProto(ClientId clientId, long callId,
      StateMachineLogEntryProto.Type type, ByteString logData, ByteString stateMachineData) {
    final StateMachineLogEntryProto.Builder b = StateMachineLogEntryProto.newBuilder()
        .setClientId(clientId.toByteString())
        .setCallId(callId)
        .setType(type)
        .setLogData(logData);
    Optional.ofNullable(stateMachineData)
        .map(StateMachineEntryProto.newBuilder()::setStateMachineData)
        .ifPresent(b::setStateMachineEntry);
    return b.build();
  }

  public static RaftConfiguration toRaftConfiguration(LogEntryProto entry) {
    Preconditions.assertTrue(entry.hasConfigurationEntry());
    final RaftConfigurationProto proto = entry.getConfigurationEntry();
    final List<RaftPeer> conf = ProtoUtils.toRaftPeers(proto.getPeersList());
    final List<RaftPeer> listener = ProtoUtils.toRaftPeers(proto.getListenersList());
    final List<RaftPeer> oldConf = ProtoUtils.toRaftPeers(proto.getOldPeersList());
    final List<RaftPeer> oldListener = ProtoUtils.toRaftPeers(proto.getOldListenersList());
    return ServerImplUtils.newRaftConfiguration(conf, listener, entry.getIndex(), oldConf, oldListener);
  }
}
