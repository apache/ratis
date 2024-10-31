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
package org.apache.ratis.server.leader;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto.NotificationProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto.SnapshotChunkProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;

import java.util.Collections;

/** Leader only proto utilities. */
final class LeaderProtoUtils {
  private LeaderProtoUtils() {}

  static SnapshotChunkProto.Builder toSnapshotChunkProtoBuilder(String requestId, int requestIndex,
      TermIndex lastTermIndex, FileChunkProto chunk, long totalSize, boolean done) {
    return SnapshotChunkProto.newBuilder()
        .setRequestId(requestId)
        .setRequestIndex(requestIndex)
        .setTermIndex(lastTermIndex.toProto())
        .addAllFileChunks(Collections.singleton(chunk))
        .setTotalSize(totalSize)
        .setDone(done);
  }

  static InstallSnapshotRequestProto toInstallSnapshotRequestProto(
      RaftServer.Division server, RaftPeerId replyId, SnapshotChunkProto.Builder chunk) {
    return toInstallSnapshotRequestProtoBuilder(server, replyId)
        .setSnapshotChunk(chunk)
        .build();
  }

  static InstallSnapshotRequestProto toInstallSnapshotRequestProto(
      RaftServer.Division server, RaftPeerId replyId, TermIndex firstAvailable) {
    return toInstallSnapshotRequestProtoBuilder(server, replyId)
        .setNotification(NotificationProto.newBuilder().setFirstAvailableTermIndex(firstAvailable.toProto()))
        .build();
  }

  private static InstallSnapshotRequestProto.Builder toInstallSnapshotRequestProtoBuilder(
      RaftServer.Division server, RaftPeerId replyId) {
    // term is not going to used by installSnapshot to update the RaftConfiguration
    final RaftConfiguration conf = server.getRaftConf();
    final LogEntryProto confLogEntryProto = LogProtoUtils.toLogEntryProto(conf, null, conf.getLogEntryIndex());
    return InstallSnapshotRequestProto.newBuilder()
        .setServerRequest(ClientProtoUtils.toRaftRpcRequestProtoBuilder(server.getMemberId(), replyId))
        .setLeaderTerm(server.getInfo().getCurrentTerm())
        .setLastRaftConfigurationLogEntryProto(confLogEntryProto);
  }
}
