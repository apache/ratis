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
package org.apache.ratis.server.util;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.util.ProtoUtils;

/**
 *  This class provides convenient utilities for converting Protocol Buffers messages to strings.
 *  The output strings are for information purpose only.
 *  They are concise and compact compared to the Protocol Buffers implementations of {@link Object#toString()}.
 *
 *  The output messages or the output formats may be changed without notice.
 *  Callers of this class should not try to parse the output strings for any purposes.
 *  Instead, they should use the public APIs provided by Protocol Buffers.
 */
public final class ServerStringUtils {
  private ServerStringUtils() {}

  public static String toAppendEntriesRequestString(AppendEntriesRequestProto request) {
    if (request == null) {
      return null;
    }
    return ProtoUtils.toString(request.getServerRequest())
        + "-t" + request.getLeaderTerm()
        + ",previous=" + TermIndex.valueOf(request.getPreviousLog())
        + ",leaderCommit=" + request.getLeaderCommit()
        + ",initializing? " + request.getInitializing()
        + ",entries: " + LogProtoUtils.toLogEntriesShortString(request.getEntriesList());
  }

  public static String toAppendEntriesReplyString(AppendEntriesReplyProto reply) {
    if (reply == null) {
      return null;
    }
    return ProtoUtils.toString(reply.getServerReply())
        + "-t" + reply.getTerm()
        + "," + reply.getResult()
        + ",nextIndex=" + reply.getNextIndex()
        + ",followerCommit=" + reply.getFollowerCommit()
        + ",matchIndex=" + reply.getMatchIndex();
  }

  public static String toInstallSnapshotRequestString(InstallSnapshotRequestProto request) {
    if (request == null) {
      return null;
    }
    final String s;
    switch (request.getInstallSnapshotRequestBodyCase()) {
      case SNAPSHOTCHUNK:
        final InstallSnapshotRequestProto.SnapshotChunkProto chunk = request.getSnapshotChunk();
        s = "chunk:" + chunk.getRequestId() + "," + chunk.getRequestIndex();
        break;
      case NOTIFICATION:
        final InstallSnapshotRequestProto.NotificationProto notification = request.getNotification();
        s = "notify:" + TermIndex.valueOf(notification.getFirstAvailableTermIndex());
        break;
      default:
        throw new IllegalStateException("Unexpected body case in " + request);
    }
    return ProtoUtils.toString(request.getServerRequest())
        + "-t" + request.getLeaderTerm()
        + "," + s;
  }

  public static String toInstallSnapshotReplyString(InstallSnapshotReplyProto reply) {
    if (reply == null) {
      return null;
    }
    final String s;
    switch (reply.getInstallSnapshotReplyBodyCase()) {
      case REQUESTINDEX:
        s = ",requestIndex=" + reply.getRequestIndex();
        break;
      case SNAPSHOTINDEX:
        s = ",snapshotIndex=" + reply.getSnapshotIndex();
        break;
      default:
        s = ""; // result is not SUCCESS
    }
    return ProtoUtils.toString(reply.getServerReply())
        + "-t" + reply.getTerm()
        + "," + reply.getResult() + s;
  }

  public static String toRequestVoteReplyString(RequestVoteReplyProto proto) {
    if (proto == null) {
      return null;
    }
    return ProtoUtils.toString(proto.getServerReply()) + "-t" + proto.getTerm();
  }
}
