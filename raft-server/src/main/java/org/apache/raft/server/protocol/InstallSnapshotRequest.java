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

import org.apache.raft.proto.RaftProtos.FileChunkProto;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.storage.FileInfo;
import org.apache.raft.statemachine.SnapshotInfo;

import java.util.List;

/**
 * The RPC request sent by the leader to install a snapshot on a follower peer.
 * Since a snapshot can be large and some RPC engines may have message size
 * limitation, the request may contain only a chunk of the snapshot, and the
 * leader may send multiple requests for a complete snapshot.
 */
public class InstallSnapshotRequest extends RaftServerRequest {
  /** An identifier for the logical request in case it is a chunk */
  private final String requestId;
  /** The index for the install snapshot chunk for the same logical InstallSnapshotRequest. */
  private final int requestIndex;
  /** Configuration for the raft quorum */
  private final RaftConfiguration raftConfiguration;
  /** the current term of the leader */
  private final long leaderTerm;
  /** the last index included in the snapshot */
  private final TermIndex termIndex;
  /** the snapshot chunk */
  private final List<FileChunkProto> chunks;
  /** total size of the snapshot */
  private final long totalSize;
  /** Whether this is the last request */
  private final boolean done;

  public InstallSnapshotRequest(String from, String to, String requestId, int requestIndex,
      long leaderTerm, SnapshotInfo snapshot, List<FileChunkProto> chunks, boolean done) {
    this(from, to, requestId, requestIndex, snapshot.getRaftConfiguration(),
        leaderTerm, snapshot.getTermIndex(), chunks,
        snapshot.getFiles().stream()
            .mapToLong(FileInfo::getFileSize).reduce(Long::sum).getAsLong(),
        done);
  }

  public InstallSnapshotRequest(String from, String to, String requestId, int requestIndex,
                                RaftConfiguration raftConfiguration,
                                long leaderTerm, TermIndex termIndex,
                                List<FileChunkProto> chunks, long totalSize, boolean done) {
    super(from, to);
    this.requestId = requestId;
    this.requestIndex = requestIndex;
    this.raftConfiguration = raftConfiguration;
    this.leaderTerm = leaderTerm; // TODO: we do not need some of these for subsequent requests.
    this.termIndex = termIndex;
    this.chunks = chunks;
    this.totalSize = totalSize;
    this.done = done;
  }

  public String getRequestId() {
    return requestId;
  }

  public int getRequestIndex() {
    return requestIndex;
  }

  public long getLeaderTerm() {
    return leaderTerm;
  }

  public long getLastIncludedIndex() {
    return termIndex.getIndex();
  }

  public long getLastIncludedTerm() {
    return termIndex.getTerm();
  }

  public List<FileChunkProto> getChunks() {
    return chunks;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public boolean isDone() {
    return done;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(super.toString())
        .append(", leaderTerm: ").append(getLeaderTerm())
        .append(", lastIncludedIndex: ").append(getLastIncludedIndex())
        .append(", lastIncludedTerm: ").append(getLastIncludedTerm())
        .append(", chunks:");
    return toString(builder, chunks)
        .append(", totalSize: ").append(getTotalSize())
        .append(", done:").append(done).toString();
  }

  private StringBuilder toString(StringBuilder builder, List<FileChunkProto> chunks) {
    builder.append("{");
    for (FileChunkProto chunk : chunks) {
      builder.append("filename:").append(chunk.getFilename())
          .append(", offset:").append(chunk.getOffset())
          .append(" data.length").append(chunk.getData().size());
    }
    return builder.append("}");
  }
}
