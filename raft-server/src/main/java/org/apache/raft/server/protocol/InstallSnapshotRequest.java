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

import com.google.protobuf.ByteString;
import org.apache.hadoop.io.MD5Hash;
import org.apache.raft.proto.RaftServerProtocolProtos.SnapshotChunkProto;

/**
 * The RPC request sent by the leader to install a snapshot on a follower peer.
 * Since a snapshot can be large and some RPC engines may have message size
 * limitation, the request may contain only a chunk of the snapshot, and the
 * leader may send multiple requests for a complete snapshot.
 */
public class InstallSnapshotRequest extends RaftServerRequest {
  /** the current term of the leader */
  private final long leaderTerm;
  /** the last index included in the snapshot */
  private final long lastIncludedIndex;
  /** the term of last log entry that is included in the snapshot */
  private final long lastIncludedTerm;
  /** the snapshot chunk */
  private final SnapshotChunkProto chunk;
  /** total size of the snapshot */
  private final long totalSize;
  /**
   * The MD5 digest of the whole snapshot file. Non-null if this is the last
   * chunk.
   */
  private final MD5Hash fileDigest;

  public InstallSnapshotRequest(String from, String to, long leaderTerm,
      long lastIncludedIndex, long lastIncludedTerm, SnapshotChunkProto chunk,
      long totalSize, MD5Hash fileDigest) {
    super(from, to);
    this.leaderTerm = leaderTerm;
    this.lastIncludedIndex = lastIncludedIndex;
    this.lastIncludedTerm = lastIncludedTerm;
    this.chunk = chunk;
    this.totalSize = totalSize;
    this.fileDigest = fileDigest;
  }

  public long getLeaderTerm() {
    return leaderTerm;
  }

  public long getLastIncludedIndex() {
    return lastIncludedIndex;
  }

  public long getLastIncludedTerm() {
    return lastIncludedTerm;
  }

  public SnapshotChunkProto getChunk() {
    return chunk;
  }

  public long getOffset() {
    return chunk.getOffset();
  }

  public ByteString getData() {
    return chunk.getData();
  }

  public long getTotalSize() {
    return totalSize;
  }

  public boolean isDone() {
    return totalSize == getOffset() + getData().size();
  }

  public MD5Hash getFileDigest() {
    return this.fileDigest;
  }

  @Override
  public String toString() {
    return super.toString() + ", leaderTerm: " + getLeaderTerm()
        + ", lastIncludedIndex: " + getLastIncludedIndex()
        + ", lastIncludedTerm: " + getLastIncludedTerm()
        + ", offset: " + getOffset()
        + ", data.length: " + getData().size()
        + ", totalSize: " + getTotalSize();
  }
}
