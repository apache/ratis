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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto.SnapshotChunkProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.FileChunkReader;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.JavaUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An {@link Iterable} of {@link InstallSnapshotRequestProto} for sending a snapshot.
 *
 * The snapshot is sent by one or more requests, where
 * a snapshot has one or more files, and
 * a file is sent by one or more chunks.
 * The number of requests is equal to the sum of the numbers of chunks of each file.
 */
class InstallSnapshotRequests implements Iterable<InstallSnapshotRequestProto> {
  private final RaftServer.Division server;
  private final RaftPeerId followerId;

  /** The snapshot to be sent. */
  private final SnapshotInfo snapshot;
  /** A fixed id for all the requests. */
  private final String requestId;

  /** Maximum chunk size. */
  private final int snapshotChunkMaxSize;

  /** The index of the current request. */
  private int requestIndex = 0;

  /** The index of the current file. */
  private int fileIndex = 0;
  /** The current file. */
  private FileChunkReader current;

  InstallSnapshotRequests(RaftServer.Division server, RaftPeerId followerId,
      String requestId, SnapshotInfo snapshot, int snapshotChunkMaxSize) {
    this.server = server;
    this.followerId = followerId;
    this.requestId = requestId;
    this.snapshot = snapshot;
    this.snapshotChunkMaxSize = snapshotChunkMaxSize;
  }

  @Override
  public Iterator<InstallSnapshotRequestProto> iterator() {
    return new Iterator<InstallSnapshotRequestProto>() {
      @Override
      public boolean hasNext() {
        return fileIndex < snapshot.getFiles().size();
      }

      @Override
      @SuppressFBWarnings("IT_NO_SUCH_ELEMENT")
      public InstallSnapshotRequestProto next() {
        return nextInstallSnapshotRequestProto();
      }
    };
  }

  private InstallSnapshotRequestProto nextInstallSnapshotRequestProto() {
    final int numFiles = snapshot.getFiles().size();
    if (fileIndex >= numFiles) {
      throw new NoSuchElementException();
    }
    final FileInfo info = snapshot.getFiles().get(fileIndex);
    try {
      if (current == null) {
        current = new FileChunkReader(info, server.getRaftStorage().getStorageDir());
      }
      final FileChunkProto chunk = current.readFileChunk(snapshotChunkMaxSize);
      if (chunk.getDone()) {
        current.close();
        current = null;
        fileIndex++;
      }

      final boolean done = fileIndex == numFiles && chunk.getDone();
      return newInstallSnapshotRequest(chunk, done);
    } catch (IOException e) {
      if (current != null) {
        try {
          current.close();
          current = null;
        } catch (IOException ignored) {
        }
      }
      throw new IllegalStateException("Failed to iterate installSnapshot requests: " + this, e);
    }
  }

  private InstallSnapshotRequestProto newInstallSnapshotRequest(FileChunkProto chunk, boolean done) {
    final long totalSize = snapshot.getFiles().stream().mapToLong(FileInfo::getFileSize).reduce(Long::sum).orElseThrow(
        () -> new IllegalStateException("Failed to compute total size for snapshot " + snapshot));
    synchronized (server) {
      final SnapshotChunkProto.Builder b = LeaderProtoUtils.toSnapshotChunkProtoBuilder(
          requestId, requestIndex++, snapshot.getTermIndex(), chunk, totalSize, done);
      return LeaderProtoUtils.toInstallSnapshotRequestProto(server, followerId, b);
    }
  }


  @Override
  public String toString() {
    return server.getId() + "->" + followerId + JavaUtils.getClassSimpleName(getClass())
        + ": requestId=" + requestId
        + ", requestIndex=" + requestIndex
        + ", fileIndex=" + fileIndex
        + ", currentFile=" + current
        + ", snapshot=" + snapshot;
  }
}
