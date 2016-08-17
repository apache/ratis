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
package org.apache.raft.server.storage;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.raft.proto.RaftServerProtocolProtos;
import org.apache.raft.server.protocol.InstallSnapshotRequest;
import org.apache.raft.server.storage.RaftStorageDirectory.SnapshotPathAndTermIndex;
import org.apache.raft.util.MD5FileUtil;
import org.apache.raft.util.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Manage snapshots of a raft peer.
 * TODO: snapshot should be treated as compaction log thus can be merged into
 *       RaftLog. In this way we can have a unified getLastTermIndex interface.
 */
public class SnapshotManager {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

  private final RaftStorage storage;
  private final String selfId;
  private volatile SnapshotPathAndTermIndex latestSnapshot;

  public SnapshotManager(RaftStorage storage, String selfId)
      throws IOException {
    this.storage = storage;
    this.selfId = selfId;
    latestSnapshot = storage.getLastestSnapshotPath();
  }

  public void installSnapshot(InstallSnapshotRequest request) throws IOException {
    final long lastIncludedIndex = request.getLastIncludedIndex();
    final long lastIncludedTerm = request.getLastIncludedTerm();
    final RaftStorageDirectory dir = storage.getStorageDir();

    SnapshotPathAndTermIndex pi = dir.getLatestSnapshot();
    if (pi != null && pi.endIndex >= lastIncludedIndex) {
      throw new IOException("There exists snapshot file "
          + pi.path.getFileName() + " in " + selfId
          + " with endIndex >= lastIncludedIndex " + lastIncludedIndex);
    }

    final File tmpSnapshotFile = dir.getTmpSnapshotFile(lastIncludedTerm,
        lastIncludedIndex);
    final RaftServerProtocolProtos.SnapshotChunkProto chunk = request.getChunk();
    FileOutputStream out = null;
    try {
      // if offset is 0, delete any existing temp snapshot file if it has the
      // same last index.
      if (chunk.getOffset() == 0) {
        if (tmpSnapshotFile.exists()) {
          RaftUtils.deleteFile(tmpSnapshotFile);
        }
        // create the temp snapshot file and put padding inside
        out = new FileOutputStream(tmpSnapshotFile);
      } else {
        Preconditions.checkState(tmpSnapshotFile.exists());
        out = new FileOutputStream(tmpSnapshotFile, true);
        FileChannel fc = out.getChannel();
        fc.position(chunk.getOffset());
      }

      // write data to the file
      out.write(chunk.getData().toByteArray());
    } finally {
      IOUtils.cleanup(null, out);
    }


    // rename the temp snapshot file if this is the last chunk. also verify
    // the md5 digest and create the md5 meta-file.
    if (request.isDone()) {
      final MD5Hash expectedDigest = request.getFileDigest();
      if (expectedDigest == null) {
        LOG.warn("MD5 digest in InstallSnapshot request is null");
      }

      // calculate the checksum of the snapshot file and compare it with the
      // file digest in the request
      MD5Hash digest = MD5FileUtil.computeMd5ForFile(tmpSnapshotFile);
      if (expectedDigest != null && !digest.equals(expectedDigest)) {
        LOG.warn("The snapshot md5 digest {} does not match expected {}",
            digest, expectedDigest);
        // rename the temp snapshot file to .corrupt
        NativeIO.renameTo(tmpSnapshotFile,
            dir.getCorruptSnapshotFile(lastIncludedTerm, lastIncludedIndex));
        throw new IOException("MD5 mismatch for snapshot-" + lastIncludedIndex
            + " installation");
      } else {
        // rename the temp snapshot file. also persist the md5 hash.
        final File snapshotFile = dir.getSnapshotFile(lastIncludedTerm,
            lastIncludedIndex);
        NativeIO.renameTo(tmpSnapshotFile, snapshotFile);
        MD5FileUtil.saveMD5File(snapshotFile, digest);
      }
    }
  }

  public SnapshotPathAndTermIndex getLatestSnapshot() {
    return latestSnapshot;
  }

  public void setLatestSnapshot(SnapshotPathAndTermIndex snapshot) {
    this.latestSnapshot = snapshot;
  }
}
