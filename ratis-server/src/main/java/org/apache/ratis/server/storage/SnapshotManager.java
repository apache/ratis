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
package org.apache.ratis.server.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shaded.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage snapshots of a raft peer.
 * TODO: snapshot should be treated as compaction log thus can be merged into
 *       RaftLog. In this way we can have a unified getLastTermIndex interface.
 */
public class SnapshotManager {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

  private final RaftStorage storage;
  private final RaftPeerId selfId;

  public SnapshotManager(RaftStorage storage, RaftPeerId selfId)
      throws IOException {
    this.storage = storage;
    this.selfId = selfId;
  }

  public void installSnapshot(StateMachine stateMachine,
      InstallSnapshotRequestProto request) throws IOException {
    final long lastIncludedIndex = request.getTermIndex().getIndex();
    final RaftStorageDirectory dir = storage.getStorageDir();

    File tmpDir = dir.getNewTempDir();
    tmpDir.mkdirs();
    tmpDir.deleteOnExit();

    LOG.info("Installing snapshot:{}, to tmp dir:{}", request, tmpDir);

    // TODO: Make sure that subsequent requests for the same installSnapshot are coming in order,
    // and are not lost when whole request cycle is done. Check requestId and requestIndex here

    for (FileChunkProto chunk : request.getFileChunksList()) {
      SnapshotInfo pi = stateMachine.getLatestSnapshot();
      if (pi != null && pi.getTermIndex().getIndex() >= lastIncludedIndex) {
        throw new IOException("There exists snapshot file "
            + pi.getFiles() + " in " + selfId
            + " with endIndex >= lastIncludedIndex " + lastIncludedIndex);
      }

      String fileName = chunk.getFilename(); // this is relative to the root dir
      // TODO: assumes flat layout inside SM dir
      File tmpSnapshotFile = new File(tmpDir,
          new File(dir.getRoot(), fileName).getName());

      FileOutputStream out = null;
      try {
        // if offset is 0, delete any existing temp snapshot file if it has the
        // same last index.
        if (chunk.getOffset() == 0) {
          if (tmpSnapshotFile.exists()) {
            FileUtils.fullyDelete(tmpSnapshotFile);
          }
          // create the temp snapshot file and put padding inside
          out = new FileOutputStream(tmpSnapshotFile);
        } else {
          Preconditions.assertTrue(tmpSnapshotFile.exists());
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
      if (chunk.getDone()) {
        final MD5Hash expectedDigest =
            new MD5Hash(chunk.getFileDigest().toByteArray());
        // calculate the checksum of the snapshot file and compare it with the
        // file digest in the request
        MD5Hash digest = MD5FileUtil.computeMd5ForFile(tmpSnapshotFile);
        if (!digest.equals(expectedDigest)) {
          LOG.warn("The snapshot md5 digest {} does not match expected {}",
              digest, expectedDigest);
          // rename the temp snapshot file to .corrupt
//          NativeIO.renameTo(tmpSnapshotFile, // TODO:
//              dir.getCorruptSnapshotFile(lastIncludedTerm, lastIncludedIndex));
          throw new IOException("MD5 mismatch for snapshot-" + lastIncludedIndex
              + " installation");
        } else {
          MD5FileUtil.saveMD5File(tmpSnapshotFile, digest);
        }
      }
    }

    if (request.getDone()) {
      LOG.info("Install snapshot is done, renaming tnp dir:{} to:{}",
          tmpDir, dir.getStateMachineDir());
      dir.getStateMachineDir().delete();
      tmpDir.renameTo(dir.getStateMachineDir());
    }
  }
}
