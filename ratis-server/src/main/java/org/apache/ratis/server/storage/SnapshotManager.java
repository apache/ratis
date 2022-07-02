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
package org.apache.ratis.server.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.function.Supplier;

import org.apache.ratis.io.CorruptedFileException;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage snapshots of a raft peer.
 * TODO: snapshot should be treated as compaction log thus can be merged into
 *       RaftLog. In this way we can have a unified getLastTermIndex interface.
 */
public class SnapshotManager {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

  private static final String CORRUPT = ".corrupt";
  private static final String TMP = ".tmp";

  private final RaftStorage storage;
  private final RaftPeerId selfId;
  private final Supplier<MessageDigest> digester = JavaUtils.memoize(MD5Hash::getDigester);

  public SnapshotManager(RaftStorage storage, RaftPeerId selfId) {
    this.storage = storage;
    this.selfId = selfId;
  }

  public void installSnapshot(StateMachine stateMachine,
      InstallSnapshotRequestProto request) throws IOException {
    final InstallSnapshotRequestProto.SnapshotChunkProto snapshotChunkRequest =
        request.getSnapshotChunk();
    final long lastIncludedIndex = snapshotChunkRequest.getTermIndex().getIndex();
    final RaftStorageDirectory dir = storage.getStorageDir();

    // create a unique temporary directory
    final File tmpDir =  new File(dir.getTmpDir(), "snapshot-" + snapshotChunkRequest.getRequestId());
    FileUtils.createDirectories(tmpDir);
    tmpDir.deleteOnExit();

    LOG.info("Installing snapshot:{}, to tmp dir:{}", request, tmpDir);

    // TODO: Make sure that subsequent requests for the same installSnapshot are coming in order,
    // and are not lost when whole request cycle is done. Check requestId and requestIndex here

    final Path stateMachineDir = dir.getStateMachineDir().toPath();
    for (FileChunkProto chunk : snapshotChunkRequest.getFileChunksList()) {
      SnapshotInfo pi = stateMachine.getLatestSnapshot();
      if (pi != null && pi.getTermIndex().getIndex() >= lastIncludedIndex) {
        throw new IOException("There exists snapshot file "
            + pi.getFiles() + " in " + selfId
            + " with endIndex >= lastIncludedIndex " + lastIncludedIndex);
      }

      String fileName = chunk.getFilename(); // this is relative to the root dir
      final Path relative = stateMachineDir.relativize(new File(dir.getRoot(), fileName).toPath());
      final File tmpSnapshotFile = new File(tmpDir, relative.toString());
      FileUtils.createDirectories(tmpSnapshotFile);

      FileOutputStream out = null;
      try {
        // if offset is 0, delete any existing temp snapshot file if it has the
        // same last index.
        if (chunk.getOffset() == 0) {
          if (tmpSnapshotFile.exists()) {
            FileUtils.deleteFully(tmpSnapshotFile);
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
        try (DigestOutputStream digestOut = new DigestOutputStream(out, digester.get())) {
          digestOut.write(chunk.getData().toByteArray());
        }
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
        final MD5Hash digest = new MD5Hash(digester.get().digest());
        if (!digest.equals(expectedDigest)) {
          LOG.warn("The snapshot md5 digest {} does not match expected {}",
              digest, expectedDigest);
          // rename the temp snapshot file to .corrupt
          String renameMessage;
          try {
            final File corruptedFile = FileUtils.move(tmpSnapshotFile, CORRUPT + StringUtils.currentDateTime());
            renameMessage = "Renamed temporary snapshot file " + tmpSnapshotFile + " to " + corruptedFile;
          } catch (IOException e) {
            renameMessage = "Tried but failed to rename temporary snapshot file " + tmpSnapshotFile
                + " to a " + CORRUPT + " file";
            LOG.warn(renameMessage, e);
            renameMessage += ": " + e;
          }
          throw new CorruptedFileException(tmpSnapshotFile,
              "MD5 mismatch for snapshot-" + lastIncludedIndex + " installation.  " + renameMessage);
        } else {
          MD5FileUtil.saveMD5File(tmpSnapshotFile, digest);
        }
      }
    }

    if (snapshotChunkRequest.getDone()) {
      rename(tmpDir, dir.getStateMachineDir());
    }
  }

  private static void rename(File tmpDir, File stateMachineDir) throws IOException {
    LOG.info("Installed snapshot, renaming temporary dir {} to {}", tmpDir, stateMachineDir);

    // rename stateMachineDir to tmp, if it exists.
    final File existingDir;
    if (stateMachineDir.exists()) {
      File moved = null;
      try {
        moved = FileUtils.move(stateMachineDir, TMP + StringUtils.currentDateTime());
      } catch(IOException e) {
        LOG.warn("Failed to rename state machine directory " + stateMachineDir.getAbsolutePath()
            + " to a " + TMP + " directory.  Try deleting it directly.", e);
        FileUtils.deleteFully(stateMachineDir);
      }
      existingDir = moved;
    } else {
      existingDir = null;
    }

    // rename tmpDir to stateMachineDir
    try {
      FileUtils.move(tmpDir, stateMachineDir);
    } catch (IOException e) {
      throw new IOException("Failed to rename temporary director " + tmpDir.getAbsolutePath()
          + " to " + stateMachineDir.getAbsolutePath(), e);
    }

    // delete existing dir
    if (existingDir != null) {
      try {
        FileUtils.deleteFully(existingDir);
      } catch (IOException e) {
        LOG.warn("Failed to delete existing directory " + existingDir.getAbsolutePath(), e);
      }
    }
  }
}
