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

import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotManagerTest {
  private static final class TestRaftStorageDirectory implements RaftStorageDirectory {
    private final File root;

    private TestRaftStorageDirectory(File root) {
      this.root = root;
    }

    @Override
    public File getRoot() {
      return root;
    }

    @Override
    public boolean isHealthy() {
      return true;
    }
  }

  private static final StateMachineStorage EMPTY_STORAGE = new StateMachineStorage() {
    @Override
    public void init(RaftStorage raftStorage) {
    }

    @Override
    public SnapshotInfo getLatestSnapshot() {
      return null;
    }

    @Override
    public void format() {
    }

    @Override
    public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) {
    }
  };

  private static InstallSnapshotRequestProto newSnapshotRequest(
      String requestId, int requestIndex, boolean requestDone, String filename, byte[] fullData,
      int offset, int chunkSize, boolean chunkDone) throws Exception {
    final FileChunkProto fileChunk = FileChunkProto.newBuilder()
        .setFilename(filename)
        .setTotalSize(fullData.length)
        .setFileDigest(ByteString.copyFrom(md5(fullData)))
        .setChunkIndex(requestIndex)
        .setOffset(offset)
        .setData(ByteString.copyFrom(fullData, offset, chunkSize))
        .setDone(chunkDone)
        .build();

    final InstallSnapshotRequestProto.SnapshotChunkProto snapshotChunk =
        InstallSnapshotRequestProto.SnapshotChunkProto.newBuilder()
            .setRequestId(requestId)
            .setRequestIndex(requestIndex)
            .setTermIndex(TermIndex.valueOf(1L, 10L).toProto())
            .addFileChunks(fileChunk)
            .setTotalSize(fullData.length)
            .setDone(requestDone)
            .build();

    return InstallSnapshotRequestProto.newBuilder()
        .setSnapshotChunk(snapshotChunk)
        .build();
  }

  private static byte[] md5(byte[] data) throws Exception {
    return MessageDigest.getInstance("MD5").digest(data);
  }

  @Test
  public void testAppendOnlyAndFinalizePublish() throws Exception {
    final File root = Files.createTempDirectory("snapshot-manager-test").toFile();
    try {
      final SnapshotManager manager = new SnapshotManager(
          RaftPeerId.valueOf("s1"), () -> new TestRaftStorageDirectory(root), EMPTY_STORAGE);
      final StateMachine stateMachine = mock(StateMachine.class);
      when(stateMachine.getLatestSnapshot()).thenReturn(null);

      final File stateMachineDir = new File(root, RaftStorageDirectory.STATE_MACHINE_DIR_NAME);
      FileUtils.createDirectories(stateMachineDir);
      final File oldSnapshot = new File(stateMachineDir, "old.snapshot");
      Files.write(oldSnapshot.toPath(), "old".getBytes(StandardCharsets.UTF_8));

      final byte[] fullData = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
      final File newSnapshot = new File(stateMachineDir, "new.snapshot");
      final String filename = new File(
          RaftStorageDirectory.STATE_MACHINE_DIR_NAME, "new.snapshot").toString();
      final File tmpSnapshot = new File(new File(
          new File(root, RaftStorageDirectory.TMP_DIR_NAME), "snapshot-request-1"), "new.snapshot");
      final InstallSnapshotRequestProto chunk0 = newSnapshotRequest(
          "request-1", 0, false, filename, fullData, 0, 8, false);
      final InstallSnapshotRequestProto chunk1 = newSnapshotRequest(
          "request-1", 1, true, filename, fullData, 8, 8, true);

      manager.appendSnapshot(chunk0, stateMachine);
      Assertions.assertTrue(oldSnapshot.exists());
      Assertions.assertTrue(tmpSnapshot.exists());
      Assertions.assertFalse(newSnapshot.exists());

      manager.appendSnapshot(chunk1, stateMachine);
      Assertions.assertTrue(oldSnapshot.exists());
      Assertions.assertTrue(tmpSnapshot.exists());
      Assertions.assertFalse(newSnapshot.exists());

      manager.finalizeSnapshot(chunk1);
      Assertions.assertFalse(oldSnapshot.exists());
      Assertions.assertFalse(tmpSnapshot.exists());
      Assertions.assertArrayEquals(fullData, Files.readAllBytes(newSnapshot.toPath()));
    } finally {
      FileUtils.deleteFully(root);
    }
  }

  @Test
  public void testFinalizeSnapshotRejectsIncompleteRequest() throws Exception {
    final File root = Files.createTempDirectory("snapshot-manager-test-incomplete").toFile();
    try {
      final SnapshotManager manager = new SnapshotManager(
          RaftPeerId.valueOf("s2"), () -> new TestRaftStorageDirectory(root), EMPTY_STORAGE);

      final InstallSnapshotRequestProto incomplete = newSnapshotRequest(
          "request-2", 0, false,
          new File(RaftStorageDirectory.STATE_MACHINE_DIR_NAME, "f.snapshot").toString(),
          "abc".getBytes(StandardCharsets.UTF_8), 0, 3, true);
      final IOException ioe = Assertions.assertThrows(IOException.class,
          () -> manager.finalizeSnapshot(incomplete));
      Assertions.assertTrue(ioe.getMessage().contains("Cannot finalize incomplete snapshot request"));
    } finally {
      FileUtils.deleteFully(root);
    }
  }
}
