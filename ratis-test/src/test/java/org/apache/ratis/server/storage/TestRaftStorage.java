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

import static java.util.stream.Collectors.toList;
import static org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.SNAPSHOT_MD5_REGEX;
import static org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.SNAPSHOT_REGEX;
import static org.apache.ratis.util.MD5FileUtil.MD5_SUFFIX;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorageDirectoryImpl.StorageState;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;

/**
 * Test RaftStorage and RaftStorageDirectory
 */
public class TestRaftStorage extends BaseTest {
  static RaftStorageImpl newRaftStorage(File dir) throws IOException {
    final RaftStorage impl = RaftStorage.newBuilder()
        .setDirectory(dir)
        .setOption(RaftStorage.StartupOption.RECOVER)
        .setStorageFreeSpaceMin(RaftServerConfigKeys.STORAGE_FREE_SPACE_MIN_DEFAULT)
        .build();
    impl.initialize();
    return Preconditions.assertInstanceOf(impl, RaftStorageImpl.class);
  }

  private File storageDir;

  @BeforeEach
  public void setup() {
    storageDir = getTestDir();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtils.deleteFully(storageDir.getParentFile());
    }
  }

  static RaftStorageImpl formatRaftStorage(File dir) throws IOException {
    final RaftStorageImpl impl = (RaftStorageImpl) RaftStorage.newBuilder()
        .setDirectory(dir)
        .setOption(RaftStorage.StartupOption.FORMAT)
        .setStorageFreeSpaceMin(SizeInBytes.valueOf(0))
        .build();
    impl.initialize();
    return impl;
  }

  @SuppressWarnings({"squid:S5783"}) // Suppress same exception warning
  @Test
  public void testNotExistent() throws IOException {
    FileUtils.deleteFully(storageDir);

    // we will format the empty directory
    final RaftStorageImpl storage = newRaftStorage(storageDir);
    Assertions.assertEquals(StorageState.NORMAL, storage.getState());

    try {
      formatRaftStorage(storageDir).close();
      Assertions.fail("the format should fail since the storage is still locked");
    } catch (IOException e) {
      Assertions.assertTrue(e.getMessage().contains("directory is already locked"));
    }

    storage.close();
    FileUtils.deleteFully(storageDir);
    Assertions.assertTrue(storageDir.createNewFile());
    try (RaftStorage ignored = newRaftStorage(storageDir)) {
      Assertions.fail();
    } catch (IOException e) {
      Assertions.assertTrue(
          e.getMessage().contains(StorageState.NON_EXISTENT.name()));
    }
  }

  /**
   * make sure the RaftStorage format works
   */
  @Test
  public void testStorage() throws Exception {
    final RaftStorageDirectoryImpl sd = new RaftStorageDirectoryImpl(storageDir, SizeInBytes.ZERO);
    try {
      StorageState state = sd.analyzeStorage(true);
      Assertions.assertEquals(StorageState.NOT_FORMATTED, state);
      Assertions.assertTrue(sd.isCurrentEmpty());
    } finally {
      sd.unlock();
    }

    RaftStorageImpl storage = newRaftStorage(storageDir);
    Assertions.assertEquals(StorageState.NORMAL, storage.getState());
    storage.close();

    Assertions.assertEquals(StorageState.NORMAL, sd.analyzeStorage(false));
    assertMetadataFile(sd.getMetaFile());

    // test format
    storage = formatRaftStorage(storageDir);
    Assertions.assertEquals(StorageState.NORMAL, storage.getState());
    final RaftStorageMetadataFile metaFile = new RaftStorageMetadataFileImpl(sd.getMetaFile());
    Assertions.assertEquals(RaftStorageMetadata.getDefault(), metaFile.getMetadata());
    storage.close();
  }

  static void assertMetadataFile(File m) throws Exception {
    Assertions.assertTrue(m.exists());
    final RaftStorageMetadataFile metaFile = new RaftStorageMetadataFileImpl(m);
    Assertions.assertEquals(RaftStorageMetadata.getDefault(), metaFile.getMetadata());

    final RaftPeerId peer1 = RaftPeerId.valueOf("peer1");
    final RaftStorageMetadata metadata = RaftStorageMetadata.valueOf(123, peer1);
    metaFile.persist(metadata);
    Assertions.assertEquals(metadata.getTerm(), 123);
    Assertions.assertEquals(metadata.getVotedFor(), peer1);
    Assertions.assertEquals(metadata, metaFile.getMetadata());

    final RaftStorageMetadataFile metaFile2 = new RaftStorageMetadataFileImpl(m);
    Assertions.assertNull(((AtomicReference<?>) RaftTestUtil.getDeclaredField(metaFile2, "metadata")).get());
    Assertions.assertEquals(metadata, metaFile2.getMetadata());
  }

  @Test
  public void testMetaFile() throws Exception {
    final RaftStorageImpl storage = formatRaftStorage(storageDir);
    assertMetadataFile(storage.getStorageDir().getMetaFile());
    storage.close();
  }

  /**
   * check if RaftStorage deletes tmp metafile when startup
   */
  @Test
  public void testCleanMetaTmpFile() throws Exception {
    RaftStorageImpl storage = newRaftStorage(storageDir);
    Assertions.assertEquals(StorageState.NORMAL, storage.getState());
    storage.close();

    final RaftStorageDirectoryImpl sd = new RaftStorageDirectoryImpl(storageDir, SizeInBytes.ZERO);
    File metaFile = sd.getMetaFile();
    FileUtils.move(metaFile, sd.getMetaTmpFile());

    Assertions.assertEquals(StorageState.NOT_FORMATTED, sd.analyzeStorage(false));

    // RaftStorage initialization should succeed as the raft-meta.tmp is
    // always cleaned.
    newRaftStorage(storageDir).close();

    Assertions.assertTrue(sd.getMetaFile().exists());
    Assertions.assertTrue(sd.getMetaTmpFile().createNewFile());
    Assertions.assertTrue(sd.getMetaTmpFile().exists());
    try {
      storage = newRaftStorage(storageDir);
      Assertions.assertEquals(StorageState.NORMAL, storage.getState());
      Assertions.assertFalse(sd.getMetaTmpFile().exists());
      Assertions.assertTrue(sd.getMetaFile().exists());
    } finally {
      storage.close();
    }
  }

  @Test
  public void testSnapshotFileName() {
    final long term = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    final long index = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    final String name = SimpleStateMachineStorage.getSnapshotFileName(term, index);
    System.out.println("name = " + name);
    final File file = new File(storageDir, name);
    final TermIndex ti = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(file);
    System.out.println("file = " + file);
    Assertions.assertEquals(term, ti.getTerm());
    Assertions.assertEquals(index, ti.getIndex());
    System.out.println("ti = " + ti);

    final File foo = new File(storageDir, "foo");
    try {
      SimpleStateMachineStorage.getTermIndexFromSnapshotFile(foo);
      Assertions.fail();
    } catch(IllegalArgumentException iae) {
      System.out.println("Good " + iae);
    }
  }

  @Test
  public void testSnapshotCleanup() throws IOException {


    SnapshotRetentionPolicy snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return 2;
      }
    };


    SimpleStateMachineStorage simpleStateMachineStorage = new SimpleStateMachineStorage();
    final RaftStorage storage = newRaftStorage(storageDir);
    simpleStateMachineStorage.init(storage);

    Set<TermIndex> termIndexSet = new HashSet<>();

    //Create 3 snapshot files in storage dir.
    while (termIndexSet.size() < 3) {
      final long term = ThreadLocalRandom.current().nextLong(1, 10L);
      final long index = ThreadLocalRandom.current().nextLong(100, 500L);
      if (termIndexSet.add(TermIndex.valueOf(term, index))) {
        createSnapshot(simpleStateMachineStorage, term, index, true);
      }
    }

    // Create 2 more snapshot files in storage dir without MD5 files
    while (termIndexSet.size() < 5) {
      final long term = ThreadLocalRandom.current().nextLong(11, 20L);
      final long index = ThreadLocalRandom.current().nextLong(501, 1000L);
      if (termIndexSet.add(TermIndex.valueOf(term, index))) {
        createSnapshot(simpleStateMachineStorage, term, index, false);
      }
    }

    // create MD5 files that will not be deleted in older version
    while (termIndexSet.size() < 7) {
      final long term = 1;
      final long index = ThreadLocalRandom.current().nextLong(0, 100L);
      if (termIndexSet.add(TermIndex.valueOf(term, index))) {
        File file = simpleStateMachineStorage.getSnapshotFile(term, index);
        File snapshotFile = new File(file.getParent(), file.getName() + MD5_SUFFIX);
        Assertions.assertTrue(snapshotFile.createNewFile());
      }
    }

    File stateMachineDir = storage.getStorageDir().getStateMachineDir();
    assertFileCount(stateMachineDir, 10);

    simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);

    // Since the MD5 files are not matching the snapshot files they are cleaned up.
    // So we still have 6 files - 4 snapshots and 2 MD5 files.
    File[] remainingFiles = assertFileCount(stateMachineDir, 6);

    List<Long> remainingIndices = termIndexSet.stream()
        .map(TermIndex::getIndex)
        .sorted(Collections.reverseOrder())
        .limit(4)
        .collect(toList());
    for (File file : remainingFiles) {
      System.out.println(file.getName());
      Matcher matcher = SNAPSHOT_REGEX.matcher(file.getName());
      if (matcher.matches()) {
        Assertions.assertTrue(remainingIndices.contains(Long.parseLong(matcher.group(2))));
      }
    }

    // Attempt to clean up again should not delete any more files.
    simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);
    assertFileCount(stateMachineDir, 6);

    //Test with Retention disabled.
    //Create 2 snapshot files in storage dir.
    for (int i = 0; i < 2; i++) {
      final long term = ThreadLocalRandom.current().nextLong(21, 30L);
      final long index = ThreadLocalRandom.current().nextLong(1000L);
      createSnapshot(simpleStateMachineStorage, term, index, false);
    }

    simpleStateMachineStorage.cleanupOldSnapshots(new SnapshotRetentionPolicy() { });
    assertFileCount(stateMachineDir, 8);
  }

  @Test
  public void testSnapshotCleanupWithMissingMd5File() throws IOException {

    SnapshotRetentionPolicy snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return 2;
      }
    };


    SimpleStateMachineStorage simpleStateMachineStorage = new SimpleStateMachineStorage();
    final RaftStorage storage = newRaftStorage(storageDir);
    simpleStateMachineStorage.init(storage);

    Set<TermIndex> termIndexSet = new HashSet<>();

    // Create one snapshot file without MD5 file
    if (termIndexSet.add(TermIndex.valueOf(1, 100))) {
      createSnapshot(simpleStateMachineStorage, 1, 100, false);
    }

    //Create 4 snapshot files in storage dir
    while (termIndexSet.size() < 5) {
      final long term = ThreadLocalRandom.current().nextLong(2, 10L);
      final long index = ThreadLocalRandom.current().nextLong(100, 1000L);
      if (termIndexSet.add(TermIndex.valueOf(term, index))) {
        createSnapshot(simpleStateMachineStorage, term, index, true);
      }
    }

    // 1 snapshot file without MD5 hash, 4 snapshots + 4 md5 hash files = 9 files
    File stateMachineDir = storage.getStorageDir().getStateMachineDir();
    assertFileCount(stateMachineDir, 9);

    simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);

    // We should have 4 files remaining, and 2 snapshots with MD5 hash
    assertFileCount(stateMachineDir, 4);
  }

  @Test
  public void testSnapshotCleanupWithLatestSnapshotMissingMd5File() throws IOException {

    SnapshotRetentionPolicy snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return 2;
      }
    };


    SimpleStateMachineStorage simpleStateMachineStorage = new SimpleStateMachineStorage();
    final RaftStorage storage = newRaftStorage(storageDir);
    simpleStateMachineStorage.init(storage);

    Set<TermIndex> termIndexSet = new HashSet<>();

    //Create 4 snapshot files in storage dir
    while (termIndexSet.size() < 4) {
      final long term = ThreadLocalRandom.current().nextLong(1, 10L);
      final long index = ThreadLocalRandom.current().nextLong(100, 1000L);
      if (termIndexSet.add(TermIndex.valueOf(term, index))) {
        createSnapshot(simpleStateMachineStorage, term, index, true);
      }
    }

    // Create a snapshot file with a missing MD5 file and having the highest term index
    if (termIndexSet.add(TermIndex.valueOf(99, 1001))) {
      createSnapshot(simpleStateMachineStorage, 99, 1001, false);
    }

    // 1 snapshot file without MD5 hash, 4 snapshots + 4 md5 hash files = 9 files
    File stateMachineDir = storage.getStorageDir().getStateMachineDir();
    assertFileCount(stateMachineDir, 9);

    simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);

    // We should have 5 files remaining, and 2 snapshots with MD5 hash and 1 snapshot file without MD5 hash
    assertFileCount(stateMachineDir, 5);
  }

  @Test
  public void testCleanupOldSnapshotsDeletesOlderSnapshotsWithMd5() throws Exception {
    SnapshotRetentionPolicy snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return 2;
      }
    };

    SimpleStateMachineStorage simpleStateMachineStorage = new SimpleStateMachineStorage();
    final RaftStorage storage = newRaftStorage(storageDir);
    simpleStateMachineStorage.init(storage);
    try {
      createSnapshot(simpleStateMachineStorage, 1, 100, true);
      createSnapshot(simpleStateMachineStorage, 1, 200, true);
      createSnapshot(simpleStateMachineStorage, 1, 300, true);
      createSnapshot(simpleStateMachineStorage, 1, 400, true);

      File stateMachineDir = storage.getStorageDir().getStateMachineDir();
      simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);

      List<String> snapshotNames = listMatchingFileNames(stateMachineDir, SNAPSHOT_REGEX);
      Assertions.assertEquals(2, snapshotNames.size());
      Assertions.assertTrue(snapshotNames.contains(SimpleStateMachineStorage.getSnapshotFileName(1, 400)));
      Assertions.assertTrue(snapshotNames.contains(SimpleStateMachineStorage.getSnapshotFileName(1, 300)));
      Assertions.assertFalse(snapshotNames.contains(SimpleStateMachineStorage.getSnapshotFileName(1, 200)));
      Assertions.assertFalse(snapshotNames.contains(SimpleStateMachineStorage.getSnapshotFileName(1, 100)));

      List<String> md5Names = listMatchingFileNames(stateMachineDir, SNAPSHOT_MD5_REGEX);
      Assertions.assertEquals(2, md5Names.size());
      Assertions.assertTrue(md5Names.contains(SimpleStateMachineStorage.getSnapshotFileName(1, 400) + MD5_SUFFIX));
      Assertions.assertTrue(md5Names.contains(SimpleStateMachineStorage.getSnapshotFileName(1, 300) + MD5_SUFFIX));
      Assertions.assertFalse(md5Names.contains(SimpleStateMachineStorage.getSnapshotFileName(1, 200) + MD5_SUFFIX));
      Assertions.assertFalse(md5Names.contains(SimpleStateMachineStorage.getSnapshotFileName(1, 100) + MD5_SUFFIX));
    } finally {
      storage.close();
    }
  }

  @Test
  public void testCleanupOldSnapshotsWithoutAnyMd5() throws Exception {
    SnapshotRetentionPolicy snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return 2;
      }
    };

    SimpleStateMachineStorage simpleStateMachineStorage = new SimpleStateMachineStorage();
    final RaftStorage storage = newRaftStorage(storageDir);
    simpleStateMachineStorage.init(storage);
    try {
      createSnapshot(simpleStateMachineStorage, 1, 100, false);
      createSnapshot(simpleStateMachineStorage, 1, 200, false);
      createSnapshot(simpleStateMachineStorage, 1, 300, false);

      File stateMachineDir = storage.getStorageDir().getStateMachineDir();
      simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);

      List<String> snapshotNames = listMatchingFileNames(stateMachineDir, SNAPSHOT_REGEX);
      Assertions.assertEquals(3, snapshotNames.size());
      Assertions.assertTrue(listMatchingFileNames(stateMachineDir, SNAPSHOT_MD5_REGEX).isEmpty());
    } finally {
      storage.close();
    }
  }

  @Test
  public void testGetLatestSnapshotReturnsNewest() throws Exception {
    SimpleStateMachineStorage simpleStateMachineStorage = new SimpleStateMachineStorage();
    final RaftStorage storage = newRaftStorage(storageDir);
    simpleStateMachineStorage.init(storage);
    try {
      Assertions.assertNull(simpleStateMachineStorage.getLatestSnapshot());

      createSnapshot(simpleStateMachineStorage, 1, 100, true);
      simpleStateMachineStorage.loadLatestSnapshot();
      SingleFileSnapshotInfo first = simpleStateMachineStorage.getLatestSnapshot();
      Assertions.assertNotNull(first);
      Assertions.assertEquals(1, first.getTerm());
      Assertions.assertEquals(100, first.getIndex());
      Assertions.assertNotNull(first.getFile().getFileDigest());

      createSnapshot(simpleStateMachineStorage, 1, 200, true);
      simpleStateMachineStorage.loadLatestSnapshot();
      SingleFileSnapshotInfo second = simpleStateMachineStorage.getLatestSnapshot();
      Assertions.assertNotNull(second);
      Assertions.assertEquals(1, second.getTerm());
      Assertions.assertEquals(200, second.getIndex());
      Assertions.assertNotNull(second.getFile().getFileDigest());
    } finally {
      storage.close();
    }
  }

  @Test
  public void testGetLatestSnapshotIgnoresSnapshotsWithoutMd5() throws Exception {
    SimpleStateMachineStorage simpleStateMachineStorage = new SimpleStateMachineStorage();
    final RaftStorage storage = newRaftStorage(storageDir);
    simpleStateMachineStorage.init(storage);
    try {
      createSnapshot(simpleStateMachineStorage, 1, 100, true);
      simpleStateMachineStorage.loadLatestSnapshot();

      createSnapshot(simpleStateMachineStorage, 1, 200, false);
      simpleStateMachineStorage.loadLatestSnapshot();

      SingleFileSnapshotInfo latest = simpleStateMachineStorage.getLatestSnapshot();
      Assertions.assertNotNull(latest);
      Assertions.assertEquals(100, latest.getIndex());
      Assertions.assertEquals(1, latest.getTerm());
    } finally {
      storage.close();
    }
  }

  private static File[] assertFileCount(File dir, int expected) {
    File[] files = dir.listFiles();
    Assertions.assertNotNull(files);
    Assertions.assertEquals(expected, files.length, Arrays.toString(files));
    return files;
  }

  private File createSnapshot(SimpleStateMachineStorage storage,
                              long term, long endIndex,
                              boolean withMd5) throws IOException {
    File snapshotFile = storage.getSnapshotFile(term, endIndex);
    Assertions.assertTrue(snapshotFile.createNewFile());

    if (withMd5) {
      MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    }

    return snapshotFile;
  }

  private static List<String> listMatchingFileNames(File dir, java.util.regex.Pattern pattern) {
    return Arrays.stream(Objects.requireNonNull(dir.list()))
        .filter(name -> pattern.matcher(name).matches())
        .collect(toList());
  }

  @Test
  public void testNotEnoughSpace() throws IOException {
    File mockStorageDir = Mockito.spy(storageDir);
    Mockito.when(mockStorageDir.getFreeSpace()).thenReturn(100L);  // 100B

    final RaftStorageDirectoryImpl sd = new RaftStorageDirectoryImpl(mockStorageDir, SizeInBytes.valueOf("100M"));
    StorageState state = sd.analyzeStorage(false);
    Assertions.assertEquals(StorageState.NO_SPACE, state);
  }
}
