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

import static org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.SNAPSHOT_REGEX;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorageDirectoryImpl.StorageState;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;

/**
 * Test RaftStorage and RaftStorageDirectory
 */
public class TestRaftStorage extends BaseTest {
  static RaftStorageImpl newRaftStorage(File dir) throws IOException {
    return new RaftStorageImpl(dir, null);
  }

  private File storageDir;

  @Before
  public void setup() {
    storageDir = getTestDir();
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtils.deleteFully(storageDir.getParentFile());
    }
  }

  static RaftStorageImpl formatRaftStorage(File dir) throws IOException {
    return new RaftStorageImpl(dir, null, RaftStorageImpl.StartupOption.FORMAT);
  }

  @Test
  public void testNotExistent() throws IOException {
    FileUtils.deleteFully(storageDir);

    // we will format the empty directory
    final RaftStorageImpl storage = newRaftStorage(storageDir);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());

    try {
      formatRaftStorage(storageDir).close();
      Assert.fail("the format should fail since the storage is still locked");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("directory is already locked"));
    }

    storage.close();
    FileUtils.deleteFully(storageDir);
    Assert.assertTrue(storageDir.createNewFile());
    try {
      newRaftStorage(storageDir);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(
          e.getMessage().contains(StorageState.NON_EXISTENT.name()));
    }
  }

  /**
   * make sure the RaftStorage format works
   */
  @Test
  public void testStorage() throws Exception {
    final RaftStorageDirectoryImpl sd = new RaftStorageDirectoryImpl(storageDir);
    try {
      StorageState state = sd.analyzeStorage(true);
      Assert.assertEquals(StorageState.NOT_FORMATTED, state);
      Assert.assertTrue(sd.isCurrentEmpty());
    } finally {
      sd.unlock();
    }

    RaftStorageImpl storage = newRaftStorage(storageDir);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());
    storage.close();

    Assert.assertEquals(StorageState.NORMAL, sd.analyzeStorage(false));
    assertMetadataFile(sd.getMetaFile());

    // test format
    storage = formatRaftStorage(storageDir);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());
    final RaftStorageMetadataFile metaFile = new RaftStorageMetadataFileImpl(sd.getMetaFile());
    Assert.assertEquals(RaftStorageMetadata.getDefault(), metaFile.getMetadata());
    storage.close();
  }

  static void assertMetadataFile(File m) throws Exception {
    Assert.assertTrue(m.exists());
    final RaftStorageMetadataFile metaFile = new RaftStorageMetadataFileImpl(m);
    Assert.assertEquals(RaftStorageMetadata.getDefault(), metaFile.getMetadata());

    final RaftPeerId peer1 = RaftPeerId.valueOf("peer1");
    final RaftStorageMetadata metadata = RaftStorageMetadata.valueOf(123, peer1);
    metaFile.persist(metadata);
    Assert.assertEquals(metadata.getTerm(), 123);
    Assert.assertEquals(metadata.getVotedFor(), peer1);
    Assert.assertEquals(metadata, metaFile.getMetadata());

    final RaftStorageMetadataFile metaFile2 = new RaftStorageMetadataFileImpl(m);
    Assert.assertNull(((AtomicReference<?>) RaftTestUtil.getDeclaredField(metaFile2, "metadata")).get());
    Assert.assertEquals(metadata, metaFile2.getMetadata());
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
    Assert.assertEquals(StorageState.NORMAL, storage.getState());
    storage.close();

    final RaftStorageDirectoryImpl sd = new RaftStorageDirectoryImpl(storageDir);
    File metaFile = sd.getMetaFile();
    FileUtils.move(metaFile, sd.getMetaTmpFile());

    Assert.assertEquals(StorageState.NOT_FORMATTED, sd.analyzeStorage(false));

    // RaftStorage initialization should succeed as the raft-meta.tmp is
    // always cleaned.
    newRaftStorage(storageDir).close();

    Assert.assertTrue(sd.getMetaFile().exists());
    Assert.assertTrue(sd.getMetaTmpFile().createNewFile());
    Assert.assertTrue(sd.getMetaTmpFile().exists());
    try {
      storage = newRaftStorage(storageDir);
      Assert.assertEquals(StorageState.NORMAL, storage.getState());
      Assert.assertFalse(sd.getMetaTmpFile().exists());
      Assert.assertTrue(sd.getMetaFile().exists());
    } finally {
      storage.close();
    }
  }

  @Test
  public void testSnapshotFileName() throws Exception {
    final long term = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    final long index = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    final String name = SimpleStateMachineStorage.getSnapshotFileName(term, index);
    System.out.println("name = " + name);
    final File file = new File(storageDir, name);
    final TermIndex ti = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(file);
    System.out.println("file = " + file);
    Assert.assertEquals(term, ti.getTerm());
    Assert.assertEquals(index, ti.getIndex());
    System.out.println("ti = " + ti);

    final File foo = new File(storageDir, "foo");
    try {
      SimpleStateMachineStorage.getTermIndexFromSnapshotFile(foo);
      Assert.fail();
    } catch(IllegalArgumentException iae) {
      System.out.println("Good " + iae);
    }
  }

  @Test
  public void testSnapshotCleanup() throws IOException {


    SnapshotRetentionPolicy snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return 3;
      }
    };


    SimpleStateMachineStorage simpleStateMachineStorage = new SimpleStateMachineStorage();
    final RaftStorage storage = newRaftStorage(storageDir);
    simpleStateMachineStorage.init(storage);

    List<Long> indices = new ArrayList<>();

    //Create 5 snapshot files in storage dir.
    for (int i = 0; i < 5; i++) {
      final long term = ThreadLocalRandom.current().nextLong(10L);
      final long index = ThreadLocalRandom.current().nextLong(1000L);
      indices.add(index);
      File file = simpleStateMachineStorage.getSnapshotFile(term, index);
      file.createNewFile();
    }

    File stateMachineDir = storage.getStorageDir().getStateMachineDir();
    Assert.assertTrue(stateMachineDir.listFiles().length == 5);
    simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);
    File[] remainingFiles = stateMachineDir.listFiles();
    Assert.assertTrue(remainingFiles.length == 3);

    Collections.sort(indices);
    Collections.reverse(indices);
    List<Long> remainingIndices = indices.subList(0, 3);
    for (File file : remainingFiles) {
      System.out.println(file.getName());
      Matcher matcher = SNAPSHOT_REGEX.matcher(file.getName());
      if (matcher.matches()) {
        Assert.assertTrue(remainingIndices.contains(Long.parseLong(matcher.group(2))));
      }
    }

    // Attempt to clean up again should not delete any more files.
    simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);
    remainingFiles = stateMachineDir.listFiles();
    Assert.assertTrue(remainingFiles.length == 3);

    //Test with Retention disabled.
    //Create 2 snapshot files in storage dir.
    for (int i = 0; i < 2; i++) {
      final long term = ThreadLocalRandom.current().nextLong(10L);
      final long index = ThreadLocalRandom.current().nextLong(1000L);
      indices.add(index);
      File file = simpleStateMachineStorage.getSnapshotFile(term, index);
      file.createNewFile();
    }

    simpleStateMachineStorage.cleanupOldSnapshots(new SnapshotRetentionPolicy() {
    });
    Assert.assertTrue(stateMachineDir.listFiles().length == 5);

  }
}
