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

import static org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.SNAPSHOT_REGEX;

import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerConstants.StartupOption;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorageDirectory.StorageState;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SnapshotRetentionPolicy;
import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;

/**
 * Test RaftStorage and RaftStorageDirectory
 */
public class TestRaftStorage extends BaseTest {
  private File storageDir;

  @Before
  public void setup() throws Exception {
    storageDir = getTestDir();
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtils.deleteFully(storageDir.getParentFile());
    }
  }

  @Test
  public void testNotExistent() throws IOException {
    FileUtils.deleteFully(storageDir);

    // we will format the empty directory
    RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());

    try {
      new RaftStorage(storageDir, StartupOption.FORMAT).close();
      Assert.fail("the format should fail since the storage is still locked");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("directory is already locked"));
    }

    storage.close();
    FileUtils.deleteFully(storageDir);
    Assert.assertTrue(storageDir.createNewFile());
    try {
      new RaftStorage(storageDir, StartupOption.REGULAR);
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
    RaftStorageDirectory sd = new RaftStorageDirectory(storageDir);
    try {
      StorageState state = sd.analyzeStorage(true);
      Assert.assertEquals(StorageState.NOT_FORMATTED, state);
      Assert.assertTrue(sd.isCurrentEmpty());
    } finally {
      sd.unlock();
    }

    RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());
    storage.close();

    Assert.assertEquals(StorageState.NORMAL, sd.analyzeStorage(false));
    File m = sd.getMetaFile();
    Assert.assertTrue(m.exists());
    MetaFile metaFile = new MetaFile(m);
    Assert.assertEquals(MetaFile.DEFAULT_TERM, metaFile.getTerm());
    Assert.assertEquals(MetaFile.EMPTY_VOTEFOR, metaFile.getVotedFor());

    metaFile.set(123, "peer1");
    metaFile.readFile();
    Assert.assertEquals(123, metaFile.getTerm());
    Assert.assertEquals("peer1", metaFile.getVotedFor());

    MetaFile metaFile2 = new MetaFile(m);
    Assert.assertFalse((Boolean) Whitebox.getInternalState(metaFile2, "loaded"));
    Assert.assertEquals(123, metaFile.getTerm());
    Assert.assertEquals("peer1", metaFile.getVotedFor());

    // test format
    storage = new RaftStorage(storageDir, StartupOption.FORMAT);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());
    metaFile = new MetaFile(sd.getMetaFile());
    Assert.assertEquals(MetaFile.DEFAULT_TERM, metaFile.getTerm());
    Assert.assertEquals(MetaFile.EMPTY_VOTEFOR, metaFile.getVotedFor());
    storage.close();
  }

  @Test
  public void testMetaFile() throws Exception {
    RaftStorage storage = new RaftStorage(storageDir, StartupOption.FORMAT);
    File m = storage.getStorageDir().getMetaFile();
    Assert.assertTrue(m.exists());
    MetaFile metaFile = new MetaFile(m);
    Assert.assertEquals(MetaFile.DEFAULT_TERM, metaFile.getTerm());
    Assert.assertEquals(MetaFile.EMPTY_VOTEFOR, metaFile.getVotedFor());

    metaFile.set(123, "peer1");
    metaFile.readFile();
    Assert.assertEquals(123, metaFile.getTerm());
    Assert.assertEquals("peer1", metaFile.getVotedFor());

    MetaFile metaFile2 = new MetaFile(m);
    Assert.assertFalse((Boolean) Whitebox.getInternalState(metaFile2, "loaded"));
    Assert.assertEquals(123, metaFile.getTerm());
    Assert.assertEquals("peer1", metaFile.getVotedFor());

    storage.close();
  }

  /**
   * check if RaftStorage deletes tmp metafile when startup
   */
  @Test
  public void testCleanMetaTmpFile() throws Exception {
    RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());
    storage.close();

    RaftStorageDirectory sd = new RaftStorageDirectory(storageDir);
    File metaFile = sd.getMetaFile();
    FileUtils.move(metaFile, sd.getMetaTmpFile());

    Assert.assertEquals(StorageState.NOT_FORMATTED, sd.analyzeStorage(false));

    try {
      new RaftStorage(storageDir, StartupOption.REGULAR);
      Assert.fail("should throw IOException since storage dir is not formatted");
    } catch (IOException e) {
      Assert.assertTrue(
          e.getMessage().contains(StorageState.NOT_FORMATTED.name()));
    }

    // let the storage dir contain both raft-meta and raft-meta.tmp
    new RaftStorage(storageDir, StartupOption.FORMAT).close();
    Assert.assertTrue(sd.getMetaFile().exists());
    Assert.assertTrue(sd.getMetaTmpFile().createNewFile());
    Assert.assertTrue(sd.getMetaTmpFile().exists());
    try {
      storage = new RaftStorage(storageDir, StartupOption.REGULAR);
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
    RaftStorage storage = new RaftStorage(storageDir, StartupOption.REGULAR);
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
