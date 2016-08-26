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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.server.RaftServerConstants.StartupOption;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.RaftStorageDirectory.StorageState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test RaftStorage and RaftStorageDirectory
 */
public class TestRaftStorage {
  private File storageDir;
  private final RaftProperties properties = new RaftProperties();

  @Before
  public void setup() throws Exception {
    storageDir = RaftTestUtil.getTestDir(TestRaftStorage.class);
    properties.set(RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_KEY,
        storageDir.getCanonicalPath());
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtil.fullyDelete(storageDir.getParentFile());
    }
  }

  @Test
  public void testNotExistent() throws IOException {
    FileUtil.fullyDelete(storageDir);

    // we will format the empty directory
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());

    try {
      new RaftStorage(properties, StartupOption.FORMAT).close();
      Assert.fail("the format should fail since the storage is still locked");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("directory is already locked"));
    }

    storage.close();
    FileUtil.fullyDelete(storageDir);
    Assert.assertTrue(storageDir.createNewFile());
    try {
      new RaftStorage(properties, StartupOption.REGULAR);
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

    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
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
    storage = new RaftStorage(properties, StartupOption.FORMAT);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());
    metaFile = new MetaFile(sd.getMetaFile());
    Assert.assertEquals(MetaFile.DEFAULT_TERM, metaFile.getTerm());
    Assert.assertEquals(MetaFile.EMPTY_VOTEFOR, metaFile.getVotedFor());
    storage.close();
  }

  @Test
  public void testMetaFile() throws Exception {
    RaftStorage storage = new RaftStorage(properties, StartupOption.FORMAT);
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
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    Assert.assertEquals(StorageState.NORMAL, storage.getState());
    storage.close();

    RaftStorageDirectory sd = new RaftStorageDirectory(storageDir);
    File metaFile = sd.getMetaFile();
    NativeIO.renameTo(metaFile, sd.getMetaTmpFile());

    Assert.assertEquals(StorageState.NOT_FORMATTED, sd.analyzeStorage(false));

    try {
      new RaftStorage(properties, StartupOption.REGULAR);
      Assert.fail("should throw IOException since storage dir is not formatted");
    } catch (IOException e) {
      Assert.assertTrue(
          e.getMessage().contains(StorageState.NOT_FORMATTED.name()));
    }

    // let the storage dir contain both raft-meta and raft-meta.tmp
    new RaftStorage(properties, StartupOption.FORMAT).close();
    Assert.assertTrue(sd.getMetaFile().exists());
    Assert.assertTrue(sd.getMetaTmpFile().createNewFile());
    Assert.assertTrue(sd.getMetaTmpFile().exists());
    try {
      storage = new RaftStorage(properties, StartupOption.REGULAR);
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
    final String name = RaftStorageDirectory.getSnapshotFileName(term, index);
    System.out.println("name = " + name);
    final File file = new File(storageDir, name);
    final TermIndex ti = RaftStorageDirectory.getTermIndexFromSnapshotFile(file);
    System.out.println("file = " + file);
    Assert.assertEquals(term, ti.getTerm());
    Assert.assertEquals(index, ti.getIndex());
    System.out.println("ti = " + ti);

    final File foo = new File(storageDir, "foo");
    try {
      RaftStorageDirectory.getTermIndexFromSnapshotFile(foo);
      Assert.fail();
    } catch(IllegalArgumentException iae) {
      System.out.println("Good " + iae);
    }
  }
}
