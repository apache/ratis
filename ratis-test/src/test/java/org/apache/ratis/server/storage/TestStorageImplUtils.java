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

import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Test cases to verify ServerState.
 */
public class TestStorageImplUtils extends BaseTest {

  private static final Supplier<File> rootTestDir = JavaUtils.memoize(
      () -> new File(BaseTest.getRootTestDir(),
          JavaUtils.getClassSimpleName(TestStorageImplUtils.class) +
              Integer.toHexString(ThreadLocalRandom.current().nextInt())));

  static File chooseNewStorageDir(List<File> volumes, String sub) throws IOException {
    final Map<File, Integer> numDirPerVolume = new HashMap<>();
    StorageImplUtils.getExistingStorageSubs(volumes, sub, numDirPerVolume);
    final File vol = StorageImplUtils.chooseMin(numDirPerVolume);
    return new File(vol, sub);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    FileUtils.deleteFully(rootTestDir.get());
  }

  /**
   * Tests choosing of storage directory when only one volume is configured.
   *
   * @throws IOException in case of exception.
   */
  @Test
  public void testChooseStorageDirWithOneVolume() throws IOException {
    File testDir = new File(rootTestDir.get(), UUID.randomUUID().toString());
    List<File> directories = Collections.singletonList(testDir);
    String subDirOne = UUID.randomUUID().toString();
    String subDirTwo = UUID.randomUUID().toString();
    final File storageDirOne = chooseNewStorageDir(directories, subDirOne);
    final File storageDirTwo = chooseNewStorageDir(directories, subDirTwo);
    File expectedOne = new File(testDir, subDirOne);
    File expectedTwo = new File(testDir, subDirTwo);
    Assertions.assertEquals(expectedOne.getCanonicalPath(),
        storageDirOne.getCanonicalPath());
    Assertions.assertEquals(expectedTwo.getCanonicalPath(),
        storageDirTwo.getCanonicalPath());
  }

  /**
   * Tests choosing of storage directory when multiple volumes are configured.
   *
   * @throws IOException in case of exception.
   */
  @Test
  public void testChooseStorageDirWithMultipleVolumes() throws IOException {
    File testDir = new File(rootTestDir.get(), UUID.randomUUID().toString());
    List<File> directories = new ArrayList<>();
    IntStream.range(0, 10).mapToObj((i) -> new File(testDir,
        Integer.toString(i))).forEach((dir) -> {
      try {
        FileUtils.createDirectories(dir);
        directories.add(dir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    directories.stream().filter((dir) -> Integer.parseInt(dir.getName()) != 6)
        .forEach(
            (dir) -> {
              try {
                FileUtils.createDirectories(
                    new File(dir, UUID.randomUUID().toString()));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    String subDir = UUID.randomUUID().toString();
    final File storageDirectory = chooseNewStorageDir(directories, subDir);
    File expected = new File(directories.get(6), subDir);
    Assertions.assertEquals(expected.getCanonicalPath(),
        storageDirectory.getCanonicalPath());
  }

  /**
   * Tests choosing of storage directory when only no volume is configured.
   */
  @Test
  public void testChooseStorageDirWithNoVolume() {
    try {
      chooseNewStorageDir(Collections.emptyList(), UUID.randomUUID().toString());
      Assertions.fail();
    } catch (IOException ex) {
      String expectedErrMsg = "No storage directory found.";
      Assertions.assertEquals(expectedErrMsg, ex.getMessage());
    }
  }

  /**
   * When there is only one directory specified in conf, auto format it.
   */
  @Test
  public void testAutoFormatSingleDirectory() throws Exception {
    final File testDir = new File(rootTestDir.get(), UUID.randomUUID().toString());
    FileUtils.createDirectories(testDir);

    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(testDir));

    final RaftStorageImpl storage = StorageImplUtils.initRaftStorage(
        "group-1", RaftStorage.StartupOption.RECOVER, properties);
    Assertions.assertNotNull(storage);
    storage.close();
  }

  /**
   * When there are multiple directories specified in conf, do not auto format.
   */
  @Test
  public void testAutoFormatMultiDirectories() throws Exception {
    final File testDir = new File(rootTestDir.get(), UUID.randomUUID().toString());
    final List<File> directories = new ArrayList<>();
    IntStream.range(0, 3).mapToObj((i) -> new File(testDir,
        Integer.toString(i))).forEach((dir) -> {
      try {
        FileUtils.createDirectories(dir);
        directories.add(dir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, directories);

    final Throwable ioe = testFailureCase("Do not auto format multi directories",
        () -> StorageImplUtils.initRaftStorage(
            "group-1", RaftStorage.StartupOption.RECOVER, properties),
        IOException.class);
    Assertions.assertTrue(ioe.getMessage().contains("Failed to RECOVER: Storage directory not found"));
  }
}