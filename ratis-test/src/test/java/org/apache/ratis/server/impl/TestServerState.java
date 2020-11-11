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
package org.apache.ratis.server.impl;

import org.apache.ratis.BaseTest;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Test cases to verify ServerState.
 */
public class TestServerState {

  private static final Supplier<File> rootTestDir = JavaUtils.memoize(
      () -> new File(BaseTest.getRootTestDir(),
          JavaUtils.getClassSimpleName(TestServerState.class) +
              Integer.toHexString(ThreadLocalRandom.current().nextInt())));

  @AfterClass
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
    File storageDirOne = ServerState.chooseStorageDir(directories, subDirOne);
    File storageDirTwo = ServerState.chooseStorageDir(directories, subDirTwo);
    File expectedOne = new File(testDir, subDirOne);
    File expectedTwo = new File(testDir, subDirTwo);
    Assert.assertEquals(expectedOne.getCanonicalPath(),
        storageDirOne.getCanonicalPath());
    Assert.assertEquals(expectedTwo.getCanonicalPath(),
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
    File storageDirectory = ServerState.chooseStorageDir(directories, subDir);
    File expected = new File(directories.get(6), subDir);
    Assert.assertEquals(expected.getCanonicalPath(),
        storageDirectory.getCanonicalPath());
  }

  /**
   * Tests choosing of storage directory when only no volume is configured.
   *
   * @throws IOException in case of exception.
   */
  @Test
  public void testChooseStorageDirWithNoVolume() {
    try {
      ServerState.chooseStorageDir(
          Collections.emptyList(), UUID.randomUUID().toString());
      Assert.fail();
    } catch (IOException ex) {
      String expectedErrMsg = "No storage directory found.";
      Assert.assertEquals(expectedErrMsg, ex.getMessage());
    }
  }

}