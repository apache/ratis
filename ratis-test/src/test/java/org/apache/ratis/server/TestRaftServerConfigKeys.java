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

package org.apache.ratis.server;

import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test cases to verify RaftServerConfigKeys.
 */
public class TestRaftServerConfigKeys {

  private static final Supplier<File> rootTestDir = JavaUtils.memoize(
      () -> new File(BaseTest.getRootTestDir(),
          JavaUtils.getClassSimpleName(TestRaftServerConfigKeys.class) +
              Integer.toHexString(ThreadLocalRandom.current().nextInt())));

  @AfterClass
  public static void tearDown() throws IOException {
    FileUtils.deleteFully(rootTestDir.get());
  }

  /**
   * Sets the value to <code>raft.server.storage.dir</code> via
   * RaftServerConfigKeys and verifies it by reading directly from property.
   */
  @Test
  public void testStorageDirProperty() {
    final File testDir = new File(
        rootTestDir.get(), UUID.randomUUID().toString());
    final List<File> directories = new ArrayList<>();
    final  RaftProperties properties = new RaftProperties();

    IntStream.range(0, 10).mapToObj((i) -> new File(testDir,
        Integer.toString(i))).forEach(directories::add);
    RaftServerConfigKeys.setStorageDir(properties, directories);

    final String expected = directories.stream().map(File::getAbsolutePath)
        .collect(Collectors.joining(","));
    final String actual = properties.get(RaftServerConfigKeys.STORAGE_DIR_KEY);
    Assert.assertEquals(expected, actual);
  }

  /**
   * Sets the value to <code>raft.server.storage.dir</code> via
   * RaftServerConfigKeys and also verifies the same via RaftServerConfigKeys.
   */
  @Test
  public void testStorageDir() {
    final File testDir = new File(
        rootTestDir.get(), UUID.randomUUID().toString());
    final List<File> directories = new ArrayList<>();
    IntStream.range(0, 10).mapToObj((i) -> new File(testDir,
        Integer.toString(i))).forEach(directories::add);
    RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, directories);

    final List<File> storageDirs = RaftServerConfigKeys.storageDir(properties);
    final List<String> expectedDirs = directories.stream()
        .map(File::getAbsolutePath).collect(Collectors.toList());
    final List<String> actualDirs = storageDirs.stream()
        .map(File::getAbsolutePath).collect(Collectors.toList());
    actualDirs.removeAll(expectedDirs);
    Assert.assertEquals(directories.size(), storageDirs.size());
    Assert.assertEquals(0, actualDirs.size());
  }
}