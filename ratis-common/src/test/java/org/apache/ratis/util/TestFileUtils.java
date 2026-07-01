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
package org.apache.ratis.util;

import org.apache.ratis.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

/** Test methods of {@link FileUtils}. */
public class TestFileUtils extends BaseTest {
  @Test
  public void testIsAncestor() throws IOException {
    runTestIsAncestor(true, "/a", "/a/b");
    runTestIsAncestor(true, "/a", "/a/");
    runTestIsAncestor(true, "/a", "/a");
    runTestIsAncestor(true, "a", "a/b");
    runTestIsAncestor(true, "a", "a/");
    runTestIsAncestor(true, "a", "a");

    runTestIsAncestor(false, "/a", "/c");
    runTestIsAncestor(false, "/a", "/abc");
    runTestIsAncestor(false, "/a", "/a/../c");
    runTestIsAncestor(false, "a", "a/../c");
    runTestIsAncestor(false, "a", "/c");
  }

  static void runTestIsAncestor(boolean expected, String ancestor, String path) throws IOException {
    final boolean computed = isAncestor(ancestor, path);
    System.out.printf("isAncestor(%2s, %-9s)? %s, expected? %s%n",
        ancestor, path, computed, expected);
    Assertions.assertSame(expected, computed);
  }

  static boolean isAncestor(String ancestor, String path) throws IOException {
    return FileUtils.isAncestor(new File(ancestor), new File(path));
  }

  @Test
  public void testRenameToCorrupt() throws IOException {
    final File dir = getClassTestDir();
    Assertions.assertTrue(dir.mkdirs());
    try {
      runTestRenameToCorrupt(dir);
    } finally {
      FileUtils.deleteFully(dir);
    }
    Assertions.assertFalse(dir.exists());
  }

  static void runTestRenameToCorrupt(File dir) throws IOException {
    final File srcFile = new File(dir, "snapshot.1_20");
    Assertions.assertFalse(srcFile.exists());
    Assertions.assertTrue(srcFile.createNewFile());
    Assertions.assertTrue(srcFile.exists());

    final File renamed = FileUtils.move(srcFile, ".corrupt");
    Assertions.assertNotNull(renamed);
    Assertions.assertTrue(renamed.exists());
    Assertions.assertFalse(srcFile.exists());

    FileUtils.deleteFully(renamed);
    Assertions.assertFalse(renamed.exists());
  }

  @Test
  public void testRatisFileDestroyerHook() throws IOException {
    final File dir = getTestDir();
    Assertions.assertTrue(dir.mkdirs());

    final AtomicInteger deletes = new AtomicInteger();
    final AtomicInteger truncates = new AtomicInteger();
    final RatisFileDestroyer previous = FileUtils.setFileDestroyer(new RatisFileDestroyer() {
      @Override
      public void delete(Path path) throws IOException {
        deletes.incrementAndGet();
        RatisFileDestroyer.super.delete(path);
      }

      @Override
      public void truncate(FileChannel channel, Path path, long targetLength) throws IOException {
        truncates.incrementAndGet();
        RatisFileDestroyer.super.truncate(channel, path, targetLength);
      }
    });
    try {
      final File file = new File(dir, "file");
      Files.write(file.toPath(), new byte[] {1, 2, 3, 4});
      FileUtils.truncateFile(file, 2);
      Assertions.assertEquals(2, file.length());
      try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
        FileUtils.truncateFile(channel, file, 1);
      }
      Assertions.assertEquals(1, file.length());

      FileUtils.deleteIfExists(file);
      Assertions.assertFalse(file.exists());
      FileUtils.deleteIfExists(file);

      final Path nested = new File(dir, "nested").toPath();
      Files.createDirectories(nested);
      Files.write(nested.resolve("child"), new byte[] {5});
      FileUtils.deleteFully(nested);
      Assertions.assertFalse(Files.exists(nested));

      Assertions.assertEquals(4, deletes.get());
      Assertions.assertEquals(2, truncates.get());
    } finally {
      FileUtils.setFileDestroyer(previous);
      FileUtils.deleteFully(dir);
    }
  }
}
