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
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

public class TestDirectoryLock extends BaseTest {
  static final String LOCKED = "LOCKED";

  private File newTestDir() throws IOException {
    final File dir = getTestDir();
    FileUtils.createDirectories(dir);
    return dir;
  }

  static String readLockFile(DirectoryLock lock) throws IOException {
    return new String(Files.readAllBytes(lock.getLockFile().toPath()), StandardCharsets.UTF_8);
  }

  @Test
  public void testLockUnlock() throws Exception {
    final File dir = newTestDir();

    final DirectoryLock lock = new DirectoryLock(dir);
    Assertions.assertFalse(lock.isLocked());
    Assertions.assertEquals(new File(dir, DirectoryLock.DEFAULT_LOCK_FILE_NAME), lock.getLockFile());

    lock.lock();
    Assertions.assertTrue(lock.isLocked());
    Assertions.assertTrue(lock.getLockFile().exists());

    // the same directory cannot be locked again
    final DirectoryLock another = new DirectoryLock(dir);
    final IOException e = Assertions.assertThrows(IOException.class, another::lock);
    Assertions.assertTrue(e.getMessage().contains("directory is already locked"), e::getMessage);
    Assertions.assertFalse(another.isLocked());
    // a failed lock attempt must not release the existing lock
    Assertions.assertThrows(IOException.class, lock::lock);
    Assertions.assertTrue(lock.isLocked());

    lock.unlock();
    Assertions.assertFalse(lock.isLocked());
    // unlock is idempotent
    lock.unlock();

    try (DirectoryLock ignored = another) {
      another.lock();
      Assertions.assertTrue(another.isLocked());
    }
    Assertions.assertFalse(another.isLocked());
  }

  @Test
  public void testCustomLockFileName() throws Exception {
    final File dir = newTestDir();

    try (DirectoryLock a = new DirectoryLock(dir, "a.lock");
         DirectoryLock b = new DirectoryLock(dir, "b.lock")) {
      a.lock();
      b.lock();
      Assertions.assertTrue(a.isLocked());
      Assertions.assertTrue(b.isLocked());
    }
  }

  @Test
  public void testTryLockNewFile() throws Exception {
    final DirectoryLock lock = new DirectoryLock(newTestDir());
    Assertions.assertFalse(lock.getLockFile().exists());

    final FileLock fileLock = lock.tryLock();
    try {
      Assertions.assertNotNull(fileLock);
      Assertions.assertTrue(fileLock.isValid());
      Assertions.assertFalse(fileLock.isShared());
      Assertions.assertEquals(DirectoryLock.JVM_NAME, readLockFile(lock));
    } finally {
      fileLock.channel().close();
    }
    Assertions.assertFalse(fileLock.isValid());
  }

  @Test
  public void testTryLockExistingFile() throws Exception {
    final DirectoryLock lock = new DirectoryLock(newTestDir());
    Assertions.assertTrue(lock.getLockFile().createNewFile());

    final FileLock fileLock = lock.tryLock();
    try {
      Assertions.assertTrue(fileLock.isValid());
      Assertions.assertEquals(DirectoryLock.JVM_NAME, readLockFile(lock));
    } finally {
      fileLock.channel().close();
    }
  }

  @Test
  public void testTryLockOverlappingInSameJvm() throws Exception {
    final File dir = newTestDir();
    final DirectoryLock lock = new DirectoryLock(dir);
    final FileLock fileLock = lock.tryLock();
    try {
      final IOException e = Assertions.assertThrows(IOException.class, new DirectoryLock(dir)::tryLock);
      Assertions.assertInstanceOf(OverlappingFileLockException.class, e.getCause());
      Assertions.assertTrue(e.getMessage().contains("directory is already locked"), e::getMessage);
      // the existing lock is not affected
      Assertions.assertTrue(fileLock.isValid());
    } finally {
      fileLock.channel().close();
    }

    // can lock again after the previous lock is released
    final FileLock again = lock.tryLock();
    Assertions.assertTrue(again.isValid());
    again.channel().close();
  }

  @Test
  public void testTryLockNonExistentDirectory() {
    final DirectoryLock lock = new DirectoryLock(new File(getTestDir(), "nonExistent"));
    Assertions.assertThrows(FileNotFoundException.class, lock::tryLock);
    Assertions.assertFalse(lock.isLocked());
  }

  /** Lock the directory in another JVM so that {@link java.nio.channels.FileChannel#tryLock()} returns null. */
  @Test
  @Timeout(value = 60)
  public void testTryLockHeldByAnotherProcess() throws Exception {
    final File dir = newTestDir();
    final String java = new File(new File(System.getProperty("java.home"), "bin"), "java").getPath();
    final Process child = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
        TestDirectoryLock.class.getName(), dir.getAbsolutePath())
        .redirectErrorStream(true)
        .start();
    try {
      try (BufferedReader in = new BufferedReader(
          new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
        for (String line; !LOCKED.equals(line = in.readLine()); ) {
          Assertions.assertNotNull(line, "The child process exited before locking " + dir);
        }

        final DirectoryLock lock = new DirectoryLock(dir);
        final IOException e = Assertions.assertThrows(IOException.class, lock::tryLock);
        Assertions.assertInstanceOf(OverlappingFileLockException.class, e.getCause());
        Assertions.assertFalse(lock.isLocked());

        // tell the child process to unlock and exit
        child.getOutputStream().close();
        Assertions.assertTrue(child.waitFor(30, TimeUnit.SECONDS));
        Assertions.assertEquals(0, child.exitValue());

        lock.lock();
        Assertions.assertTrue(lock.isLocked());
        lock.unlock();
      }
    } finally {
      child.destroyForcibly();
    }
  }

  /** Lock the given directory, print {@link #LOCKED} and then hold the lock until stdin is closed. */
  public static void main(String[] args) throws IOException {
    try (DirectoryLock lock = new DirectoryLock(new File(args[0]))) {
      lock.lock();
      System.out.println(LOCKED);
      System.out.flush();
      while (System.in.read() != -1) {
        // wait for stdin to be closed
      }
    }
  }
}
