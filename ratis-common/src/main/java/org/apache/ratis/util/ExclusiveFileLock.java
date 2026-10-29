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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * An exclusive lock on a directory using a lock file inside the directory.
 * <p>
 * Locking is not supported by all file systems.
 * E.g., NFS does not consistently support exclusive locks.
 * If locking is supported, it guarantees exclusive access to the directory.
 * Otherwise, no guarantee is given.
 */
public final class ExclusiveFileLock implements AutoCloseable {
  static final Logger LOG = LoggerFactory.getLogger(ExclusiveFileLock.class);

  public static final String DEFAULT_LOCK_FILE_NAME = "in_use.lock";
  static final String JVM_NAME = ManagementFactory.getRuntimeMXBean().getName();

  private final File lockFile;
  private FileLock lock;

  public ExclusiveFileLock(File dir) {
    this(dir, DEFAULT_LOCK_FILE_NAME);
  }

  public ExclusiveFileLock(File dir, String lockFileName) {
    Objects.requireNonNull(dir, "dir == null");
    this.lockFile = new File(dir, Objects.requireNonNull(lockFileName, "lockFileName == null"));
  }

  public File getLockFile() {
    return lockFile;
  }

  public synchronized boolean isLocked() {
    return lock != null;
  }

  /**
   * Lock the directory to provide exclusive access.
   * It retries {@link #tryLock()} a few times before giving up.
   *
   * @throws IOException if locking fails,
   *         including the case that the directory is already locked.
   */
  public synchronized void lock() throws IOException {
    final FileLock newLock = FileUtils.attempt(this::tryLock, () -> "tryLock " + lockFile);
    if (newLock == null) {
      final String msg = "Cannot lock " + this.lockFile + ". The directory is already locked";
      LOG.info(msg);
      throw new IOException(msg);
    }
    // Don't overwrite lock until success - this way if we accidentally
    // call lock twice, the internal state won't be cleared by the second
    // (failed) lock attempt
    lock = newLock;
  }

  /**
   * Attempts to acquire an exclusive lock on the directory.
   *
   * @return A lock object representing the newly-acquired lock; it is never null.
   * @throws IOException if locking fails.
   *         When the directory is already locked, either by this or another process,
   *         the cause of the exception is an {@link OverlappingFileLockException}.
   */
  @SuppressWarnings({"squid:S2095"}) // Suppress closeable  warning
  FileLock tryLock() throws IOException {
    boolean deletionHookAdded = false;
    if (!lockFile.exists()) {
      lockFile.deleteOnExit();
      deletionHookAdded = true;
    }
    final RandomAccessFile file = new RandomAccessFile(lockFile, "rws");
    final FileLock res;
    try {
      res = file.getChannel().tryLock();
      if (null == res) {
        LOG.error("Unable to acquire file lock on path {}", lockFile);
        throw new OverlappingFileLockException();
      }
      file.write(JVM_NAME.getBytes(StandardCharsets.UTF_8));
      LOG.info("Lock on {} acquired by nodename {}", lockFile, JVM_NAME);
    } catch (OverlappingFileLockException oe) {
      // Cannot read from the locked file on Windows.
      LOG.error("It appears that another process has already locked the directory: " + this.lockFile, oe);
      file.close();
      throw new IOException("Failed to lock " + this.lockFile + ". The directory is already locked", oe);
    } catch(IOException e) {
      LOG.error("Failed to acquire lock on " + lockFile
          + ". If this directory is mounted via NFS, "
          + "ensure that the appropriate nfs lock services are running.", e);
      file.close();
      throw e;
    }
    if (!deletionHookAdded) {
      // If the file existed prior to our startup, we didn't
      // call deleteOnExit above. But since we successfully locked
      // the dir, we can take care of cleaning it up.
      lockFile.deleteOnExit();
    }
    return res;
  }

  /** Unlock the directory.  It is a no-op if the directory is not locked. */
  public synchronized void unlock() throws IOException {
    if (lock == null) {
      return;
    }
    lock.release();
    lock.channel().close();
    lock = null;
  }

  @Override
  public void close() throws IOException {
    unlock();
  }

  @Override
  public String toString() {
    return "DirectoryLock:" + lockFile;
  }
}
