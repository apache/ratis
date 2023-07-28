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

import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static java.nio.file.Files.newDirectoryStream;

class RaftStorageDirectoryImpl implements RaftStorageDirectory {

  private static final String IN_USE_LOCK_NAME = "in_use.lock";
  private static final String META_FILE_NAME = "raft-meta";
  private static final String CONF_EXTENSION = ".conf";
  private static final String JVM_NAME = ManagementFactory.getRuntimeMXBean().getName();

  enum StorageState {
    UNINITIALIZED,
    NON_EXISTENT,
    NOT_FORMATTED,
    NO_SPACE,
    NORMAL
  }

  private final File root; // root directory
  private FileLock lock;   // storage lock
  private final SizeInBytes freeSpaceMin;

  /**
   * Constructor
   * @param dir directory corresponding to the storage
   */
  RaftStorageDirectoryImpl(File dir, SizeInBytes freeSpaceMin) {
    this.root = dir;
    this.lock = null;
    this.freeSpaceMin = freeSpaceMin;
  }

  @Override
  public File getRoot() {
    return root;
  }

  /**
   * Clear and re-create storage directory.
   * <p>
   * Removes contents of the current directory and creates an empty directory.
   *
   * This does not fully format storage directory.
   * It cannot write the version file since it should be written last after
   * all other storage type dependent files are written.
   * Derived storage is responsible for setting specific storage values and
   * writing the version file to disk.
   */
  void clearDirectory() throws IOException {
    clearDirectory(getCurrentDir());
    clearDirectory(getStateMachineDir());
  }

  private static void clearDirectory(File dir) throws IOException {
    if (dir.exists()) {
      LOG.info("{} already exists.  Deleting it ...", dir);
      FileUtils.deleteFully(dir);
    }
    FileUtils.createDirectories(dir);
  }


  File getMetaFile() {
    return new File(getCurrentDir(), META_FILE_NAME);
  }

  File getMetaTmpFile() {
    return AtomicFileOutputStream.getTemporaryFile(getMetaFile());
  }

  File getMetaConfFile() {
    return new File(getCurrentDir(), META_FILE_NAME + CONF_EXTENSION);
  }

  /**
   * Check to see if current/ directory is empty.
   */
  boolean isCurrentEmpty() throws IOException {
    File currentDir = getCurrentDir();
    if(!currentDir.exists()) {
      // if current/ does not exist, it's safe to format it.
      return true;
    }
    try(DirectoryStream<Path> dirStream =
            newDirectoryStream(currentDir.toPath())) {
      if (dirStream.iterator().hasNext()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check consistency of the storage directory.
   *
   * @return state {@link StorageState} of the storage directory
   */
  StorageState analyzeStorage(boolean toLock) throws IOException {
    Objects.requireNonNull(root, "root directory is null");

    String rootPath = root.getCanonicalPath();
    try { // check that storage exists
      if (!root.exists()) {
        LOG.info("The storage directory {} does not exist. Creating ...", rootPath);
        FileUtils.createDirectories(root);
      }
      // or is inaccessible
      if (!root.isDirectory()) {
        LOG.warn("{} is not a directory", rootPath);
        return StorageState.NON_EXISTENT;
      }
      if (!Files.isWritable(root.toPath())) {
        LOG.warn("The storage directory {} is not writable.", rootPath);
        return StorageState.NON_EXISTENT;
      }
    } catch(SecurityException ex) {
      LOG.warn("Cannot access storage directory " + rootPath, ex);
      return StorageState.NON_EXISTENT;
    }

    if (toLock) {
      this.lock(); // lock storage if it exists
    }

    // check enough space
    if (!hasEnoughSpace()) {
      LOG.warn("There are not enough space left for directory {}"
          + " free space min required: {} free space actual: {}",
          rootPath, freeSpaceMin, root.getFreeSpace());
      return StorageState.NO_SPACE;
    }

    // check whether current directory is valid
    if (isHealthy()) {
      return StorageState.NORMAL;
    } else {
      return StorageState.NOT_FORMATTED;
    }
  }

  @Override
  public boolean isHealthy() {
    return getMetaFile().exists();
  }

  private boolean hasEnoughSpace() {
    return root.getFreeSpace() >= freeSpaceMin.getSize();
  }

  /**
   * Lock storage to provide exclusive access.
   *
   * <p> Locking is not supported by all file systems.
   * E.g., NFS does not consistently support exclusive locks.
   *
   * <p> If locking is supported we guarantee exclusive access to the
   * storage directory. Otherwise, no guarantee is given.
   *
   * @throws IOException if locking fails
   */
  void lock() throws IOException {
    final File lockF = new File(root, IN_USE_LOCK_NAME);
    final FileLock newLock = FileUtils.attempt(() -> tryLock(lockF), () -> "tryLock " + lockF);
    if (newLock == null) {
      String msg = "Cannot lock storage " + this.root
          + ". The directory is already locked";
      LOG.info(msg);
      throw new IOException(msg);
    }
    // Don't overwrite lock until success - this way if we accidentally
    // call lock twice, the internal state won't be cleared by the second
    // (failed) lock attempt
    lock = newLock;
  }

  /**
   * Attempts to acquire an exclusive lock on the storage.
   *
   * @return A lock object representing the newly-acquired lock or
   * <code>null</code> if storage is already locked.
   * @throws IOException if locking fails.
   */
  private FileLock tryLock(File lockF) throws IOException {
    boolean deletionHookAdded = false;
    if (!lockF.exists()) {
      lockF.deleteOnExit();
      deletionHookAdded = true;
    }
    RandomAccessFile file = new RandomAccessFile(lockF, "rws");
    FileLock res;
    try {
      res = file.getChannel().tryLock();
      if (null == res) {
        LOG.error("Unable to acquire file lock on path {}", lockF);
        throw new OverlappingFileLockException();
      }
      file.write(JVM_NAME.getBytes(StandardCharsets.UTF_8));
      LOG.info("Lock on {} acquired by nodename {}", lockF, JVM_NAME);
    } catch (OverlappingFileLockException oe) {
      // Cannot read from the locked file on Windows.
      LOG.error("It appears that another process "
          + "has already locked the storage directory: " + root, oe);
      file.close();
      throw new IOException("Failed to lock storage " + this.root + ". The directory is already locked", oe);
    } catch(IOException e) {
      LOG.error("Failed to acquire lock on " + lockF
          + ". If this storage directory is mounted via NFS, "
          + "ensure that the appropriate nfs lock services are running.", e);
      file.close();
      throw e;
    }
    if (!deletionHookAdded) {
      // If the file existed prior to our startup, we didn't
      // call deleteOnExit above. But since we successfully locked
      // the dir, we can take care of cleaning it up.
      lockF.deleteOnExit();
    }
    return res;
  }

  /**
   * Unlock storage.
   */
  void unlock() throws IOException {
    if (this.lock == null) {
      return;
    }
    this.lock.release();
    lock.channel().close();
    lock = null;
  }

  @Override
  public String toString() {
    return "Storage Directory " + this.root;
  }
}
