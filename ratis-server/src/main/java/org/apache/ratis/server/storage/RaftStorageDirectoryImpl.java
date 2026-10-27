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
import org.apache.ratis.util.DirectoryLock;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static java.nio.file.Files.newDirectoryStream;

class RaftStorageDirectoryImpl implements RaftStorageDirectory {

  private static final String META_FILE_NAME = "raft-meta";
  private static final String CONF_EXTENSION = ".conf";
  private static final String BOOTSTRAP_CONF_EXTENSION = ".bootstrap";

  enum StorageState {
    UNINITIALIZED,
    NON_EXISTENT,
    NOT_FORMATTED,
    NO_SPACE,
    NORMAL
  }

  private final File root; // root directory
  private final DirectoryLock lock; // storage lock
  private final SizeInBytes freeSpaceMin;

  /**
   * Constructor
   * @param dir directory corresponding to the storage
   */
  RaftStorageDirectoryImpl(File dir, SizeInBytes freeSpaceMin) {
    this.root = dir;
    this.lock = new DirectoryLock(dir);
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

  File getBootstrapConfFile() {
    return new File(getCurrentDir(), META_FILE_NAME + BOOTSTRAP_CONF_EXTENSION);
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
    final long freeSpace = root.getFreeSpace();
    if (freeSpace < freeSpaceMin.getSize()) {
      LOG.warn("{} in directory {}: free space = {} < required = {}",
          StorageState.NO_SPACE, rootPath, freeSpace, freeSpaceMin);
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

  /**
   * Lock storage to provide exclusive access.
   *
   * @throws IOException if locking fails
   * @see DirectoryLock#lock()
   */
  void lock() throws IOException {
    lock.lock();
  }

  /**
   * Unlock storage.
   */
  void unlock() throws IOException {
    lock.unlock();
  }

  @Override
  public String toString() {
    return "Storage Directory " + this.root;
  }
}
