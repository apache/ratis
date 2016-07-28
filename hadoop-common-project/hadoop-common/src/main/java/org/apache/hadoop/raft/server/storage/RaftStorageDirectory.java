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
package org.apache.hadoop.raft.server.storage;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.raft.util.AtomicFileOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.hadoop.raft.server.RaftConstants.INVALID_LOG_INDEX;

public class RaftStorageDirectory {
  static final Logger LOG = LoggerFactory.getLogger(RaftStorageDirectory.class);

  static final String STORAGE_DIR_CURRENT = "current";
  static final String STORAGE_FILE_LOCK = "in_use.lock";
  static final String META_FILE_NAME = "raft-meta";
  static final String LOG_FILE_INPROGRESS = "inprogress";
  static final String LOG_FILE_PREFIX = "log";
  static final String SNAPSHOT_FILE_PREFIX = "snapshot";
  static final Pattern CLOSED_SEGMENT_REGEX = Pattern.compile("log_(\\d+)-(\\d+)");
  static final Pattern OPEN_SEGMENT_REGEX = Pattern.compile("log_inprogress_(\\d+)(?:\\..*)?");
  static final Pattern SNAPSHOT_REGEX = Pattern.compile(SNAPSHOT_FILE_PREFIX + "_(\\d+)");

  private static final List<Pattern> LOGSEGMENTS_REGEXES =
      ImmutableList.of(CLOSED_SEGMENT_REGEX, OPEN_SEGMENT_REGEX);

  enum StorageState {
    NON_EXISTENT,
    NOT_FORMATTED,
    NORMAL
  }

  public static class PathAndIndex {
    public final Path path;
    public final long startIndex;
    public final long endIndex;

    PathAndIndex(Path path, long startIndex, long endIndex) {
      this.path = path;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }
  }

  private final File root; // root directory
  private FileLock lock;   // storage lock

  /**
   * Constructor
   * @param dir directory corresponding to the storage
   */
  RaftStorageDirectory(File dir) {
    this.root = dir;
    this.lock = null;
  }

  /**
   * Get root directory of this storage
   */
  File getRoot() {
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
    File curDir = this.getCurrentDir();
    if (curDir.exists()) {
      File[] files = FileUtil.listFiles(curDir);
      LOG.info("Will remove files: " + Arrays.toString(files));
      if (!(FileUtil.fullyDelete(curDir)))
        throw new IOException("Cannot remove current directory: " + curDir);
    }
    if (!curDir.mkdirs())
      throw new IOException("Cannot create directory " + curDir);
  }

  /**
   * Directory {@code current} contains latest files defining
   * the file system meta-data.
   *
   * @return the directory path
   */
  File getCurrentDir() {
    return new File(root, STORAGE_DIR_CURRENT);
  }

  File getMetaFile() {
    return new File(getCurrentDir(), META_FILE_NAME);
  }

  File getMetaTmpFile() {
    return new File(getCurrentDir(), META_FILE_NAME
        + AtomicFileOutputStream.TMP_EXTENSION);
  }

  File getOpenLogFile(long startIndex) {
    return new File(getCurrentDir(), getOpenLogFileName(startIndex));
  }

  static String getOpenLogFileName(long startIndex) {
    return LOG_FILE_PREFIX + "_" + LOG_FILE_INPROGRESS + "_" + startIndex;
  }

  File getClosedLogFile(long startIndex, long endIndex) {
    return new File(getCurrentDir(), getClosedLogFileName(startIndex, endIndex));
  }

  static String getClosedLogFileName(long startIndex, long endIndex) {
    return LOG_FILE_PREFIX + "_" + startIndex + "-" + endIndex;
  }

  static String getSnapshotFileName(long endIndex) {
    return SNAPSHOT_FILE_PREFIX + "_" + endIndex;
  }

  public File getSnapshotFile(long endIndex) {
    return new File(getCurrentDir(), getSnapshotFileName(endIndex));
  }

  PathAndIndex getLatestSnapshot() throws IOException {
    PathAndIndex latest = null;
    try (DirectoryStream<Path> stream =
             Files.newDirectoryStream(getCurrentDir().toPath())) {
      for (Path path : stream) {
          Matcher matcher = SNAPSHOT_REGEX.matcher(path.getFileName().toString());
          if (matcher.matches()) {
            final long endIndex = Long.parseLong(matcher.group(1));
            if (latest == null || endIndex > latest.endIndex) {
              latest = new PathAndIndex(path, 0, endIndex);
          }
        }
      }
    }
    return latest;
  }

  /**
   * @return log segment files sorted based on their index.
   */
  List<PathAndIndex> getLogSegmentFiles() throws IOException {
    List<PathAndIndex> list = new ArrayList<>();
    try (DirectoryStream<Path> stream =
             Files.newDirectoryStream(getCurrentDir().toPath())) {
      for (Path path : stream) {
        for (Pattern pattern : LOGSEGMENTS_REGEXES) {
          Matcher matcher = pattern.matcher(path.getFileName().toString());
          if (matcher.matches()) {
            final long startIndex = Long.parseLong(matcher.group(1));
            final long endIndex = matcher.groupCount() == 2 ?
                Long.parseLong(matcher.group(2)) : INVALID_LOG_INDEX;
            list.add(new PathAndIndex(path, startIndex, endIndex));
          }
        }
      }
    }
    Collections.sort(list,
        (o1, o2) -> o1.startIndex == o2.startIndex ?
            0 : (o1.startIndex < o2.startIndex ? -1 : 1));
    return list;
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
    Preconditions.checkState(root != null, "root directory is null");

    String rootPath = root.getCanonicalPath();
    try { // check that storage exists
      if (!root.exists()) {
        LOG.info(rootPath + " does not exist. Creating ...");
        if (!root.mkdirs()) {
          throw new IOException("Cannot create directory " + rootPath);
        }
      }
      // or is inaccessible
      if (!root.isDirectory()) {
        LOG.warn(rootPath + "is not a directory");
        return StorageState.NON_EXISTENT;
      }
      if (!FileUtil.canWrite(root)) {
        LOG.warn("Cannot access storage directory " + rootPath);
        return StorageState.NON_EXISTENT;
      }
    } catch(SecurityException ex) {
      LOG.warn("Cannot access storage directory " + rootPath, ex);
      return StorageState.NON_EXISTENT;
    }

    if (toLock) {
      this.lock(); // lock storage if it exists
    }

    // check whether current directory is valid
    if (hasMetaFile()) {
      return StorageState.NORMAL;
    } else {
      return StorageState.NOT_FORMATTED;
    }
  }

  boolean hasMetaFile() throws IOException {
    return getMetaFile().exists();
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
  public void lock() throws IOException {
    FileLock newLock = tryLock();
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
  private FileLock tryLock() throws IOException {
    boolean deletionHookAdded = false;
    File lockF = new File(root, STORAGE_FILE_LOCK);
    if (!lockF.exists()) {
      lockF.deleteOnExit();
      deletionHookAdded = true;
    }
    RandomAccessFile file = new RandomAccessFile(lockF, "rws");
    String jvmName = ManagementFactory.getRuntimeMXBean().getName();
    FileLock res;
    try {
      res = file.getChannel().tryLock();
      if (null == res) {
        LOG.error("Unable to acquire file lock on path " + lockF.toString());
        throw new OverlappingFileLockException();
      }
      file.write(jvmName.getBytes(Charsets.UTF_8));
      LOG.info("Lock on " + lockF + " acquired by nodename " + jvmName);
    } catch (OverlappingFileLockException oe) {
      // Cannot read from the locked file on Windows.
      LOG.error("It appears that another process "
          + "has already locked the storage directory: " + root, oe);
      file.close();
      return null;
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
  public void unlock() throws IOException {
    if (this.lock == null)
      return;
    this.lock.release();
    lock.channel().close();
    lock = null;
  }

  @Override
  public String toString() {
    return "Storage Directory " + this.root;
  }
}
