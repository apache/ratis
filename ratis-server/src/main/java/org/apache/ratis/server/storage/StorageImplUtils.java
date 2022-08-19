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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys.Log;
import org.apache.ratis.server.storage.RaftStorage.StartupOption;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.ratis.server.RaftServer.Division.LOG;

public final class StorageImplUtils {
  private static final File[] EMPTY_FILE_ARRAY = {};

  private StorageImplUtils() {
    //Never constructed
  }

  public static SnapshotManager newSnapshotManager(RaftPeerId id) {
    return new SnapshotManager(id);
  }

  /** Create a {@link RaftStorageImpl}. */
  public static RaftStorageImpl newRaftStorage(File dir, SizeInBytes freeSpaceMin,
      RaftStorage.StartupOption option, Log.CorruptionPolicy logCorruptionPolicy) {
    return new RaftStorageImpl(dir, freeSpaceMin, option, logCorruptionPolicy);
  }

  private static List<File> getExistingStorageSubs(List<File> volumes, String targetSubDir,
      Map<File, Integer> dirsPerVol) {
    return volumes.stream().flatMap(volume -> {
          final File[] dirs = Optional.ofNullable(volume.listFiles()).orElse(EMPTY_FILE_ARRAY);
          Optional.ofNullable(dirsPerVol).ifPresent(map -> map.put(volume, dirs.length));
          return Arrays.stream(dirs);
        }).filter(dir -> targetSubDir.equals(dir.getName()))
        .collect(Collectors.toList());
  }

  /**
   * Get a list of existing subdirectories matching the given storage directory name from the given root directories.
   */
  static List<File> getExistingStorageSubs(String storageDirName, StartupOption option,
      List<File> rootDirs, Map<File, Integer> dirsPerVol) throws IOException {
    Preconditions.assertEmpty(dirsPerVol, "dirsPerVol");
    final List<File> existingSubs = getExistingStorageSubs(rootDirs, storageDirName, dirsPerVol);
    final int size = existingSubs.size();
    if (option == StartupOption.RECOVER) {
      if (size > 1) {
        throw new IOException("Failed to " + option + ": More than one existing directories found " + existingSubs
            + " for " + storageDirName);
      } else if (size == 0) {
        throw new IOException("Failed to " + option + ": Storage directory not found for " + storageDirName
            + " from " + rootDirs);
      }
    } else if (option == StartupOption.FORMAT) {
      if (size > 0) {
        throw new IOException("Failed to " + option + ": One or more existing directories found " + existingSubs
            + " for " + storageDirName);
      }
    } else {
      throw new IllegalArgumentException("Illegal option: " + option);
    }
    return existingSubs;
  }

  /**
   * Choose a {@link RaftStorage} for the given storage directory name from the given configuration properties
   * and then try to call {@link RaftStorage#initialize()}.
   * <p />
   * {@link StartupOption#FORMAT}:
   * - When there are more than one existing directories, throw an exception.
   * - When there is an existing directory, throw an exception.
   * - When there is no existing directory, try to initialize a new directory from the list specified
   *   in the configuration properties until a directory succeeded or all directories failed.
   * <p />
   * {@link StartupOption#RECOVER}:
   * - When there are more than one existing directories, throw an exception.
   * - When there is an existing directory, if it fails to initialize, throw an exception but not try a new directory.
   * - When there is no existing directory, throw an exception.
   *
   * @param storageDirName the storage directory name
   * @param option the startup option
   * @param properties the configuration properties
   * @return the chosen storage, which is initialized successfully.
   */
  public static RaftStorageImpl initRaftStorage(String storageDirName, StartupOption option,
      RaftProperties properties) throws IOException {
    final SizeInBytes freeSpaceMin = RaftServerConfigKeys.storageFreeSpaceMin(properties);
    final Log.CorruptionPolicy logCorruptionPolicy = RaftServerConfigKeys.Log.corruptionPolicy(properties);
    final List<File> dirsInConf = RaftServerConfigKeys.storageDir(properties);

    final Map<File, Integer> dirsPerVol = new HashMap<>();
    final List<File> existingDirs = getExistingStorageSubs(storageDirName, option, dirsInConf, dirsPerVol);
    if (option == StartupOption.RECOVER) {
      return recoverExistingStorageDir(existingDirs.get(0), freeSpaceMin, logCorruptionPolicy);
    } else if (option == StartupOption.FORMAT) {
      return formatNewStorageDir(storageDirName, dirsInConf, dirsPerVol, freeSpaceMin, logCorruptionPolicy);
    } else {
      throw new IllegalArgumentException("Illegal option: " + option);
    }
  }

  private static RaftStorageImpl recoverExistingStorageDir(File dir, SizeInBytes freeSpaceMin,
      Log.CorruptionPolicy logCorruptionPolicy) throws IOException {
    try {
      final RaftStorageImpl storage = newRaftStorage(dir, freeSpaceMin, StartupOption.RECOVER, logCorruptionPolicy);
      storage.initialize();
      return storage;
    } catch (Throwable e) {
      if (e instanceof IOException) {
        throw e;
      }
      throw new IOException("Failed to initialize the existing directory " + dir.getAbsolutePath(), e);
    }
  }

  private static RaftStorageImpl formatNewStorageDir(String storageDirName, List<File> dirsInConf,
      Map<File, Integer> dirsPerVol, SizeInBytes freeSpaceMin, Log.CorruptionPolicy logCorruptionPolicy)
      throws IOException {
    for(List<File> volumes = new ArrayList<>(dirsInConf); !volumes.isEmpty(); ) {
      final File dir = chooseNewStorageDir(storageDirName, dirsPerVol);
      try {
        final RaftStorageImpl storage = newRaftStorage(dir, freeSpaceMin, StartupOption.FORMAT, logCorruptionPolicy);
        storage.initialize();
        return storage;
      } catch (Throwable e) {
        LOG.warn("Failed to initialize a new directory " + dir.getAbsolutePath(), e);
        final String parent = dir.getParentFile().getAbsolutePath();
        volumes.removeIf(v -> v.getAbsolutePath().equals(parent));
        if (!volumes.isEmpty()) {
          LOG.warn("Retry other directories " + volumes, e);
        }
      }
    }
    throw new IOException("Failed to FORMAT a new storage dir for " + storageDirName + " from " + dirsInConf);
  }

  /** @return a volume with the min dirs. */
  static File chooseNewStorageDir(String targetSubDir, Map<File, Integer> dirsPerVol) throws IOException {
    return dirsPerVol.entrySet().stream()
        .min(Map.Entry.comparingByValue())
        .map(e -> new File(e.getKey(), targetSubDir))
        .orElseThrow(() -> new IOException("No storage directory found."));
  }
}
