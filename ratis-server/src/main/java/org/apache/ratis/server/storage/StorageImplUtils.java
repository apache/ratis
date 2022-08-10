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
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.ratis.server.RaftServer.Division.LOG;
import static org.apache.ratis.server.storage.RaftStorage.StartupOption.RECOVER;

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

  static File chooseExistingStorageDir(List<File> volumes, String targetSubDir, Map<File, Integer> numDirsPerVolume)
      throws IOException {
    final List<File> matchedSubDirs = new ArrayList<>();
    volumes.stream().flatMap(volume -> {
          final File[] dirs = Optional.ofNullable(volume.listFiles()).orElse(EMPTY_FILE_ARRAY);
          Optional.ofNullable(numDirsPerVolume).ifPresent(map -> map.put(volume, dirs.length));
          return Arrays.stream(dirs);
        }).filter(dir -> targetSubDir.equals(dir.getName()))
        .forEach(matchedSubDirs::add);

    final int size = matchedSubDirs.size();
    if (size > 1) {
      throw new IOException("More than one directories found for " + targetSubDir + ": " + matchedSubDirs);
    }
    return size == 1 ? matchedSubDirs.get(0) : null;
  }

  static File chooseStorageDir(File existing, String targetSubDir, Map<File, Integer> numDirsPerVolume)
      throws IOException {
    if (existing != null) {
      return existing;
    }
    // return a volume with the min dirs
    return numDirsPerVolume.entrySet().stream()
        .min(Map.Entry.comparingByValue())
        .map(e -> new File(e.getKey(), targetSubDir))
        .orElseThrow(() -> new IOException("No storage directory found."));
  }

  /**
   * Choose a {@link RaftStorage} for the given storage directory name from the given configuration properties
   * and then try to call {@link RaftStorage#initialize()}.
   *
   * Failure handling:
   * - When there are more than one existing directories, throw an exception.
   * - When there is an existing directory, if it fails, throw an exception but not try to initialize a new directory.
   * - When there is no existing directory, try to initialize a new directory from the list specified
   *   in the configuration properties until a directory succeeded or all directories failed.
   *
   * @param storageDirName the storage directory name
   * @param properties the configuration properties
   * @return the chosen storage, which is initialized successfully.
   */
  public static RaftStorageImpl chooseRaftStorage(String storageDirName, RaftProperties properties) throws IOException {
    final SizeInBytes freeSpaceMin = RaftServerConfigKeys.storageFreeSpaceMin(properties);
    final Log.CorruptionPolicy logCorruptionPolicy = RaftServerConfigKeys.Log.corruptionPolicy(properties);
    final List<File> dirsInConf = RaftServerConfigKeys.storageDir(properties);

    final Map<File, Integer> numDirsPerVolume = new HashMap<>();
    final File existing = chooseExistingStorageDir(dirsInConf, storageDirName, numDirsPerVolume);

    for(List<File> volumes = new ArrayList<>(dirsInConf); !volumes.isEmpty(); ) {
      final File dir = chooseStorageDir(existing, storageDirName, numDirsPerVolume);
      try {
        final RaftStorageImpl storage = newRaftStorage(dir, freeSpaceMin, RECOVER, logCorruptionPolicy);
        storage.initialize();
        return storage;
      } catch (Throwable e) {
        if (existing != null) {
          if (e instanceof IOException) {
            throw e;
          }
          throw new IOException("Failed to initialize the existing directory " + existing.getAbsolutePath(), e);
        }

        LOG.warn("Failed to initialize a new directory " + dir.getAbsolutePath(), e);
        final String parent = dir.getParentFile().getAbsolutePath();
        volumes.removeIf(v -> v.getAbsolutePath().equals(parent));
        if (!volumes.isEmpty()) {
          LOG.warn("Retry other directories " + volumes, e);
        }
      }
    }

    throw new IOException("No healthy directories found among " + dirsInConf);
  }
}
