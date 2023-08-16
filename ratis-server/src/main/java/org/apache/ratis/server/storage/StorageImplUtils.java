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
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.ratis.server.RaftServer.Division.LOG;

public final class StorageImplUtils {
  private static final File[] EMPTY_FILE_ARRAY = {};

  private StorageImplUtils() {
    //Never constructed
  }

  public static SnapshotManager newSnapshotManager(RaftPeerId id,
                                                   Supplier<RaftStorageDirectory> dir, StateMachineStorage smStorage) {
    return new SnapshotManager(id, dir, smStorage);
  }

  /** Create a {@link RaftStorageImpl}. */
  @SuppressWarnings("java:S2095") // return Closable
  public static RaftStorageImpl newRaftStorage(File dir, SizeInBytes freeSpaceMin,
      RaftStorage.StartupOption option, Log.CorruptionPolicy logCorruptionPolicy) {
    return new RaftStorageImpl(dir, freeSpaceMin, option, logCorruptionPolicy);
  }

  /** @return a list of existing subdirectories matching the given storage directory name from the given volumes. */
  static List<File> getExistingStorageSubs(List<File> volumes, String targetSubDir, Map<File, Integer> dirsPerVol) {
    return volumes.stream().flatMap(volume -> {
          final File[] dirs = Optional.ofNullable(volume.listFiles()).orElse(EMPTY_FILE_ARRAY);
          Optional.ofNullable(dirsPerVol).ifPresent(map -> map.put(volume, dirs.length));
          return Arrays.stream(dirs);
        }).filter(dir -> targetSubDir.equals(dir.getName()))
        .collect(Collectors.toList());
  }

  /** @return a volume with the min dirs. */
  static File chooseMin(Map<File, Integer> dirsPerVol) throws IOException {
    return dirsPerVol.entrySet().stream()
        .min(Map.Entry.comparingByValue())
        .map(Map.Entry::getKey)
        .orElseThrow(() -> new IOException("No storage directory found."));
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
   * - When there is no existing directory, if only one directory is specified in the configuration, format it;
   *   otherwise, there are >1 directories specified, throw an exception.
   *
   * @param storageDirName the storage directory name
   * @param option the startup option
   * @param properties the configuration properties
   * @return the chosen storage, which is initialized successfully.
   */
  public static RaftStorageImpl initRaftStorage(String storageDirName, StartupOption option,
      RaftProperties properties) throws IOException {
    return new Op(storageDirName, option, properties).run();
  }

  private static class Op {
    private final String storageDirName;
    private final StartupOption option;

    private final SizeInBytes freeSpaceMin;
    private final Log.CorruptionPolicy logCorruptionPolicy;
    private final List<File> dirsInConf;

    private final List<File> existingSubs;
    private final Map<File, Integer> dirsPerVol = new HashMap<>();

    Op(String storageDirName, StartupOption option, RaftProperties properties) {
      this.storageDirName = storageDirName;
      this.option = option;

      this.freeSpaceMin = RaftServerConfigKeys.storageFreeSpaceMin(properties);
      this.logCorruptionPolicy = RaftServerConfigKeys.Log.corruptionPolicy(properties);
      this.dirsInConf = RaftServerConfigKeys.storageDir(properties);

      this.existingSubs = getExistingStorageSubs(dirsInConf, this.storageDirName, dirsPerVol);
    }

    RaftStorageImpl run() throws IOException {
      if (option == StartupOption.FORMAT) {
        return format();
      } else if (option == StartupOption.RECOVER) {
        final RaftStorageImpl recovered = recover();
        return recovered != null? recovered: format();
      } else {
        throw new IllegalArgumentException("Illegal option: " + option);
      }
    }

    @SuppressWarnings("java:S1181") // catch Throwable
    private RaftStorageImpl format() throws IOException {
      if (!existingSubs.isEmpty()) {
        throw new IOException("Failed to " + option + ": One or more existing directories found " + existingSubs
            + " for " + storageDirName);
      }

      while (!dirsPerVol.isEmpty()) {
        final File vol = chooseMin(dirsPerVol);
        final File dir = new File(vol, storageDirName);
        try {
          final RaftStorageImpl storage = newRaftStorage(dir, freeSpaceMin, StartupOption.FORMAT, logCorruptionPolicy);
          storage.initialize();
          return storage;
        } catch (Throwable e) {
          LOG.warn("Failed to initialize a new directory " + dir.getAbsolutePath(), e);
          dirsPerVol.remove(vol);
        }
      }
      throw new IOException("Failed to FORMAT a new storage dir for " + storageDirName + " from " + dirsInConf);
    }

    @SuppressWarnings("java:S1181") // catch Throwable
    private RaftStorageImpl recover() throws IOException {
      final int size = existingSubs.size();
      if (size > 1) {
        throw new IOException("Failed to " + option + ": More than one existing directories found "
            + existingSubs + " for " + storageDirName);
      } else if (size == 0) {
        if (dirsInConf.size() == 1) {
          // fallback to FORMAT
          return null;
        }
        throw new IOException("Failed to " + option + ": Storage directory not found for "
            + storageDirName + " from " + dirsInConf);
      }

      final File dir = existingSubs.get(0);
      try {
        final RaftStorageImpl storage = newRaftStorage(dir, freeSpaceMin, StartupOption.RECOVER, logCorruptionPolicy);
        storage.initialize();
        return storage;
      } catch (IOException e) {
        throw e;
      } catch (Throwable e) {
        throw new IOException("Failed to initialize the existing directory " + dir.getAbsolutePath(), e);
      }
    }
  }
}
