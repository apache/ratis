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

import org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.ReflectionUtils;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** The storage of a raft server. */
public interface RaftStorage extends Closeable {
  Logger LOG = LoggerFactory.getLogger(RaftStorage.class);

  /** @return the storage directory. */
  RaftStorageDirectory getStorageDir();

  /** @return the metadata file. */
  RaftStorageMetadataFile getMetadataFile();

  /** @return the corruption policy for raft log. */
  CorruptionPolicy getLogCorruptionPolicy();

   static Builder newBuilder() {
    return new Builder();
  }

  enum StartupOption {
    /** Format the storage. */
    FORMAT,
    RECOVER
  }

  class Builder {

    private static final Method NEW_RAFT_STORAGE_METHOD = initNewRaftStorageMethod();

    private static Method initNewRaftStorageMethod() {
      final String className = RaftStorage.class.getPackage().getName() + ".StorageImplUtils";
      //final String className = "org.apache.ratis.server.storage.RaftStorageImpl";
      final Class<?>[] argClasses = { File.class, CorruptionPolicy.class, StartupOption.class, long.class };
      try {
        final Class<?> clazz = ReflectionUtils.getClassByName(className);
        return clazz.getMethod("newRaftStorage", argClasses);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to initNewRaftStorageMethod", e);
      }
    }

    private static RaftStorage newRaftStorage(File dir, CorruptionPolicy logCorruptionPolicy,
        StartupOption option, SizeInBytes storageFreeSpaceMin) throws IOException {
      try {
        return (RaftStorage) NEW_RAFT_STORAGE_METHOD.invoke(null,
            dir, logCorruptionPolicy, option, storageFreeSpaceMin.getSize());
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Failed to build " + dir, e);
      } catch (InvocationTargetException e) {
        Throwable t = e.getTargetException();
        if (t.getCause() instanceof IOException) {
          throw IOUtils.asIOException(t.getCause());
        }
        throw IOUtils.asIOException(e.getCause());
      }
    }


    private File directory;
    private CorruptionPolicy logCorruptionPolicy;
    private StartupOption option;
    private SizeInBytes storageFreeSpaceMin;

    public Builder setDirectory(File directory) {
      this.directory = directory;
      return this;
    }

    public Builder setLogCorruptionPolicy(CorruptionPolicy logCorruptionPolicy) {
      this.logCorruptionPolicy = logCorruptionPolicy;
      return this;
    }

    public Builder setOption(StartupOption option) {
      this.option = option;
      return this;
    }

    public Builder setStorageFreeSpaceMin(SizeInBytes storageFreeSpaceMin) {
      this.storageFreeSpaceMin = storageFreeSpaceMin;
      return this;
    }

    public RaftStorage build() throws IOException {
      return newRaftStorage(directory, logCorruptionPolicy, option, storageFreeSpaceMin);
    }
  }
}
