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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.storage.RaftStorageDirectoryImpl.StorageState;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

/** The storage of a {@link org.apache.ratis.server.RaftServer}. */
public class RaftStorageImpl implements RaftStorage {
  private final RaftStorageDirectoryImpl storageDir;
  private final StartupOption startupOption;
  private final CorruptionPolicy logCorruptionPolicy;
  private volatile StorageState state = StorageState.UNINITIALIZED;
  private final MetaFile metaFile = new MetaFile();

  RaftStorageImpl(File dir, SizeInBytes freeSpaceMin, StartupOption option, CorruptionPolicy logCorruptionPolicy) {
    LOG.debug("newRaftStorage: {}, freeSpaceMin={}, option={}, logCorruptionPolicy={}",
        dir, freeSpaceMin, option, logCorruptionPolicy);
    this.storageDir = new RaftStorageDirectoryImpl(dir, freeSpaceMin);
    this.logCorruptionPolicy = Optional.ofNullable(logCorruptionPolicy).orElseGet(CorruptionPolicy::getDefault);
    this.startupOption = option;
  }

  @Override
  public void initialize() throws IOException {
    try {
      if (startupOption == StartupOption.FORMAT) {
        if (storageDir.analyzeStorage(false) == StorageState.NON_EXISTENT) {
          throw new IOException("Cannot format " + storageDir);
        }
        storageDir.lock();
        format();
        state = storageDir.analyzeStorage(false);
      } else {
        // metaFile is initialized here
        state = analyzeAndRecoverStorage(true);
      }
    } catch (Throwable t) {
      unlockOnFailure(storageDir);
      throw t;
    }

    if (state != StorageState.NORMAL) {
      unlockOnFailure(storageDir);
      throw new IOException("Failed to load " + storageDir + ": " + state);
    }
  }

  static void unlockOnFailure(RaftStorageDirectoryImpl dir) {
    try {
      dir.unlock();
    } catch (Throwable t) {
      LOG.warn("Failed to unlock " + dir, t);
    }
  }

  StorageState getState() {
    return state;
  }

  public CorruptionPolicy getLogCorruptionPolicy() {
    return logCorruptionPolicy;
  }

  private void format() throws IOException {
    storageDir.clearDirectory();
    metaFile.set(storageDir.getMetaFile()).persist(RaftStorageMetadata.getDefault());
    LOG.info("Storage directory {} has been successfully formatted.", storageDir.getRoot());
  }

  private void cleanMetaTmpFile() throws IOException {
    FileUtils.deleteIfExists(storageDir.getMetaTmpFile());
  }

  private StorageState analyzeAndRecoverStorage(boolean toLock) throws IOException {
    StorageState storageState = storageDir.analyzeStorage(toLock);
    // Existence of raft-meta.tmp means the change of votedFor/term has not
    // been committed. Thus we should delete the tmp file.
    if (storageState != StorageState.NON_EXISTENT) {
      cleanMetaTmpFile();
    }
    if (storageState == StorageState.NORMAL) {
      final File f = storageDir.getMetaFile();
      if (!f.exists()) {
        throw new FileNotFoundException("Metadata file " + f + " does not exists.");
      }
      final RaftStorageMetadata metadata = metaFile.set(f).getMetadata();
      LOG.info("Read {} from {}", metadata, f);
      return StorageState.NORMAL;
    } else if (storageState == StorageState.NOT_FORMATTED &&
        storageDir.isCurrentEmpty()) {
      format();
      return StorageState.NORMAL;
    } else {
      return storageState;
    }
  }

  @Override
  public RaftStorageDirectoryImpl getStorageDir() {
    return storageDir;
  }

  @Override
  public void close() throws IOException {
    storageDir.unlock();
  }

  @Override
  public RaftStorageMetadataFile getMetadataFile() {
    return metaFile.get();
  }

  public void writeRaftConfiguration(LogEntryProto conf) {
    File confFile = storageDir.getMetaConfFile();
    try (OutputStream fio = new AtomicFileOutputStream(confFile)) {
      conf.writeTo(fio);
    } catch (Exception e) {
      LOG.error("Failed writing configuration to file:" + confFile, e);
    }
  }

  public RaftConfiguration readRaftConfiguration() {
    File confFile = storageDir.getMetaConfFile();
    if (!confFile.exists()) {
      return null;
    } else {
      try (InputStream fio = FileUtils.newInputStream(confFile)) {
        LogEntryProto confProto = LogEntryProto.newBuilder().mergeFrom(fio).build();
        return LogProtoUtils.toRaftConfiguration(confProto);
      } catch (Exception e) {
        LOG.error("Failed reading configuration from file:" + confFile, e);
        return null;
      }
    }
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":" + getStorageDir();
  }

  static class MetaFile {
    private final AtomicReference<RaftStorageMetadataFileImpl> ref = new AtomicReference<>();

    RaftStorageMetadataFile get() {
      return ref.get();
    }

    RaftStorageMetadataFile set(File file) {
      final RaftStorageMetadataFileImpl impl = new RaftStorageMetadataFileImpl(file);
      ref.set(impl);
      return impl;
    }
  }
}
