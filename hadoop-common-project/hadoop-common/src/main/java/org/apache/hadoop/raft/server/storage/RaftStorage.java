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

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.server.storage.RaftStorageDirectory.StorageState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

class RaftStorage {
  private static final Logger LOG = LoggerFactory.getLogger(RaftStorage.class);

  private final RaftStorageDirectory storageDir;
  private final StorageState state;
  private MetaFile metaFile;

  RaftStorage(File dir) throws IOException {
    storageDir = new RaftStorageDirectory(dir);
    analyzeAndRecoverStorage(); // metaFile is initialized here
    state = StorageState.NORMAL;
  }

  StorageState getState() {
    return state;
  }

  void format() throws IOException {
    storageDir.clearDirectory();
    metaFile = writeMetaFile(MetaFile.DEFAULT_TERM, MetaFile.EMPTY_VOTEFOR);
    Preconditions.checkState(storageDir.analyzeStorage() == StorageState.NORMAL);
    LOG.info("Storage directory " + storageDir.getRoot()
        + " has been successfully formatted.");
  }

  private MetaFile writeMetaFile(long term, String votedFor) throws IOException {
    MetaFile metaFile = new MetaFile(storageDir.getMetaFile());
    metaFile.set(term, votedFor);
    return metaFile;
  }

  private void cleanMetaTmpFile() throws IOException {
    Files.deleteIfExists(storageDir.getMetaTmpFile().toPath());
  }

  void analyzeAndRecoverStorage() throws IOException {
    StorageState storageState = storageDir.analyzeStorage();
    if (storageState == StorageState.NORMAL) {
      metaFile = new MetaFile(storageDir.getMetaFile());
      assert metaFile.exists();
      metaFile.readFile();
      // Existence of raft-meta.tmp means the change of votedFor/term has been
      // committed. Thus we should delete the tmp file.
      cleanMetaTmpFile();
    } else if (storageState == StorageState.NOT_FORMATTED &&
        storageDir.isCurrentEmpty()) {
      format();
    }
    throw new IOException("Cannot load " + storageDir
        + ". Its state: " + storageState);
  }

  RaftStorageDirectory getStorageDir() {
    return storageDir;
  }
}
