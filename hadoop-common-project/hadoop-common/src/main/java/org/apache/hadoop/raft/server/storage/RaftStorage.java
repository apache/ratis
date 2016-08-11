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
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.storage.RaftStorageDirectory.LogPathAndIndex;
import org.apache.hadoop.raft.server.storage.RaftStorageDirectory.SnapshotPathAndTermIndex;
import org.apache.hadoop.raft.server.storage.RaftStorageDirectory.StorageState;
import org.apache.hadoop.raft.util.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.apache.hadoop.raft.server.RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_DEFAULT;
import static org.apache.hadoop.raft.server.RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_KEY;

public class RaftStorage implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RaftStorage.class);

  // TODO support multiple storage directories
  private final RaftStorageDirectory storageDir;
  private final StorageState state;
  private volatile MetaFile metaFile;

  public RaftStorage(RaftProperties prop, RaftConstants.StartupOption option)
      throws IOException {
    final String dir = prop.get(RAFT_SERVER_STORAGE_DIR_KEY,
        RAFT_SERVER_STORAGE_DIR_DEFAULT);
    storageDir = new RaftStorageDirectory(
        new File(RaftUtils.stringAsURI(dir).getPath()));
    if (option == RaftConstants.StartupOption.FORMAT) {
      if (storageDir.analyzeStorage(false) == StorageState.NON_EXISTENT) {
        throw new IOException("Cannot format " + storageDir);
      }
      storageDir.lock();
      format();
      Preconditions.checkState((state = storageDir.analyzeStorage(false)) ==
          StorageState.NORMAL);
    } else {
      state = analyzeAndRecoverStorage(true); // metaFile is initialized here
      if (state != StorageState.NORMAL) {
        storageDir.unlock();
        throw new IOException("Cannot load " + storageDir
            + ". Its state: " + state);
      }
    }
  }

  StorageState getState() {
    return state;
  }

  private void format() throws IOException {
    storageDir.clearDirectory();
    metaFile = writeMetaFile(MetaFile.DEFAULT_TERM, MetaFile.EMPTY_VOTEFOR);
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

  private StorageState analyzeAndRecoverStorage(boolean toLock)
      throws IOException {
    StorageState storageState = storageDir.analyzeStorage(toLock);
    if (storageState == StorageState.NORMAL) {
      metaFile = new MetaFile(storageDir.getMetaFile());
      assert metaFile.exists();
      metaFile.readFile();
      // Existence of raft-meta.tmp means the change of votedFor/term has not
      // been committed. Thus we should delete the tmp file.
      cleanMetaTmpFile();
      return StorageState.NORMAL;
    } else if (storageState == StorageState.NOT_FORMATTED &&
        storageDir.isCurrentEmpty()) {
      format();
      return StorageState.NORMAL;
    } else {
      return storageState;
    }
  }

  public RaftStorageDirectory getStorageDir() {
    return storageDir;
  }

  @Override
  public void close() throws IOException {
    storageDir.unlock();
  }

  MetaFile getMetaFile() {
    return metaFile;
  }

  public SnapshotPathAndTermIndex getLastestSnapshotPath() throws IOException {
    return storageDir.getLatestSnapshot();
  }
}
