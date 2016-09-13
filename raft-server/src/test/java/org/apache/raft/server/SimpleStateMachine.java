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
package org.apache.raft.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Daemon;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.protocol.Message;
import org.apache.raft.server.storage.LogInputStream;
import org.apache.raft.server.storage.LogOutputStream;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.server.storage.RaftStorageDirectory;
import org.apache.raft.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.raft.server.RaftServerConfigKeys.RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT;

/**
 * A {@link StateMachine} implementation example that simply stores all the log
 * entries in a list. Mainly used for test.
 *
 * For snapshot it simply merges all the log segments together.
 */
public class SimpleStateMachine implements StateMachine {
  static volatile int SNAPSHOT_THRESHOLD = 100;
  static final Logger LOG = LoggerFactory.getLogger(SimpleStateMachine.class);
  public static final String RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_KEY
      = "raft.test.simple.state.machine.take.snapshot";
  public static final boolean RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_DEFAULT = false;

  private final List<LogEntryProto> list =
      Collections.synchronizedList(new ArrayList<>());
  private final Daemon checkpointer;
  private volatile boolean running = true;
  private long endIndexLastCkpt = RaftServerConstants.INVALID_LOG_INDEX;
  private RaftStorage storage;
  private RaftConfiguration raftConf;

  public SimpleStateMachine() {
    checkpointer = new Daemon(() -> {
      while (running) {
        if (list.get(list.size() - 1).getIndex() - endIndexLastCkpt >=
            SNAPSHOT_THRESHOLD) {
          LogEntryProto entry = list.get(list.size() - 1);
          File snapshotFile = storage.getStorageDir()
              .getSnapshotFile(entry.getTerm(), entry.getIndex());
          endIndexLastCkpt = takeSnapshot(snapshotFile, storage);
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
      }
    });
  }

  @Override
  public void initialize(RaftProperties properties, RaftStorage storage) {
    this.storage = storage;
    if (properties.getBoolean(RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_KEY,
        RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_DEFAULT)) {
      checkpointer.start();
    }
  }

  @Override
  public synchronized Message applyLogEntry(LogEntryProto entry) {
    Preconditions.checkArgument(list.isEmpty() ||
        entry.getIndex() - 1 == list.get(list.size() - 1).getIndex());
    list.add(entry);
    return null;
  }

  @Override
  public long takeSnapshot(File snapshotFile, RaftStorage storage) {
    final long endIndex = RaftStorageDirectory.getIndexFromSnapshotFile(snapshotFile);
    try (LogOutputStream out = new LogOutputStream(snapshotFile, false,
        RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT)) {
      for (final LogEntryProto entry : list) {
        if (entry.getIndex() > endIndex) {
          break;
        } else {
          out.write(entry);
        }
      }
      out.flush();
    } catch (IOException e) {
      LOG.warn("Failed to take snapshot", e);
    }

    try {
      final MD5Hash digest = MD5FileUtil.computeMd5ForFile(snapshotFile);
      MD5FileUtil.saveMD5File(snapshotFile, digest);
    } catch (IOException e) {
      LOG.warn("Hit IOException when computing MD5 for snapshot file "
          + snapshotFile, e);
    }

    // TODO: purge log segments
    return endIndex;
  }

  @Override
  public synchronized long loadSnapshot(File snapshotFile) throws IOException {
    if (snapshotFile == null || !snapshotFile.exists()) {
      LOG.warn("The snapshot file {} does not exist", snapshotFile);
      return RaftServerConstants.INVALID_LOG_INDEX;
    } else {
      final long endIndex = RaftStorageDirectory.getIndexFromSnapshotFile(snapshotFile);
      try (LogInputStream in =
               new LogInputStream(snapshotFile, 0, endIndex, false)) {
        LogEntryProto entry;
        while ((entry = in.nextEntry()) != null) {
          applyLogEntry(entry);
        }
      }
      Preconditions.checkState(
          !list.isEmpty() && endIndex == list.get(list.size() - 1).getIndex(),
          "endIndex=%s, list=%s", endIndex, list);
      this.endIndexLastCkpt = endIndex;
      return endIndex;
    }
  }

  @Override
  public synchronized long reloadSnapshot(File snapshotFile) throws IOException {
    if (snapshotFile == null || !snapshotFile.exists()) {
      LOG.warn("The snapshot file {} does not exist", snapshotFile);
      return RaftServerConstants.INVALID_LOG_INDEX;
    } else {
      final long endIndex = RaftStorageDirectory.getIndexFromSnapshotFile(snapshotFile);
      final long lastIndexInList = list.isEmpty() ?
          RaftServerConstants.INVALID_LOG_INDEX :
          list.get(list.size() - 1).getIndex();
      Preconditions.checkState(endIndex > lastIndexInList);
      try (LogInputStream in =
               new LogInputStream(snapshotFile, 0, endIndex, false)) {
        LogEntryProto entry;
        while ((entry = in.nextEntry()) != null) {
          if (entry.getIndex() > lastIndexInList) {
            applyLogEntry(entry);
          }
        }
      }
      Preconditions.checkState(endIndex == list.get(list.size() - 1).getIndex());
      this.endIndexLastCkpt = endIndex;
      return endIndex;
    }
  }

  @Override
  public void setRaftConfiguration(RaftConfiguration conf) {
    this.raftConf = conf;
  }

  @Override
  public RaftConfiguration getRaftConfiguration() {
    return this.raftConf;
  }

  @Override
  public void close() throws IOException {
    running = false;
    checkpointer.interrupt();
  }

  @VisibleForTesting
  public LogEntryProto[] getContent() {
    return list.toArray(new LogEntryProto[list.size()]);
  }
}
