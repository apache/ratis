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
package org.apache.raft.statemachine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.raft.RaftTestUtil.SimpleMessage;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.io.MD5Hash;
import org.apache.raft.protocol.Message;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.LogInputStream;
import org.apache.raft.server.storage.LogOutputStream;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.raft.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.raft.statemachine.SimpleStateMachineStorage.SingleFileSnapshotInfo;
import org.apache.raft.util.Daemon;
import org.apache.raft.util.LifeCycle;
import org.apache.raft.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link StateMachine} implementation example that simply stores all the log
 * entries in a list. Mainly used for test.
 *
 * For snapshot it simply merges all the log segments together.
 */
public class SimpleStateMachine extends BaseStateMachine {
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
  private SimpleStateMachineStorage storage;
  private TermIndexTracker termIndexTracker;
  private final RaftProperties properties = new RaftProperties();

  public SimpleStateMachine() {
    this.storage  = new SimpleStateMachineStorage();
    this.termIndexTracker = new TermIndexTracker();
    checkpointer = new Daemon(() -> {
      while (running) {
        try {
          if (list.get(list.size() - 1).getIndex() - endIndexLastCkpt >=
              SNAPSHOT_THRESHOLD) {
            endIndexLastCkpt = takeSnapshot();
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignored) {
          }
        } catch (IOException ioe) {
          LOG.warn("Received IOException in Checkpointer", ioe);
        }
      }
    });
  }

  @Override
  public synchronized void initialize(String id, RaftProperties properties,
      RaftStorage raftStorage) throws IOException {
    LOG.info("Initializing " + getClass().getSimpleName() + ":" + id);
    lifeCycle.startAndTransition(() -> {
      super.initialize(id, properties, raftStorage);
      storage.init(raftStorage);
      loadSnapshot(storage.findLatestSnapshot());

      if (properties.getBoolean(
          RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_KEY,
          RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_DEFAULT)) {
        checkpointer.start();
      }
    });
  }

  @Override
  public synchronized void pause() {
    lifeCycle.transition(LifeCycle.State.PAUSING);
    lifeCycle.transition(LifeCycle.State.PAUSED);
  }

  @Override
  public synchronized void reinitialize(String id, RaftProperties properties,
      RaftStorage storage) throws IOException {
    LOG.info("Reinitializing " + getClass().getSimpleName() + ":" + id);
    initialize(id, properties, storage);
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    LogEntryProto entry = trx.getLogEntry().get();
    list.add(entry);
    termIndexTracker.update(new TermIndex(entry.getTerm(), entry.getIndex()));
    return CompletableFuture.completedFuture(
        new SimpleMessage(entry.getIndex() + " OK"));
  }

  @Override
  public long takeSnapshot() throws IOException {
    TermIndex termIndex = termIndexTracker.getLatestTermIndex();
    if (termIndex.getTerm() <= 0 || termIndex.getIndex() <= 0) {
      return RaftServerConstants.INVALID_LOG_INDEX;
    }
    final long endIndex = termIndex.getIndex();

    // TODO: snapshot should be written to a tmp file, then renamed
    File snapshotFile = storage.getSnapshotFile(termIndex.getTerm(),
        termIndex.getIndex());
    LOG.debug("Taking a snapshot with t:{}, i:{}, file:{}", termIndex.getTerm(),
        termIndex.getIndex(), snapshotFile);
    try (LogOutputStream out = new LogOutputStream(snapshotFile, false, properties)) {
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

    try {
      this.storage.loadLatestSnapshot();
    } catch (IOException e) {
      LOG.warn("Hit IOException when loading latest snapshot for snapshot file "
          + snapshotFile, e);
    }
    // TODO: purge log segments
    return endIndex;
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  public synchronized long loadSnapshot(SingleFileSnapshotInfo snapshot)
      throws IOException {
    if (snapshot == null || !snapshot.getFile().getPath().toFile().exists()) {
      LOG.info("The snapshot file {} does not exist",
          snapshot == null ? null : snapshot.getFile());
      return RaftServerConstants.INVALID_LOG_INDEX;
    } else {
      LOG.info("Loading snapshot with t:{}, i:{}, file:{}", snapshot.getTerm(),
          snapshot.getIndex(), snapshot.getFile().getPath());
      final long endIndex = snapshot.getIndex();
      try (LogInputStream in = new LogInputStream(
          snapshot.getFile().getPath().toFile(), 0, endIndex, false)) {
        LogEntryProto entry;
        while ((entry = in.nextEntry()) != null) {
          list.add(entry);
          termIndexTracker.update(
              new TermIndex(entry.getTerm(), entry.getIndex()));
        }
      }
      Preconditions.checkState(
          !list.isEmpty() && endIndex == list.get(list.size() - 1).getIndex(),
          "endIndex=%s, list=%s", endIndex, list);
      this.endIndexLastCkpt = endIndex;
      termIndexTracker.init(snapshot.getTermIndex());
      this.storage.loadLatestSnapshot();
      return endIndex;
    }
  }

  @Override
  public CompletableFuture<RaftClientReply> query(
      RaftClientRequest request) {
    return CompletableFuture.completedFuture(
        new RaftClientReply(request, new SimpleMessage("query success")));
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request)
      throws IOException {
    return new TransactionContext(this, request, SMLogEntryProto.newBuilder()
        .setData(request.getMessage().getContent())
        .build());
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
    // do nothing
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> {
      running = false;
      checkpointer.interrupt();
    });
  }

  @VisibleForTesting
  public LogEntryProto[] getContent() {
    return list.toArray(new LogEntryProto[list.size()]);
  }
}
