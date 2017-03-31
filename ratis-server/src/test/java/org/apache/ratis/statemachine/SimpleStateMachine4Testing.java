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
package org.apache.ratis.statemachine;

import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.LogInputStream;
import org.apache.ratis.server.storage.LogOutputStream;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.MD5FileUtil;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link StateMachine} implementation example that simply stores all the log
 * entries in a list. Mainly used for test.
 *
 * For snapshot it simply merges all the log segments together.
 */
public class SimpleStateMachine4Testing extends BaseStateMachine {
  private static volatile int SNAPSHOT_THRESHOLD = 100;
  private static final Logger LOG = LoggerFactory.getLogger(SimpleStateMachine4Testing.class);
  private static final String RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_KEY
      = "raft.test.simple.state.machine.take.snapshot";
  private static final boolean RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_DEFAULT = false;

  public static SimpleStateMachine4Testing get(RaftServerImpl s) {
    return (SimpleStateMachine4Testing)s.getStateMachine();
  }

  private final List<LogEntryProto> list =
      Collections.synchronizedList(new ArrayList<>());
  private final Daemon checkpointer;
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final TermIndexTracker termIndexTracker = new TermIndexTracker();
  private final RaftProperties properties = new RaftProperties();

  private volatile boolean running = true;
  private long endIndexLastCkpt = RaftServerConstants.INVALID_LOG_INDEX;

  SimpleStateMachine4Testing() {
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
  public synchronized void initialize(RaftPeerId id, RaftProperties properties,
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
  public synchronized void reinitialize(RaftPeerId id, RaftProperties properties,
      RaftStorage storage) throws IOException {
    LOG.info("Reinitializing " + getClass().getSimpleName() + ":" + id);
    initialize(id, properties, storage);
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry().get());
    list.add(entry);
    termIndexTracker.update(ServerProtoUtils.toTermIndex(entry));
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
  public SimpleStateMachineStorage getStateMachineStorage() {
    return storage;
  }

  private synchronized long loadSnapshot(SingleFileSnapshotInfo snapshot)
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
          termIndexTracker.update(ServerProtoUtils.toTermIndex(entry));
        }
      }
      Preconditions.assertTrue(
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

  public LogEntryProto[] getContent() {
    return list.toArray(new LogEntryProto[list.size()]);
  }
}
