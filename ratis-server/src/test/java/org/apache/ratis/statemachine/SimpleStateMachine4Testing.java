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
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.LogInputStream;
import org.apache.ratis.server.storage.LogOutputStream;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

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

  public static SimpleStateMachine4Testing get(RaftServer s) {
    return (SimpleStateMachine4Testing)s.getStateMachine();
  }

  private final List<LogEntryProto> list =
      Collections.synchronizedList(new ArrayList<>());
  private final Daemon checkpointer;
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final RaftProperties properties = new RaftProperties();
  private long segmentMaxSize =
      RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
  private long preallocatedSize =
      RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
  private int bufferSize =
      RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();

  private volatile boolean running = true;
  private boolean blockTransaction = false;
  private final Semaphore blockingSemaphore = new Semaphore(1);
  private long endIndexLastCkpt = RaftServerConstants.INVALID_LOG_INDEX;

  SimpleStateMachine4Testing() {
    checkpointer = new Daemon(() -> {
      while (running) {
          if (list.get(list.size() - 1).getIndex() - endIndexLastCkpt >=
              SNAPSHOT_THRESHOLD) {
            endIndexLastCkpt = takeSnapshot();
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignored) {
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
    LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry());
    list.add(entry);
    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
    return CompletableFuture.completedFuture(
        new SimpleMessage(entry.getIndex() + " OK"));
  }

  @Override
  public long takeSnapshot() {
    final TermIndex termIndex = getLastAppliedTermIndex();
    if (termIndex.getTerm() <= 0 || termIndex.getIndex() <= 0) {
      return RaftServerConstants.INVALID_LOG_INDEX;
    }
    final long endIndex = termIndex.getIndex();

    // TODO: snapshot should be written to a tmp file, then renamed
    File snapshotFile = storage.getSnapshotFile(termIndex.getTerm(),
        termIndex.getIndex());
    LOG.debug("Taking a snapshot with t:{}, i:{}, file:{}", termIndex.getTerm(),
        termIndex.getIndex(), snapshotFile);
    try (LogOutputStream out = new LogOutputStream(snapshotFile, false,
        segmentMaxSize, preallocatedSize, bufferSize)) {
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
          updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        }
      }
      Preconditions.assertTrue(
          !list.isEmpty() && endIndex == list.get(list.size() - 1).getIndex(),
          "endIndex=%s, list=%s", endIndex, list);
      this.endIndexLastCkpt = endIndex;
      setLastAppliedTermIndex(snapshot.getTermIndex());
      this.storage.loadLatestSnapshot();
      return endIndex;
    }
  }

  /**
   * Query the n-th log entry.
   * @param request an index represented in a UTF-8 String, or an empty message.
   * @return a completed future of the n-th log entry,
   *         where n is the last applied index if the request is empty,
   *         otherwise, n is the index represented in the request.
   */
  @Override
  public CompletableFuture<Message> query(Message request) {
    final ByteString bytes = request.getContent();
    try {
      final long index = bytes.isEmpty()? getLastAppliedTermIndex().getIndex()
          : Long.parseLong(bytes.toStringUtf8());
      LOG.info("query log index " + index);
      final LogEntryProto entry = list.get(Math.toIntExact(index - 1));
      return CompletableFuture.completedFuture(Message.valueOf(entry.toByteString()));
    } catch (Exception e) {
      LOG.warn("Failed request " + request, e);
      return JavaUtils.completeExceptionally(new StateMachineException(
          "Failed request " + request, e));
    }
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request)
      throws IOException {
    if (blockTransaction) {
      try {
        //blocks until blockTransaction is set to false
        blockingSemaphore.acquire();
        blockingSemaphore.release();
      } catch (InterruptedException e) {
        LOG.error("Could not block applyTransaction", e);
        Thread.currentThread().interrupt();
      }
    }
    return new TransactionContextImpl(this, request, SMLogEntryProto.newBuilder()
        .setData(request.getMessage().getContent())
        .build());
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

  public void setBlockTransaction(boolean blockTransactionVal) throws InterruptedException {
    this.blockTransaction = blockTransactionVal;
    if (blockTransactionVal) {
      blockingSemaphore.acquire();
    } else {
      blockingSemaphore.release();
    }
  }
}
