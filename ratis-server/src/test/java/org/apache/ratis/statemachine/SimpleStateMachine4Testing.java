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
package org.apache.ratis.statemachine;

import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.LogInputStream;
import org.apache.ratis.server.storage.LogOutputStream;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.MD5FileUtil;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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

  private final SortedMap<Long, LogEntryProto> indexMap = Collections.synchronizedSortedMap(new TreeMap<>());
  private final SortedMap<String, LogEntryProto> dataMap = Collections.synchronizedSortedMap(new TreeMap<>());
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


  static class Blocking {
    enum Type {
      START_TRANSACTION, READ_STATE_MACHINE_DATA, WRITE_STATE_MACHINE_DATA, FLUSH_STATE_MACHINE_DATA
    }

    private final EnumMap<Type, CompletableFuture<Void>> maps = new EnumMap<>(Type.class);

    void block(Type type) {
      LOG.info("block {}", type);
      final CompletableFuture<Void> future = new CompletableFuture<>();
      final CompletableFuture<Void> previous = maps.putIfAbsent(type, future);
      Preconditions.assertNull(previous, "previous");
    }

    void unblock(Type type) {
      LOG.info("unblock {}", type);
      final CompletableFuture<Void> future = maps.remove(type);
      Objects.requireNonNull(future, "future == null");
      future.complete(null);
    }

    void await(Type type) {
      try {
        maps.getOrDefault(type, CompletableFuture.completedFuture(null)).get();
      } catch(InterruptedException | ExecutionException e) {
        throw new IllegalStateException("Failed to await " + type, e);
      }
    }
  }

  private final Blocking blocking = new Blocking();
  private long endIndexLastCkpt = RaftServerConstants.INVALID_LOG_INDEX;
  private volatile RoleInfoProto slownessInfo = null;
  private volatile RoleInfoProto leaderElectionTimeoutInfo = null;

  public SimpleStateMachine4Testing() {
    checkpointer = new Daemon(() -> {
      while (running) {
        if (indexMap.lastKey() - endIndexLastCkpt >= SNAPSHOT_THRESHOLD) {
          endIndexLastCkpt = takeSnapshot();
        }

        try {
          TimeUnit.SECONDS.sleep(1);
        } catch(InterruptedException ignored) {
        }
      }
    });
  }

  public RoleInfoProto getSlownessInfo() {
    return slownessInfo;
  }

  public RoleInfoProto getLeaderElectionTimeoutInfo() {
    return leaderElectionTimeoutInfo;
  }

  private void put(LogEntryProto entry) {
    final LogEntryProto previous = indexMap.put(entry.getIndex(), entry);
    Preconditions.assertNull(previous, "previous");
    final String s = entry.getStateMachineLogEntry().getLogData().toStringUtf8();
    dataMap.put(s, entry);
    LOG.info("{}: put {}, {} -> {}", getId(), entry.getIndex(),
        s.length() <= 10? s: s.substring(0, 10) + "...",
        ServerProtoUtils.toLogEntryString(entry));
  }

  @Override
  public synchronized void initialize(RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException {
    LOG.info("Initializing " + this);
    lifeCycle.startAndTransition(() -> {
      super.initialize(server, groupId, raftStorage);
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
  public synchronized void reinitialize() throws IOException {
    LOG.info("Reinitializing " + this);
    loadSnapshot(storage.findLatestSnapshot());
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry());
    put(entry);
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
      for (final LogEntryProto entry : indexMap.values()) {
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
          put(entry);
          updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        }
      }
      Preconditions.assertTrue(
          !indexMap.isEmpty() && endIndex == indexMap.lastKey(),
          "endIndex=%s, indexMap=%s", endIndex, indexMap);
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
    final String string = request.getContent().toStringUtf8();
    Exception exception;
    try {
      LOG.info("query " + string);
      final LogEntryProto entry = dataMap.get(string);
      if (entry != null) {
        return CompletableFuture.completedFuture(Message.valueOf(entry.toByteString()));
      }
      exception = new IndexOutOfBoundsException(getId() + ": LogEntry not found for query " + string);
    } catch (Exception e) {
      LOG.warn("Failed request " + request, e);
      exception = e;
    }
    return JavaUtils.completeExceptionally(new StateMachineException(
        "Failed request " + request, exception));
  }

  static final ByteString STATE_MACHINE_DATA = ByteString.copyFromUtf8("StateMachine Data");

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) {
    blocking.await(Blocking.Type.START_TRANSACTION);
    return TransactionContext.newBuilder()
        .setStateMachine(this)
        .setClientRequest(request)
        .setStateMachineData(STATE_MACHINE_DATA)
        .build();
  }

  @Override
  public CompletableFuture<?> writeStateMachineData(LogEntryProto entry) {
    blocking.await(Blocking.Type.WRITE_STATE_MACHINE_DATA);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<ByteString> readStateMachineData(LogEntryProto entry) {
    blocking.await(Blocking.Type.READ_STATE_MACHINE_DATA);
    return CompletableFuture.completedFuture(STATE_MACHINE_DATA);
  }

  @Override
  public CompletableFuture<Void> flushStateMachineData(long index) {
    blocking.await(Blocking.Type.FLUSH_STATE_MACHINE_DATA);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> {
      running = false;
      checkpointer.interrupt();
    });
  }

  public LogEntryProto[] getContent() {
    return indexMap.values().toArray(new LogEntryProto[0]);
  }

  public void blockStartTransaction() {
    blocking.block(Blocking.Type.START_TRANSACTION);
  }
  public void unblockStartTransaction() {
    blocking.unblock(Blocking.Type.START_TRANSACTION);
  }

  public void blockWriteStateMachineData() {
    blocking.block(Blocking.Type.WRITE_STATE_MACHINE_DATA);
  }
  public void unblockWriteStateMachineData() {
    blocking.unblock(Blocking.Type.WRITE_STATE_MACHINE_DATA);
  }

  public void blockFlushStateMachineData() {
    blocking.block(Blocking.Type.FLUSH_STATE_MACHINE_DATA);
  }
  public void unblockFlushStateMachineData() {
    blocking.unblock(Blocking.Type.FLUSH_STATE_MACHINE_DATA);
  }

  @Override
  public void notifySlowness(RaftGroup group, RoleInfoProto roleInfoProto) {
    LOG.info("{}: notifySlowness {}, {}", this, group, roleInfoProto);
    slownessInfo = roleInfoProto;
  }

  @Override
  public void notifyExtendedNoLeader(RaftGroup group, RoleInfoProto roleInfoProto) {
    LOG.info("{}: notifyExtendedNoLeader {}, {}", this, group, roleInfoProto);
    leaderElectionTimeoutInfo = roleInfoProto;
  }
}
