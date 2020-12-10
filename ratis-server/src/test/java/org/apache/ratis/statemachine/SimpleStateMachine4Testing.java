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
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogInputStream;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StateMachine} implementation example that simply stores all the log
 * entries in a list. Mainly used for test.
 *
 * For snapshot it simply merges all the log segments together.
 */
public class SimpleStateMachine4Testing extends BaseStateMachine {
  private static final int SNAPSHOT_THRESHOLD = 100;
  private static final Logger LOG = LoggerFactory.getLogger(SimpleStateMachine4Testing.class);
  private static final String RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_KEY
      = "raft.test.simple.state.machine.take.snapshot";
  private static final boolean RAFT_TEST_SIMPLE_STATE_MACHINE_TAKE_SNAPSHOT_DEFAULT = false;
  private boolean notifiedAsLeader;

  public static SimpleStateMachine4Testing get(RaftServer.Division s) {
    return (SimpleStateMachine4Testing)s.getStateMachine();
  }

  private final SortedMap<Long, LogEntryProto> indexMap = Collections.synchronizedSortedMap(new TreeMap<>());
  private final SortedMap<String, LogEntryProto> dataMap = Collections.synchronizedSortedMap(new TreeMap<>());
  private final Daemon checkpointer;
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final RaftProperties properties = new RaftProperties();
  private final long segmentMaxSize = RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
  private final long preallocatedSize = RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
  private final int bufferSize = RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();

  private volatile boolean running = true;

  public static class Collecting {
    public enum Type {
      APPLY_TRANSACTION
    }

    private final EnumMap<Type, BlockingQueue<Runnable>> map = new EnumMap<>(Type.class);

    BlockingQueue<Runnable> get(Type type) {
      return map.get(type);
    }

    public BlockingQueue<Runnable> enable(Type type) {
      final BlockingQueue<Runnable> q = new LinkedBlockingQueue<>();
      final BlockingQueue<Runnable> previous = map.put(type, q);
      Preconditions.assertNull(previous, "previous");
      return q;
    }

    <T> CompletableFuture<T> collect(Type type, T value) {
      final BlockingQueue<Runnable> q = get(type);
      if (q == null) {
        return CompletableFuture.completedFuture(value);
      }

      final CompletableFuture<T> future = new CompletableFuture<>();
      final boolean offered = q.offer(() -> future.complete(value));
      Preconditions.assertTrue(offered);
      return future;
    }
  }

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

    CompletableFuture<Void> getFuture(Type type) {
      return maps.getOrDefault(type, CompletableFuture.completedFuture(null));
    }

    void await(Type type) {
      try {
        getFuture(type).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Failed to await " + type, e);
      } catch(ExecutionException e) {
        throw new IllegalStateException("Failed to await " + type, e);
      }
    }
  }

  private final Blocking blocking = new Blocking();
  private final Collecting collecting = new Collecting();
  private long endIndexLastCkpt = RaftLog.INVALID_LOG_INDEX;
  private volatile RoleInfoProto slownessInfo = null;
  private volatile RoleInfoProto leaderElectionTimeoutInfo = null;

  private RaftGroupId groupId;

  public SimpleStateMachine4Testing() {
    checkpointer = new Daemon(() -> {
      while (running) {
        if (indexMap.lastKey() - endIndexLastCkpt >= SNAPSHOT_THRESHOLD) {
          endIndexLastCkpt = takeSnapshot();
        }

        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      }
    });
  }

  public Collecting collecting() {
    return collecting;
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
        LogProtoUtils.toLogEntryString(entry));
  }

  @Override
  public synchronized void initialize(RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException {
    LOG.info("Initializing " + this);
    this.groupId = groupId;
    getLifeCycle().startAndTransition(() -> {
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
    getLifeCycle().transition(LifeCycle.State.PAUSING);
    getLifeCycle().transition(LifeCycle.State.PAUSED);
  }

  @Override
  public synchronized void reinitialize() throws IOException {
    LOG.info("Reinitializing " + this);
    loadSnapshot(storage.findLatestSnapshot());
    if (getLifeCycleState() == LifeCycle.State.PAUSED) {
      getLifeCycle().transition(LifeCycle.State.STARTING);
      getLifeCycle().transition(LifeCycle.State.RUNNING);
    }
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry());
    put(entry);
    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());

    final SimpleMessage m = new SimpleMessage(entry.getIndex() + " OK");
    return collecting.collect(Collecting.Type.APPLY_TRANSACTION, m);
  }

  @Override
  public long takeSnapshot() {
    final TermIndex termIndex = getLastAppliedTermIndex();
    if (termIndex.getTerm() <= 0 || termIndex.getIndex() <= 0) {
      return RaftLog.INVALID_LOG_INDEX;
    }
    final long endIndex = termIndex.getIndex();

    // TODO: snapshot should be written to a tmp file, then renamed
    File snapshotFile = storage.getSnapshotFile(termIndex.getTerm(),
        termIndex.getIndex());
    LOG.debug("Taking a snapshot with t:{}, i:{}, file:{}", termIndex.getTerm(),
        termIndex.getIndex(), snapshotFile);
    try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(snapshotFile, false,
        segmentMaxSize, preallocatedSize, ByteBuffer.allocateDirect(bufferSize))) {
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
      return RaftLog.INVALID_LOG_INDEX;
    } else {
      LOG.info("Loading snapshot {}", snapshot);
      final long endIndex = snapshot.getIndex();
      try (SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(
          snapshot.getFile().getPath().toFile(), 0, endIndex, false)) {
        LogEntryProto entry;
        while ((entry = in.nextEntry()) != null) {
          put(entry);
          updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        }
      }
      // The end index is greater than last entry in indexMap as it also
      // includes the configuration and metadata entries
      Preconditions.assertTrue(
          !indexMap.isEmpty() && endIndex >= indexMap.lastKey(),
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
  public CompletableFuture<Void> write(LogEntryProto entry) {
    return blocking.getFuture(Blocking.Type.WRITE_STATE_MACHINE_DATA);
  }

  @Override
  public CompletableFuture<ByteString> read(LogEntryProto entry) {
    return blocking.getFuture(Blocking.Type.READ_STATE_MACHINE_DATA)
        .thenApply(v -> STATE_MACHINE_DATA);
  }

  @Override
  public CompletableFuture<Void> flush(long index) {
    return blocking.getFuture(Blocking.Type.FLUSH_STATE_MACHINE_DATA);
  }

  @Override
  public void close() {
    getLifeCycle().checkStateAndClose(() -> {
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
  public void notifyFollowerSlowness(RoleInfoProto roleInfoProto) {
    LOG.info("{}: notifySlowness {}, {}", this, groupId, roleInfoProto);
    slownessInfo = roleInfoProto;
  }

  @Override
  public void notifyExtendedNoLeader(RoleInfoProto roleInfoProto) {
    LOG.info("{}: notifyExtendedNoLeader {}, {}", this, groupId, roleInfoProto);
    leaderElectionTimeoutInfo = roleInfoProto;
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId raftPeerId) {
    if (groupMemberId.getPeerId().equals(raftPeerId)) {
      notifiedAsLeader = true;
    }
  }

  public boolean isNotifiedAsLeader() {
    return notifiedAsLeader;
  }

  protected File getSMdir() {
    return storage.getSmDir();
  }
}
