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

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A state machine keeping a single integer value.
 * <p>
 * - Update:
 *   The update requests have to submit the current value.
 *   When the state machine receive an update request,
 *   if the value in request matches the value in the state machine,
 *   the state machine increments its value by 1 and then returns a success reply.
 *   Otherwise, it returns an exception reply.
 * <p>
 * - Query:
 *   When the state machine receive a query request,
 *   it returns the current value.
 */
public class MonotonicStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(MonotonicStateMachine.class);
  static int INTEGER_BYTE_SIZE = Integer.SIZE / Byte.SIZE;

  private int value;
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  SimpleStateMachineStorage getStorage() {
    return storage;
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    storage.init(raftStorage);
    reinitialize();
  }

  @Override
  public void reinitialize() throws IOException {
    final Snapshot snapshot = Snapshot.load(storage.getLatestSnapshot());
    LOG.info("{}: loaded snapshot {} from {}", getId(), snapshot, this, new Throwable());

    if (snapshot != null) {
      setSnapshot(snapshot);
    }
    LOG.info("{}: getLatestSnapshot {} from {}", getId(), getLatestSnapshot(), this);
    LOG.info("{}: getLastAppliedTermIndex {} from {}", getId(), getLastAppliedTermIndex(), this);
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    return CompletableFuture.completedFuture(Message.valueOf(String.valueOf(value)));
  }

  synchronized int getValue() {
    return value;
  }

  /** For loading snapshots. */
  synchronized void setSnapshot(Snapshot snapshot) {
    value = snapshot.getValue();
    updateLastAppliedTermIndex(snapshot.getApplied());
  }

  /**
   * For {@link #applyTransaction(TransactionContext)}.
   * @return -1 if success; otherwise return the current value.
   */
  synchronized int incrementValue(int expected, long term, long index) {
    if (value != expected) {
      return value;
    }

    value++;
    updateLastAppliedTermIndex(term, index);
    return -1;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final long index = trx.getLogEntry().getIndex();
    LOG.debug("{}: apply trx with index={}", getId(), index);

    final ByteString data = trx.getLogEntry().getStateMachineLogEntry().getLogData();
    if (data.size() != INTEGER_BYTE_SIZE) {
      return JavaUtils.completeExceptionally(new IllegalArgumentException(
          "Unexpected data size " + data.size() + " (expected: " + INTEGER_BYTE_SIZE + ")"));
    }

    final int parsed = data.asReadOnlyByteBuffer().getInt();
    final int returned = incrementValue(parsed, trx.getLogEntry().getTerm(), index);
    final boolean success = returned == -1;
    final int current = success? parsed: returned;
    LOG.info("{}: Parsed = {}, current = {}, success? {}", getId(), parsed, current, success);
    if (!success) {
      return JavaUtils.completeExceptionally(new IllegalArgumentException(
          "Value mismatched: parsed == " + parsed + " but current == " + current));
    }
    return CompletableFuture.completedFuture(Message.valueOf(String.valueOf(current + 1)));
  }

  synchronized Snapshot getSnapshot() {
    return new Snapshot(getLastAppliedTermIndex(), value);
  }

  void saveSnapshot(Snapshot snapshot, File snapshotFile) throws IOException {
    // write the counter value into the snapshot file
    final byte[] data = new byte[INTEGER_BYTE_SIZE];
    ByteBuffer.wrap(data).putInt(snapshot.getValue());

    final File dir = snapshotFile.getParentFile();
    try {
      final boolean made = dir.mkdirs();
      LOG.info("{}: mkDir? {}: {}", getId(), made, dir);
      Files.write(snapshotFile.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    } catch (IOException e) {
      throw new IOException("Failed to write snapshot file " + snapshotFile
          + " for last applied index=" + snapshot.getApplied(), e);
    }

    // update snapshot in storage
    final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
    storage.updateLatestSnapshot(new SingleFileSnapshotInfo(info, snapshot.getApplied()));

    FileUtils.listDir(dir, s -> LOG.info("{}", s), LOG::error);
  }

  @Override
  public synchronized long takeSnapshot() throws IOException {
    final Snapshot snapshot = getSnapshot();
    final TermIndex applied = snapshot.getApplied();
    final File snapshotFile = storage.getSnapshotFile(applied.getTerm(), applied.getIndex());
    saveSnapshot(snapshot, snapshotFile);
    return applied.getIndex();
  }

  static class Snapshot {
    private final TermIndex applied;
    private final int value;

    Snapshot(TermIndex applied, int value) {
      this.applied = applied;
      this.value = value;
    }

    TermIndex getApplied() {
      return applied;
    }

    int getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "Snapshot{applied=" + applied + ", value=" + value + '}';
    }

    static Snapshot load(SingleFileSnapshotInfo snapshot) throws IOException {
      if (snapshot == null) {
        return null;
      }
      final File snapshotFile = snapshot.getFile().getPath().toFile();
      if (!snapshotFile.exists()) {
        LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
        return null;
      }
      if (snapshotFile.length() != INTEGER_BYTE_SIZE) {
        throw new IOException("Unexpected snapshot file length" + snapshotFile + "(expected: " + INTEGER_BYTE_SIZE
            + ") for snapshot " + snapshot);
      }
      // verify md5
      final MD5Hash md5 = snapshot.getFile().getFileDigest();
      if (md5 != null) {
        MD5FileUtil.verifySavedMD5(snapshotFile, md5);
      }

      final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
      final byte[] data;
      try {
        data = Files.readAllBytes(snapshotFile.toPath());
      } catch (IOException e) {
        throw new IOException("Failed to load " + snapshot, e);
      }
      final int parsed = ByteBuffer.wrap(data).getInt();
      if (parsed < 0) {
        throw new IOException("Negative value " + parsed + " in snapshot " + snapshot);
      }
      return new Snapshot(last, parsed);
    }
  }

  static Message getUpdateRequest(int value) {
    final byte[] data = ByteBuffer.allocate(INTEGER_BYTE_SIZE).putInt(value).array();
    final String s = String.valueOf(value);
    return Message.valueOf(ByteString.copyFrom(data), () -> s);
  }

  static List<Message> getUpdateRequests(int base, int numMessages) {
    final List<Message> messages = new ArrayList<>();
    for(int i = 0; i < numMessages; i++) {
      messages.add(getUpdateRequest(base + i));
    }
    return messages;
  }
}
