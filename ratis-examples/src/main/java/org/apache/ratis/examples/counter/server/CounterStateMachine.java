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
package org.apache.ratis.examples.counter.server;

import org.apache.ratis.examples.counter.CounterCommand;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MD5FileUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link org.apache.ratis.statemachine.StateMachine} implementation for the {@link CounterServer}.
 * This class maintain a {@link AtomicInteger} object as a state and accept two commands:
 * <p>
 * - {@link CounterCommand#GET} is a readonly command
 *   which is handled by the {@link #query(Message)} method.
 * <p>
 * - {@link CounterCommand#INCREMENT} is a transactional command
 *   which is handled by the {@link #applyTransaction(TransactionContext)} method.
 */
public class CounterStateMachine extends BaseStateMachine {
  /** The state of the {@link CounterStateMachine}. */
  static class CounterState {
    private final TermIndex applied;
    private final int counter;

    CounterState(TermIndex applied, int counter) {
      this.applied = applied;
      this.counter = counter;
    }

    TermIndex getApplied() {
      return applied;
    }

    int getCounter() {
      return counter;
    }
  }

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final AtomicInteger counter = new AtomicInteger(0);

  /** @return the current state. */
  private synchronized CounterState getState() {
    return new CounterState(getLastAppliedTermIndex(), counter.get());
  }

  private synchronized void updateState(TermIndex applied, int counterValue) {
    updateLastAppliedTermIndex(applied);
    counter.set(counterValue);
  }

  private synchronized int incrementCounter(TermIndex termIndex) {
    updateLastAppliedTermIndex(termIndex);
    return counter.incrementAndGet();
  }

  /**
   * Initialize the state machine storage and then load the state.
   *
   * @param server  the server running this state machine
   * @param groupId the id of the {@link org.apache.ratis.protocol.RaftGroup}
   * @param raftStorage the storage of the server
   * @throws IOException if it fails to load the state.
   */
  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    storage.init(raftStorage);
    reinitialize();
  }

  /**
   * Simply load the latest snapshot.
   *
   * @throws IOException if it fails to load the state.
   */
  @Override
  public void reinitialize() throws IOException {
    load(storage.getLatestSnapshot());
  }

  /**
   * Store the current state as a snapshot file in the {@link #storage}.
   *
   * @return the index of the snapshot
   */
  @Override
  public long takeSnapshot() {
    //get the current state
    final CounterState state = getState();
    final long index = state.getApplied().getIndex();

    //create a file with a proper name to store the snapshot
    final File snapshotFile = storage.getSnapshotFile(state.getApplied().getTerm(), index);

    //write the counter value into the snapshot file
    try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(
        Files.newOutputStream(snapshotFile.toPath())))) {
      out.writeInt(state.getCounter());
    } catch (IOException ioe) {
      LOG.warn("Failed to write snapshot file \"" + snapshotFile
          + "\", last applied index=" + state.getApplied());
    }

    // update storage
    final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
    storage.updateLatestSnapshot(new SingleFileSnapshotInfo(info, state.getApplied()));

    //return the index of the stored snapshot (which is the last applied one)
    return index;
  }

  /**
   * Load the state of the state machine from the {@link #storage}.
   *
   * @param snapshot the information of the snapshot being loaded
   * @return the index of the snapshot or -1 if snapshot is invalid
   * @throws IOException if it failed to read from storage
   */
  private long load(SingleFileSnapshotInfo snapshot) throws IOException {
    //check null
    if (snapshot == null) {
      return RaftLog.INVALID_LOG_INDEX;
    }
    //check if the snapshot file exists.
    final Path snapshotPath = snapshot.getFile().getPath();
    if (!Files.exists(snapshotPath)) {
      LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotPath, snapshot);
      return RaftLog.INVALID_LOG_INDEX;
    }

    // verify md5
    final MD5Hash md5 = snapshot.getFile().getFileDigest();
    if (md5 != null) {
      MD5FileUtil.verifySavedMD5(snapshotPath.toFile(), md5);
    }

    //read the TermIndex from the snapshot file name
    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotPath.toFile());

    //read the counter value from the snapshot file
    final int counterValue;
    try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(snapshotPath)))) {
      counterValue = in.readInt();
    }

    //update state
    updateState(last, counterValue);

    return last.getIndex();
  }

  /**
   * Process {@link CounterCommand#GET}, which gets the counter value.
   *
   * @param request the GET request
   * @return a {@link Message} containing the current counter value as a {@link String}.
   */
  @Override
  public CompletableFuture<Message> query(Message request) {
    final String command = request.getContent().toStringUtf8();
    if (!CounterCommand.GET.matches(command)) {
      return JavaUtils.completeExceptionally(new IllegalArgumentException("Invalid Command: " + command));
    }
    return CompletableFuture.completedFuture(Message.valueOf(counter.toString()));
  }

  /**
   * Apply the {@link CounterCommand#INCREMENT} by incrementing the counter object.
   *
   * @param trx the transaction context
   * @return the message containing the updated counter value
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry();

    //check if the command is valid
    final String command = entry.getStateMachineLogEntry().getLogData().toStringUtf8();
    if (!CounterCommand.INCREMENT.matches(command)) {
      return JavaUtils.completeExceptionally(new IllegalArgumentException("Invalid Command: " + command));
    }
    //increment the counter and update term-index
    final TermIndex termIndex = TermIndex.valueOf(entry);
    final long incremented = incrementCounter(termIndex);

    //if leader, log the incremented value and the term-index
    if (trx.getServerRole() == RaftPeerRole.LEADER) {
      LOG.info("{}: Increment to {}", termIndex, incremented);
    }

    //return the new value of the counter to the client
    return CompletableFuture.completedFuture(Message.valueOf(String.valueOf(incremented)));
  }
}
