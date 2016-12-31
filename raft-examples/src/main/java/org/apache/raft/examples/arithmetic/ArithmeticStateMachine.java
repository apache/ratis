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
package org.apache.raft.examples.arithmetic;

import org.apache.raft.conf.RaftProperties;
import org.apache.raft.examples.arithmetic.expression.Expression;
import org.apache.raft.protocol.Message;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.server.impl.RaftServerConstants;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.raft.statemachine.*;
import org.apache.raft.statemachine.SimpleStateMachineStorage.SingleFileSnapshotInfo;
import org.apache.raft.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ArithmeticStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(ArithmeticStateMachine.class);

  private final Map<String, Double> variables = new ConcurrentHashMap<>();

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final TermIndexTracker termIndexTracker = new TermIndexTracker();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  void reset() {
    variables.clear();
    termIndexTracker.reset();
  }

  @Override
  public void initialize(String id, RaftProperties properties, RaftStorage raftStorage)
      throws IOException {
    super.initialize(id, properties, raftStorage);
    this.storage.init(raftStorage);
    loadSnapshot(storage.getLatestSnapshot());
  }

  @Override
  public void reinitialize(String id, RaftProperties properties, RaftStorage storage)
      throws IOException {
    close();
    this.initialize(id, properties, storage);
  }

  @Override
  public long takeSnapshot() throws IOException {
    final Map<String, Double> copy;
    final TermIndex last;
    try(final AutoCloseableLock readLock = readLock()) {
      copy = new HashMap<>(variables);
      last = termIndexTracker.getLatestTermIndex();
    }

    File snapshotFile =  new File(SimpleStateMachineStorage.getSnapshotFileName(
        last.getTerm(), last.getIndex()));

    try(final ObjectOutputStream out = new ObjectOutputStream(
        new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
      out.writeObject(copy);
    } catch(IOException ioe) {
      LOG.warn("Failed to write snapshot file \"" + snapshotFile
          + "\", last applied index=" + last);
    }

    return last.getIndex();
  }

  public long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    return load(snapshot, false);
  }

  private long load(SingleFileSnapshotInfo snapshot, boolean reload) throws IOException {
    if (snapshot == null || !snapshot.getFile().getPath().toFile().exists()) {
      LOG.warn("The snapshot file {} does not exist", snapshot);
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    File snapshotFile =snapshot.getFile().getPath().toFile();
    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try(final AutoCloseableLock writeLock = writeLock();
        final ObjectInputStream in = new ObjectInputStream(
            new BufferedInputStream(new FileInputStream(snapshotFile)))) {
      if (reload) {
        reset();
      }
      termIndexTracker.init(last);
      variables.putAll((Map<String, Double>) in.readObject());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
    return last.getIndex();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public CompletableFuture<RaftClientReply> query(
      RaftClientRequest request) {
    final Expression q = Expression.Utils.bytes2Expression(
        request.getMessage().getContent().toByteArray(), 0);
    final Double result;
    try(final AutoCloseableLock readLock = readLock()) {
      result = q.evaluate(variables);
    }
    final Expression r = Expression.Utils.double2Expression(result);
    LOG.debug("QUERY: {} = {}", q, r);
    final RaftClientReply reply = new RaftClientReply(request,
        Expression.Utils.toMessage(r));
    return CompletableFuture.completedFuture(reply);
  }

  @Override
  public void close() {
    reset();
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry().get();
    final AssignmentMessage assignment = new AssignmentMessage(
        () -> entry.getSmLogEntry().getData());

    final long index = entry.getIndex();
    final Double result;
    try(final AutoCloseableLock writeLock = writeLock()) {
      result = assignment.evaluate(variables);
      termIndexTracker.update(new TermIndex(entry.getTerm(), index));
    }
    final Expression r = Expression.Utils.double2Expression(result);
    LOG.debug("{}: {} = {}, variables={}", index, assignment, r, variables);
    return CompletableFuture.completedFuture(Expression.Utils.toMessage(r));
  }
}
