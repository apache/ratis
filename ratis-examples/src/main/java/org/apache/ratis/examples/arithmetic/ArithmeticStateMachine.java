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
package org.apache.ratis.examples.arithmetic;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.arithmetic.expression.Expression;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.*;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ArithmeticStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(ArithmeticStateMachine.class);

  private final Map<String, Double> variables = new ConcurrentHashMap<>();

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final AtomicReference<TermIndex> latestTermIndex = new AtomicReference<>();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  void reset() {
    variables.clear();
    latestTermIndex.set(null);
  }

  @Override
  public void initialize(RaftPeerId id, RaftProperties properties,
      RaftStorage raftStorage) throws IOException {
    super.initialize(id, properties, raftStorage);
    this.storage.init(raftStorage);
    loadSnapshot(storage.getLatestSnapshot());
  }

  @Override
  public void reinitialize(RaftPeerId id, RaftProperties properties,
      RaftStorage storage) throws IOException {
    close();
    this.initialize(id, properties, storage);
  }

  @Override
  public long takeSnapshot() throws IOException {
    final Map<String, Double> copy;
    final TermIndex last;
    try(final AutoCloseableLock readLock = readLock()) {
      copy = new HashMap<>(variables);
      last = latestTermIndex.get();
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
      latestTermIndex.set(last);
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
      updateLatestTermIndex(entry.getTerm(), index);
    }
    final Expression r = Expression.Utils.double2Expression(result);
    LOG.debug("{}: {} = {}, variables={}", index, assignment, r, variables);
    return CompletableFuture.completedFuture(Expression.Utils.toMessage(r));
  }

  private void updateLatestTermIndex(long term, long index) {
    final TermIndex newTI = TermIndex.newTermIndex(term, index);
    final TermIndex oldTI = latestTermIndex.getAndSet(newTI);
    if (oldTI != null) {
      Preconditions.assertTrue(newTI.compareTo(oldTI) >= 0);
    }
  }
}
