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

package org.apache.ratis.statemachine.impl;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base implementation for StateMachines.
 */
public class BaseStateMachine implements StateMachine {
  private final CompletableFuture<RaftServer> server = new CompletableFuture<>();
  private volatile RaftGroupId groupId;
  protected final LifeCycle lifeCycle = new LifeCycle(getClass().getSimpleName());

  private final AtomicReference<TermIndex> lastAppliedTermIndex = new AtomicReference<>();

  private final SortedMap<Long, CompletableFuture<Void>> transactionFutures = new TreeMap<>();

  public RaftPeerId getId() {
    return server.isDone()? server.join().getId(): null;
  }

  @Override
  public LifeCycle.State getLifeCycleState() {
    return lifeCycle.getCurrentState();
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException {
    this.groupId = groupId;
    this.server.complete(server);
    lifeCycle.setName("" + this);
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    return getStateMachineStorage().getLatestSnapshot();
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) throws IOException {
    // do nothing
  }

  @Override
  public void pause() {
  }

  @Override
  public void reinitialize() throws IOException {
  }

  @Override
  public TransactionContext applyTransactionSerial(TransactionContext trx) {
    return trx;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    // return the same message contained in the entry
    return CompletableFuture.completedFuture(
        Message.valueOf(trx.getLogEntry().getStateMachineLogEntry().getLogData()));
  }

  @Override
  public TermIndex getLastAppliedTermIndex() {
    return lastAppliedTermIndex.get();
  }

  protected void setLastAppliedTermIndex(TermIndex newTI) {
    lastAppliedTermIndex.set(newTI);
  }

  protected boolean updateLastAppliedTermIndex(long term, long index) {
    final TermIndex newTI = TermIndex.newTermIndex(term, index);
    final TermIndex oldTI = lastAppliedTermIndex.getAndSet(newTI);
    if (!newTI.equals(oldTI)) {
      LOG.trace("{}: update lastAppliedTermIndex from {} to {}", getId(), oldTI, newTI);
      if (oldTI != null) {
        Preconditions.assertTrue(newTI.compareTo(oldTI) >= 0,
            () -> getId() + ": Failed updateLastAppliedTermIndex: newTI = "
                + newTI + " < oldTI = " + oldTI);
      }
      return true;
    }

    synchronized (transactionFutures) {
      for(long i; !transactionFutures.isEmpty() && (i = transactionFutures.firstKey()) <= index; ) {
        transactionFutures.remove(i).complete(null);
      }
    }
    return false;
  }

  @Override
  public long takeSnapshot() throws IOException {
    return RaftServerConstants.INVALID_LOG_INDEX;
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return new StateMachineStorage() {
      @Override
      public void init(RaftStorage raftStorage) throws IOException {
      }

      @Override
      public SnapshotInfo getLatestSnapshot() {
        return null;
      }

      @Override
      public void format() throws IOException {
      }
    };
  }

  @Override
  public CompletableFuture<Message> queryStale(Message request, long minIndex) {
    if (getLastAppliedTermIndex().getIndex() < minIndex) {
      synchronized (transactionFutures) {
        if (getLastAppliedTermIndex().getIndex() < minIndex) {
          return transactionFutures.computeIfAbsent(minIndex, key -> new CompletableFuture<>())
              .thenCompose(v -> query(request));
        }
      }
    }
    return query(request);
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
    return TransactionContext.newBuilder()
        .setStateMachine(this)
        .setClientRequest(request)
        .build();
  }

  @Override
  public TransactionContext cancelTransaction(TransactionContext trx) throws IOException {
    return trx;
  }

  @Override
  public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
    return trx;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":"
        + (!server.isDone()? "uninitialized": getId() + ":" + groupId);
  }
}
