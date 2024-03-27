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
package org.apache.ratis.statemachine.impl;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Implementation of {@link TransactionContext}
 *
 * This is a private API.  Applications should use {@link TransactionContext} and {@link TransactionContext.Builder}.
 */
public class TransactionContextImpl implements TransactionContext {
  /** The role of the server when this object is created. */
  private final RaftPeerRole serverRole;
  /** The {@link StateMachine} that originated the transaction. */
  private final StateMachine stateMachine;

  /** Original request from the client */
  private final RaftClientRequest clientRequest;

  /** Exception from the {@link StateMachine} or from the log */
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile Exception exception;

  /** Data from the {@link StateMachine} */
  private final StateMachineLogEntryProto stateMachineLogEntry;

  /**
   * Context specific to the state machine.
   * The {@link StateMachine} can use this object to carry state between
   * {@link StateMachine#startTransaction(RaftClientRequest)} and
   * {@link StateMachine#applyTransaction(TransactionContext)}.
   */
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile Object stateMachineContext;

  /**
   * Whether to commit the transaction to the RAFT Log.
   * In some cases the {@link StateMachine} may want to indicate
   * that the transaction should not be committed
   */
  private boolean shouldCommit = true;

  /** Committed LogEntry. */
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile LogEntryProto logEntry;
  /** Committed LogEntry copy. */
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile Supplier<LogEntryProto> logEntryCopy;

  /** For wrapping {@link #logEntry} in order to release the underlying buffer. */
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile ReferenceCountedObject<?> delegatedRef;

  private final CompletableFuture<Long> logIndexFuture = new CompletableFuture<>();

  private TransactionContextImpl(RaftPeerRole serverRole, RaftClientRequest clientRequest, StateMachine stateMachine,
      StateMachineLogEntryProto stateMachineLogEntry) {
    this.serverRole = serverRole;
    this.clientRequest = clientRequest;
    this.stateMachine = stateMachine;
    this.stateMachineLogEntry = stateMachineLogEntry;
  }

  /**
   * Construct a {@link TransactionContext} from a client request.
   * Used by the state machine to start a transaction
   * and send the Log entry representing the transaction data
   * to be applied to the raft log.
   */
  TransactionContextImpl(RaftClientRequest clientRequest, StateMachine stateMachine,
      StateMachineLogEntryProto stateMachineLogEntry, ByteString logData, ByteString stateMachineData,
      Object stateMachineContext) {
    this(RaftPeerRole.LEADER, clientRequest, stateMachine,
        get(stateMachineLogEntry, clientRequest, logData, stateMachineData));
    this.stateMachineContext = stateMachineContext;
  }

  private static StateMachineLogEntryProto get(StateMachineLogEntryProto stateMachineLogEntry,
      RaftClientRequest clientRequest, ByteString logData, ByteString stateMachineData) {
    if (stateMachineLogEntry != null) {
      return stateMachineLogEntry;
    } else {
      return LogProtoUtils.toStateMachineLogEntryProto(clientRequest, logData, stateMachineData);
    }
  }

  /**
   * Construct a {@link TransactionContext} from a {@link LogEntryProto}.
   * Used by followers for applying committed entries to the state machine.
   * @param logEntry the log entry to be applied
   */
  TransactionContextImpl(RaftPeerRole serverRole, StateMachine stateMachine, LogEntryProto logEntry) {
    this(serverRole, null, stateMachine, logEntry.getStateMachineLogEntry());
    setLogEntry(logEntry);
    this.logIndexFuture.complete(logEntry.getIndex());
  }

  @Override
  public RaftPeerRole getServerRole() {
    return serverRole;
  }

  @Override
  public RaftClientRequest getClientRequest() {
    return clientRequest;
  }

  public void setDelegatedRef(ReferenceCountedObject<?> ref) {
    this.delegatedRef = ref;
  }

  @Override
  public ReferenceCountedObject<LogEntryProto> wrap(LogEntryProto entry) {
    if (delegatedRef == null) {
      return TransactionContext.super.wrap(entry);
    }
    final LogEntryProto expected = getLogEntryUnsafe();
    Objects.requireNonNull(expected, "logEntry == null");
    Preconditions.assertSame(expected.getTerm(), entry.getTerm(), "entry.term");
    Preconditions.assertSame(expected.getIndex(), entry.getIndex(), "entry.index");
    return delegatedRef.delegate(entry);
  }

  @Override
  public StateMachineLogEntryProto getStateMachineLogEntry() {
    return stateMachineLogEntry;
  }

  @Override
  public Exception getException() {
    return exception;
  }

  @Override
  public TransactionContext setStateMachineContext(Object context) {
    this.stateMachineContext = context;
    return this;
  }

  @Override
  public Object getStateMachineContext() {
    return stateMachineContext;
  }

  @Override
  public LogEntryProto initLogEntry(long term, long index) {
    Preconditions.assertTrue(serverRole == RaftPeerRole.LEADER);
    Preconditions.assertNull(logEntry, "logEntry");
    Objects.requireNonNull(stateMachineLogEntry, "stateMachineLogEntry == null");

    logIndexFuture.complete(index);
    return setLogEntry(LogProtoUtils.toLogEntryProto(stateMachineLogEntry, term, index));
  }

  public CompletableFuture<Long> getLogIndexFuture() {
    return logIndexFuture;
  }

  private LogEntryProto setLogEntry(LogEntryProto entry) {
    this.logEntry = entry;
    this.logEntryCopy = MemoizedSupplier.valueOf(() -> LogProtoUtils.copy(entry));
    return entry;
  }


  @Override
  public LogEntryProto getLogEntry() {
    return logEntryCopy == null ? null : logEntryCopy.get();
  }

  @Override
  public LogEntryProto getLogEntryUnsafe() {
    return logEntry;
  }


  @Override
  public TransactionContext setException(Exception ioe) {
    this.exception = ioe;
    return this;
  }

  @Override
  public TransactionContext setShouldCommit(boolean sCommit) {
    this.shouldCommit = sCommit;
    return this;
  }

  @Override
  public boolean shouldCommit() {
    // TODO: Hook this up in the server to bypass the RAFT Log and send back a response to client
    return this.shouldCommit;
  }

  @Override
  public TransactionContext preAppendTransaction() throws IOException {
    return stateMachine.preAppendTransaction(this);
  }

  @Override
  public TransactionContext cancelTransaction() throws IOException {
    // TODO: This is not called from Raft server / log yet. When an IOException happens, we should
    // call this to let the SM know that Transaction cannot be synced
    return stateMachine.cancelTransaction(this);
  }

  public static LogEntryProto getLogEntry(TransactionContext context) {
    return ((TransactionContextImpl) context).logEntry;
  }
}
