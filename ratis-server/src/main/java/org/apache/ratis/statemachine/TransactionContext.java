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

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.RaftProtos.SMLogEntryProto;

/**
 * Context for a transaction.
 * The transaction might have originated from a client request, or it
 * maybe coming from another replica of the state machine through the RAFT log.
 * {@link TransactionContext} can be created from
 * either the {@link StateMachine} or the state machine updater.
 *
 * In the first case, the {@link StateMachine} is a leader. When it receives
 * a {@link StateMachine#startTransaction(RaftClientRequest)} request, it returns
 * a {@link TransactionContext} with the changes from the {@link StateMachine}.
 * The same context will be passed back to the {@link StateMachine}
 * via the {@link StateMachine#applyTransaction(TransactionContext)} call
 * or the {@link StateMachine#notifyNotLeader(Collection)} call.
 *
 * In the second case, the {@link StateMachine} is a follower.
 * The {@link TransactionContext} will be a committed entry coming from
 * the RAFT log from the leader.
 */
public class TransactionContext {

  /** The {@link StateMachine} that originated the transaction. */
  private final StateMachine stateMachine;

  /** Original request from the client */
  private Optional<RaftClientRequest> clientRequest = Optional.empty();

  /** Exception from the {@link StateMachine} or from the log */
  private Optional<Exception> exception = Optional.empty();

  /** Data from the {@link StateMachine} */
  private Optional<SMLogEntryProto> smLogEntryProto = Optional.empty();

  /**
   * Context specific to the state machine.
   * The {@link StateMachine} can use this object to carry state between
   * {@link StateMachine#startTransaction(RaftClientRequest)} and
   * {@link StateMachine#applyTransaction(TransactionContext)}.
   */
  private Optional<Object> stateMachineContext = Optional.empty();

  /**
   * Whether to commit the transaction to the RAFT Log.
   * In some cases the {@link StateMachine} may want to indicate
   * that the transaction should not be committed
   */
  private boolean shouldCommit = true;

  /** Committed LogEntry. */
  private Optional<LogEntryProto> logEntry = Optional.empty();

  private TransactionContext(StateMachine stateMachine) {
    this.stateMachine = stateMachine;
  }

  /** The same as this(stateMachine, clientRequest, smLogEntryProto, null). */
  public TransactionContext(
      StateMachine stateMachine, RaftClientRequest clientRequest,
      SMLogEntryProto smLogEntryProto) {
    this(stateMachine, clientRequest, smLogEntryProto, null);
  }

  /**
   * Construct a {@link TransactionContext} from a client request.
   * Used by the state machine to start a transaction
   * and send the Log entry representing the transaction data
   * to be applied to the raft log.
   */
  public TransactionContext(
      StateMachine stateMachine, RaftClientRequest clientRequest,
      SMLogEntryProto smLogEntryProto, Object stateMachineContext) {
    this(stateMachine);
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.ofNullable(smLogEntryProto);
    this.stateMachineContext = Optional.ofNullable(stateMachineContext);
  }

  /** The same as this(stateMachine, clientRequest, exception, null). */
  public TransactionContext(
      StateMachine stateMachine, RaftClientRequest clientRequest,
      Exception exception) {
    this(stateMachine, clientRequest, exception, null);
  }

  /**
   * Construct a {@link TransactionContext} from a client request to signal
   * an exception so that the RAFT server will fail the request on behalf
   * of the {@link StateMachine}.
   */
  public TransactionContext(
      StateMachine stateMachine, RaftClientRequest clientRequest,
      Exception exception, Object stateMachineContext) {
    this(stateMachine);
    this.clientRequest = Optional.of(clientRequest);
    this.exception = Optional.of(exception);
    this.stateMachineContext = Optional.ofNullable(stateMachineContext);
  }

  /**
   * Construct a {@link TransactionContext} from a {@link LogEntryProto}.
   * Used by followers for applying committed entries to the state machine.
   * @param logEntry the log entry to be applied
   */
  public TransactionContext(StateMachine stateMachine, LogEntryProto logEntry) {
    this(stateMachine);
    this.smLogEntryProto = Optional.of(logEntry.getSmLogEntry());
    this.logEntry = Optional.of(logEntry);
  }

  public Optional<RaftClientRequest> getClientRequest() {
    return this.clientRequest;
  }

  public Optional<SMLogEntryProto> getSMLogEntry() {
    return this.smLogEntryProto;
  }

  public Optional<Exception> getException() {
    return this.exception;
  }

  public TransactionContext setStateMachineContext(Object stateMachineContext) {
    this.stateMachineContext = Optional.ofNullable(stateMachineContext);
    return this;
  }

  public Optional<Object> getStateMachineContext() {
    return stateMachineContext;
  }

  public TransactionContext setLogEntry(LogEntryProto logEntry) {
    this.logEntry = Optional.of(logEntry);
    return this;
  }

  public TransactionContext setSmLogEntryProto(SMLogEntryProto smLogEntryProto) {
    this.smLogEntryProto = Optional.of(smLogEntryProto);
    return this;
  }

  public Optional<LogEntryProto> getLogEntry() {
    return logEntry;
  }

  private TransactionContext setException(IOException ioe) {
    assert !this.exception.isPresent();
    this.exception = Optional.of(ioe);
    return this;
  }

  public TransactionContext setShouldCommit(boolean shouldCommit) {
    this.shouldCommit = shouldCommit;
    return this;
  }

  public boolean shouldCommit() {
    // TODO: Hook this up in the server to bypass the RAFT Log and send back a response to client
    return this.shouldCommit;
  }

  // proxy StateMachine methods. We do not want to expose the SM to the RaftLog

  /**
   * This is called before the transaction passed from the StateMachine is appended to the raft log.
   * This method will be called from log append and having the same strict serial order that the
   * Transactions will have in the RAFT log. Since this is called serially in the critical path of
   * log append, it is important to do only required operations here.
   * @return The Transaction context.
   */
  public TransactionContext preAppendTransaction() throws IOException {
    return stateMachine.preAppendTransaction(this);
  }

  /**
   * Called to notify the state machine that the Transaction passed cannot be appended (or synced).
   * The exception field will indicate whether there was an exception or not.
   * @return cancelled transaction
   */
  public TransactionContext cancelTransaction() throws IOException {
    // TODO: This is not called from Raft server / log yet. When an IOException happens, we should
    // call this to let the SM know that Transaction cannot be synced
    return stateMachine.cancelTransaction(this);
  }
}
