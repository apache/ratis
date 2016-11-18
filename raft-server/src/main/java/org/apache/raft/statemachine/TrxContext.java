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
package org.apache.raft.statemachine;

import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.raft.shaded.proto.RaftProtos.SMLogEntryProto;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/**
 * Context for a transaction. The transaction might have originated from a client request, or it
 * maybe coming from another replica of the state machine through the RAFT log. TrxContext can be
 * either created from the StateMachine or can be created by the StateMachineUpdater. In the first
 * case, the StateMachine receives a
 * {@link StateMachine#startTransaction(RaftClientRequest)} request, and
 * should return a TrxContext with the changes from the SM. The same context will come back to the
 * SM via {@link StateMachine#applyTransaction(TrxContext)} call
 * or {@link StateMachine#notifyNotLeader(Collection)} call. In the second
 * case, if the StateMachine is a follower, the TrxContext will be a committed entry coming from
 * the RAFT log from the leader.
 */
public class TrxContext {

  /** The StateMachine that originated the Transaction. */
  protected final StateMachine stateMachine;

  /** Original request from the client */
  protected Optional<RaftClientRequest> clientRequest;

  /** Exception from the StateMachine or log */
  protected Optional<Exception> exception;

  /** Data from the StateMachine */
  protected Optional<SMLogEntryProto> smLogEntryProto;

  /** Context specific to the State machine. The StateMachine can use this object to carry state
   * between startTransaction() and applyLogEntries() */
  protected Optional<Object> stateMachineContext;

  /** Whether to commit the transaction to the RAFT Log. In some cases the SM may want to indicate
   * that the transaction should not be committed */
  protected boolean shouldCommit = true;

  /**
   * Committed LogEntry.
   */
  protected Optional<LogEntryProto> logEntry;

  private TrxContext(StateMachine stateMachine) {
    this.stateMachine = stateMachine;
  }

  /**
   * Construct a TrxContext from a client request. Used by the state machine to start a transaction
   * and send the Log entry representing the SM data to be applied to the raft log.
   */
  public TrxContext(StateMachine stateMachine,
      RaftClientRequest clientRequest, SMLogEntryProto smLogEntryProto) {
    this(stateMachine, clientRequest, smLogEntryProto, null);
  }

  /**
   * Construct a TrxContext from a client request. Used by the state machine to start a transaction
   * and send the Log entry representing the SM data to be applied to the raft log.
   */
  public TrxContext(StateMachine stateMachine, RaftClientRequest clientRequest) {
    this(stateMachine);
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.empty();
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.ofNullable(stateMachineContext);
    this.logEntry = Optional.empty();
  }

  /**
   * Construct a TrxContext from a client request. Used by the state machine to start a transaction
   * and send the Log entry representing the SM data to be applied to the raft log.
   */
  public TrxContext(StateMachine stateMachine, RaftClientRequest clientRequest,
      SMLogEntryProto smLogEntryProto, Object stateMachineContext) {
    this(stateMachine);
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.of(smLogEntryProto);
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.ofNullable(stateMachineContext);
    this.logEntry = Optional.empty();
  }

  /**
   * Construct a TrxContext from a client request to signal a failure. RAFT server will fail this
   * request on behalf of the SM.
   */
  public TrxContext(StateMachine stateMachine, RaftClientRequest clientRequest,
                    Exception exception) {
    this(stateMachine, clientRequest, exception, null);
  }

  /**
   * Construct a TrxContext from a client request to signal a failure. RAFT server will fail this
   * request on behalf of the SM.
   */
  public TrxContext(StateMachine stateMachine, RaftClientRequest clientRequest, Exception exception,
                    Object stateMachineContext) {
    this(stateMachine);
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.empty();
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.ofNullable(stateMachineContext);
    this.logEntry = Optional.empty();
  }

  /**
   * Construct a TrxContext from a LogEntry. Used by followers for applying committed entries to the
   * state machine
   * @param logEntry the log entry to be applied
   */
  public TrxContext(StateMachine stateMachine, LogEntryProto logEntry) {
    this(stateMachine);
    this.clientRequest = Optional.empty();
    this.smLogEntryProto = Optional.of(logEntry.getSmLogEntry());
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.empty();
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

  public TrxContext setStateMachineContext(Object stateMachineContext) {
    this.stateMachineContext = Optional.ofNullable(stateMachineContext);
    return this;
  }

  public Optional<Object> getStateMachineContext() {
    return stateMachineContext;
  }

  public TrxContext setLogEntry(LogEntryProto logEntry) {
    this.logEntry = Optional.of(logEntry);
    return this;
  }

  public TrxContext setSmLogEntryProto(SMLogEntryProto smLogEntryProto) {
    this.smLogEntryProto = Optional.of(smLogEntryProto);
    return this;
  }

  public Optional<LogEntryProto> getLogEntry() {
    return logEntry;
  }

  private TrxContext setException(IOException ioe) {
    assert !this.exception.isPresent();
    this.exception = Optional.of(ioe);
    return this;
  }

  public TrxContext setShouldCommit(boolean shouldCommit) {
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
  public TrxContext preAppendTransaction() throws IOException {
    return stateMachine.preAppendTransaction(this);
  }

  /**
   * Called to notify the state machine that the Transaction passed cannot be appended (or synced).
   * The exception field will indicate whether there was an exception or not.
   * @return cancelled transaction
   */
  public TrxContext cancelTransaction() throws IOException {
    // TODO: This is not called from Raft server / log yet. When an IOException happens, we should
    // call this to let the SM know that Transaction cannot be synced
    return stateMachine.cancelTransaction(this);
  }
}
