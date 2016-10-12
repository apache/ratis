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

import org.apache.raft.proto.RaftProtos;
import org.apache.raft.protocol.RaftClientRequest;

import java.util.Collection;
import java.util.Optional;

/**
 * Context for a transaction. The transaction might have originated from a client request, or it
 * maybe coming from another replica of the state machine through the RAFT log. TrxContext can be
 * either created from the StateMachine or can be created by the StateMachineUpdater. In the first
 * case, the StateMachine receives a
 * {@link StateMachine#startTransaction(RaftClientRequest)} request, and
 * should return a TrxContext with the changes from the SM. The same context will come back to the
 * SM via {@link StateMachine#applyLogEntry(TrxContext)} call
 * or {@link StateMachine#notifyNotLeader(Collection)} call. In the second
 * case, if the StateMachine is a follower, the TrxContext will be a committed entry coming from
 * the RAFT log from the leader.
 */
public class TrxContext {

  /** Original request from the client */
  protected final Optional<RaftClientRequest> clientRequest;

  /** Exception from the StateMachine */
  protected final Optional<Exception> exception;

  /** Data from the StateMachine */
  protected final Optional<RaftProtos.SMLogEntryProto> smLogEntryProto;

  /** Context specific to the State machine. The StateMachine can use this object to carry state
   * between startTransaction() and applyLogEntries() */
  protected final Optional<Object> stateMachineContext;

  /**
   * Committed LogEntry.
   */
  protected Optional<RaftProtos.LogEntryProto> logEntry = Optional.empty();

  /**
   * Construct a TrxContext from a client request. Used by the state machine to start a transaction
   * and send the Log entry representing the SM data to be applied to the raft log.
   */
  public TrxContext(RaftClientRequest clientRequest, RaftProtos.SMLogEntryProto smLogEntryProto) {
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.of(smLogEntryProto);
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.empty();
  }

  /**
   * Construct a TrxContext from a client request. Used by the state machine to start a transaction
   * and send the Log entry representing the SM data to be applied to the raft log.
   */
  public TrxContext(RaftClientRequest clientRequest, RaftProtos.SMLogEntryProto smLogEntryProto,
                    Object stateMachineContext) {
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.of(smLogEntryProto);
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.of(stateMachineContext);
  }

  /**
   * Construct a TrxContext from a client request to signal a failure. RAFT server will fail this
   * request on behalf of the SM.
   */
  public TrxContext(RaftClientRequest clientRequest, Exception exception) {
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.empty();
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.empty();
  }

  /**
   * Construct a TrxContext from a client request to signal a failure. RAFT server will fail this
   * request on behalf of the SM.
   */
  public TrxContext(RaftClientRequest clientRequest, Exception exception,
                    Object stateMachineContext) {
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.empty();
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.of(stateMachineContext);
  }

  /**
   * Construct a TrxContext from a LogEntry. Used by followers for applying committed entries to the
   * state machine
   * @param logEntry
   */
  public TrxContext(RaftProtos.LogEntryProto logEntry) {
    this.clientRequest = Optional.empty();
    this.smLogEntryProto = Optional.of(logEntry.getSmLogEntry());
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.empty();
    this.logEntry = Optional.of(logEntry);
  }

  public Optional<RaftClientRequest> getClientRequest() {
    return this.clientRequest;
  }

  public Optional<RaftProtos.SMLogEntryProto> getSMLogEntry() {
    return this.smLogEntryProto;
  }

  public Optional<Exception> getException() {
    return this.exception;
  }

  public Optional<Object> getStateMachineContext() {
    return stateMachineContext;
  }

  public void setLogEntry(RaftProtos.LogEntryProto logEntry) {
    this.logEntry = Optional.of(logEntry);
  }

  public Optional<RaftProtos.LogEntryProto> getLogEntry() {
    return logEntry;
  }
}
