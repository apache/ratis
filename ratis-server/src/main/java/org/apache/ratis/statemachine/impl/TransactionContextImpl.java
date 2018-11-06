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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.Preconditions;

import java.io.IOException;
import java.util.Objects;

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
  private RaftClientRequest clientRequest;

  /** Exception from the {@link StateMachine} or from the log */
  private Exception exception;

  /** Data from the {@link StateMachine} */
  private StateMachineLogEntryProto smLogEntryProto;

  /**
   * Context specific to the state machine.
   * The {@link StateMachine} can use this object to carry state between
   * {@link StateMachine#startTransaction(RaftClientRequest)} and
   * {@link StateMachine#applyTransaction(TransactionContext)}.
   */
  private Object stateMachineContext;

  /**
   * Whether to commit the transaction to the RAFT Log.
   * In some cases the {@link StateMachine} may want to indicate
   * that the transaction should not be committed
   */
  private boolean shouldCommit = true;

  /** Committed LogEntry. */
  private LogEntryProto logEntry;

  private TransactionContextImpl(RaftPeerRole serverRole, StateMachine stateMachine) {
    this.serverRole = serverRole;
    this.stateMachine = stateMachine;
  }

  /**
   * Construct a {@link TransactionContext} from a client request.
   * Used by the state machine to start a transaction
   * and send the Log entry representing the transaction data
   * to be applied to the raft log.
   */
  public TransactionContextImpl(
      StateMachine stateMachine, RaftClientRequest clientRequest,
      StateMachineLogEntryProto smLogEntryProto, Object stateMachineContext) {
    this(RaftPeerRole.LEADER, stateMachine);
    this.clientRequest = clientRequest;
    this.smLogEntryProto = smLogEntryProto != null? smLogEntryProto
        : ServerProtoUtils.toStateMachineLogEntryProto(clientRequest, null, null);
    this.stateMachineContext = stateMachineContext;
  }

  /**
   * Construct a {@link TransactionContext} from a {@link LogEntryProto}.
   * Used by followers for applying committed entries to the state machine.
   * @param logEntry the log entry to be applied
   */
  public TransactionContextImpl(RaftPeerRole serverRole, StateMachine stateMachine, LogEntryProto logEntry) {
    this(serverRole, stateMachine);
    this.logEntry = logEntry;
    this.smLogEntryProto = logEntry.getStateMachineLogEntry();
  }

  @Override
  public RaftPeerRole getServerRole() {
    return serverRole;
  }

  @Override
  public RaftClientRequest getClientRequest() {
    return clientRequest;
  }

  @Override
  public StateMachineLogEntryProto getStateMachineLogEntry() {
    return smLogEntryProto;
  }

  @Override
  public Exception getException() {
    return exception;
  }

  @Override
  public TransactionContext setStateMachineContext(Object stateMachineContext) {
    this.stateMachineContext = stateMachineContext;
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
    Objects.requireNonNull(smLogEntryProto, "smLogEntryProto == null");
    return logEntry = ServerProtoUtils.toLogEntryProto(smLogEntryProto, term, index);
  }

  @Override
  public TransactionContext setStateMachineLogEntryProto(StateMachineLogEntryProto smLogEntryProto) {
    this.smLogEntryProto = smLogEntryProto;
    return this;
  }

  @Override
  public LogEntryProto getLogEntry() {
    return logEntry;
  }

  @Override
  public TransactionContext setException(Exception ioe) {
    this.exception = ioe;
    return this;
  }

  @Override
  public TransactionContext setShouldCommit(boolean shouldCommit) {
    this.shouldCommit = shouldCommit;
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
}
