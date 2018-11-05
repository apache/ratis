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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

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
public interface TransactionContext {
  /** @return the role of the server when this context is created. */
  RaftPeerRole getServerRole();

  /**
   * Returns the original request from the {@link RaftClientRequest}
   * @return the original request from the {@link RaftClientRequest}
   */
  RaftClientRequest getClientRequest();

  /**
   * Returns the data from the {@link StateMachine}
   * @return the data from the {@link StateMachine}
   */
  StateMachineLogEntryProto getStateMachineLogEntry();

  /** Set exception in case of failure. */
  TransactionContext setException(Exception exception);

  /**
   * Returns the exception from the {@link StateMachine} or the log
   * @return the exception from the {@link StateMachine} or the log
   */
  Exception getException();

  /**
   * Sets the {@link StateMachine} the {@link TransactionContext} is specific to, the method would
   * not create a new transaction context, it updates the {@link StateMachine} it associates with
   * @param stateMachineContext state machine context
   * @return transaction context specific to the given {@link StateMachine}
   */
  TransactionContext setStateMachineContext(Object stateMachineContext);

  /**
   * Returns the {@link StateMachine} the current {@link TransactionContext} specific to
   * @return the {@link StateMachine} the current {@link TransactionContext} specific to
   */
  Object getStateMachineContext();

  /**
   * Initialize {@link LogEntryProto} using the internal {@link StateMachineLogEntryProto}.
   * @param term The current term.
   * @param index The index of the log entry.
   * @return the result {@link LogEntryProto}
   */
  LogEntryProto initLogEntry(long term, long index);

  /**
   * Sets the data from the {@link StateMachine}
   * @param smLogEntryProto data from {@link StateMachine}
   * @return the current {@link TransactionContext} itself
   */
  TransactionContext setStateMachineLogEntryProto(StateMachineLogEntryProto smLogEntryProto);

  /**
   * Returns the committed log entry
   * @return the committed log entry
   */
  LogEntryProto getLogEntry();

  /**
   * Sets whether to commit the transaction to the RAFT log or not
   * @param shouldCommit true if the transaction is supposed to be committed to the RAFT log
   * @return the current {@link TransactionContext} itself
   */
  TransactionContext setShouldCommit(boolean shouldCommit);

  /**
   * It indicates if the transaction should be committed to the RAFT log
   * @return true if it commits the transaction to the RAFT log, otherwise, false
   */
  boolean shouldCommit();

  // proxy StateMachine methods. We do not want to expose the SM to the RaftLog

  /**
   * This is called before the transaction passed from the StateMachine is appended to the raft log.
   * This method will be called from log append and having the same strict serial order that the
   * Transactions will have in the RAFT log. Since this is called serially in the critical path of
   * log append, it is important to do only required operations here.
   * @return The Transaction context.
   */
  TransactionContext preAppendTransaction() throws IOException;

  /**
   * Called to notify the state machine that the Transaction passed cannot be appended (or synced).
   * The exception field will indicate whether there was an exception or not.
   * @return cancelled transaction
   */
  TransactionContext cancelTransaction() throws IOException;

  static Builder newBuilder() {
    return new Builder();
  }

  class Builder {
    private RaftPeerRole serverRole = RaftPeerRole.LEADER;
    private StateMachine stateMachine;
    private Object stateMachineContext;

    private RaftClientRequest clientRequest;
    private LogEntryProto logEntry;
    private StateMachineLogEntryProto stateMachineLogEntry;
    private ByteString logData;
    private ByteString stateMachineData;

    public Builder setServerRole(RaftPeerRole serverRole) {
      this.serverRole = serverRole;
      return this;
    }

    public Builder setStateMachine(StateMachine stateMachine) {
      this.stateMachine = stateMachine;
      return this;
    }

    public Builder setStateMachineContext(Object stateMachineContext) {
      this.stateMachineContext = stateMachineContext;
      return this;
    }

    public Builder setClientRequest(RaftClientRequest clientRequest) {
      this.clientRequest = clientRequest;
      return this;
    }

    public Builder setLogEntry(LogEntryProto logEntry) {
      this.logEntry = logEntry;
      return this;
    }

    public Builder setStateMachineLogEntry(StateMachineLogEntryProto stateMachineLogEntry) {
      this.stateMachineLogEntry = stateMachineLogEntry;
      return this;
    }

    public Builder setLogData(ByteString logData) {
      this.logData = logData;
      return this;
    }

    public Builder setStateMachineData(ByteString stateMachineData) {
      this.stateMachineData = stateMachineData;
      return this;
    }

    public TransactionContext build() {
      Objects.requireNonNull(serverRole, "serverRole == null");
      Objects.requireNonNull(stateMachine, "stateMachine == null");
      if (clientRequest != null) {
        Preconditions.assertTrue(serverRole == RaftPeerRole.LEADER,
            () -> "serverRole MUST be LEADER since clientRequest != null, serverRole is " + serverRole);
        Preconditions.assertNull(logEntry, () -> "logEntry MUST be null since clientRequest != null");
        if (stateMachineLogEntry == null) {
          stateMachineLogEntry = ServerProtoUtils.toStateMachineLogEntryProto(clientRequest, logData, stateMachineData);
        }
        return new TransactionContextImpl(stateMachine, clientRequest, stateMachineLogEntry, stateMachineContext);
      } else {
        Objects.requireNonNull(logEntry, "logEntry MUST NOT be null since clientRequest == null");
        Preconditions.assertTrue(logEntry.hasStateMachineLogEntry(),
            () -> "Unexpected logEntry: stateMachineLogEntry not found, logEntry=" + logEntry);
        return new TransactionContextImpl(serverRole, stateMachine, logEntry);
      }
    }
  }
}
